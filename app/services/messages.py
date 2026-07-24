"""Message browsing and search.

Kafka has no index. Every search is a forward scan of the log, so the design here
is about bounding that scan: the caller picks a window (a tail lookback, a
timestamp, or the whole topic), and the scan stops at whichever comes first --
the result limit, the scan budget, the end of the log, or the client hanging up.

Hits stream out as they're found rather than accumulating in memory.
"""

import asyncio
import base64
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import AsyncIterator, Literal

from confluent_kafka import Consumer, KafkaError, TopicPartition

from ..config import settings
from ..kafka_client import base_config
from .decode import decode

StartMode = Literal["latest", "earliest", "timestamp"]


@dataclass
class Message:
    partition: int
    offset: int
    timestamp_ms: int
    key: str | None
    value: str | None
    headers: list[tuple[str, str]]
    size: int
    binary: bool = False
    # How the value was decoded ("string"/"json"/"avro"/"json-schema"/"protobuf"/…)
    # and, for registry-encoded payloads, the schema id from the wire header.
    encoding: str = "string"
    schema_id: int | None = None

    @property
    def when(self) -> str:
        if self.timestamp_ms <= 0:
            return "—"
        return datetime.fromtimestamp(self.timestamp_ms / 1000, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S.%f"
        )[:-3]

    @property
    def is_tombstone(self) -> bool:
        return self.value is None

    @property
    def pretty(self) -> str | None:
        """Value re-rendered as indented JSON, or None if it isn't JSON."""
        if self.value is None:
            return None
        try:
            return json.dumps(json.loads(self.value), indent=2, ensure_ascii=False)
        except (ValueError, TypeError):
            return None


@dataclass
class ScanSpec:
    topic: str
    partition: int | None = None
    start: StartMode = "latest"
    lookback: int = 1000
    timestamp_ms: int | None = None
    limit: int = 100
    key_contains: str = ""
    value_contains: str = ""
    header_key: str = ""
    header_value: str = ""
    json_path: str = ""
    json_value: str = ""
    max_scan: int = field(default_factory=lambda: settings.kafkascope_scan_limit)

    @property
    def has_filters(self) -> bool:
        return bool(
            self.key_contains
            or self.value_contains
            or self.header_key
            or self.json_path
        )


@dataclass
class ScanStats:
    scanned: int = 0
    hits: int = 0
    reason: str = ""


def _decode(raw: bytes | None) -> tuple[str | None, bool]:
    """Return (text, is_binary). Binary payloads are shown base64'd rather than mangled."""
    if raw is None:
        return None, False
    try:
        return raw.decode("utf-8"), False
    except UnicodeDecodeError:
        return base64.b64encode(raw).decode("ascii"), True


def _json_lookup(payload: str, path: str):
    """Walk a dotted path into a JSON payload. Integer segments index into lists."""
    try:
        node = json.loads(payload)
    except (ValueError, TypeError):
        return None

    for segment in path.split("."):
        if isinstance(node, dict):
            node = node.get(segment)
        elif isinstance(node, list) and segment.isdigit():
            index = int(segment)
            node = node[index] if index < len(node) else None
        else:
            return None
        if node is None:
            return None
    return node


def _build(raw) -> Message:
    key = decode(raw.key())
    value = decode(raw.value())
    return Message(
        partition=raw.partition(),
        offset=raw.offset(),
        timestamp_ms=raw.timestamp()[1],
        key=key.text,
        value=value.text,
        headers=[(k, _decode(v)[0] or "") for k, v in (raw.headers() or [])],
        size=len(raw.value() or b"") + len(raw.key() or b""),
        binary=value.binary,
        encoding=value.encoding,
        schema_id=value.schema_id,
    )


def fetch_one(topic: str, partition: int, offset: int) -> Message | None:
    """Read exactly one message by coordinate — used to prefill a resend."""
    consumer = Consumer(
        {
            **base_config(),
            "group.id": "kafkascope-fetch",
            "enable.auto.commit": False,
        }
    )
    try:
        consumer.assign([TopicPartition(topic, partition, offset)])
        batch = consumer.consume(1, timeout=settings.kafkascope_request_timeout)
        if not batch:
            return None
        raw = batch[0]
        if raw.error():
            raise RuntimeError(raw.error().str())
        return _build(raw)
    finally:
        consumer.close()


def matches(msg: Message, spec: ScanSpec) -> bool:
    if spec.key_contains and spec.key_contains.lower() not in (msg.key or "").lower():
        return False

    if spec.value_contains and spec.value_contains.lower() not in (msg.value or "").lower():
        return False

    if spec.header_key:
        found = [v for k, v in msg.headers if k == spec.header_key]
        if not found:
            return False
        if spec.header_value and not any(spec.header_value.lower() in v.lower() for v in found):
            return False

    if spec.json_path:
        if msg.value is None:
            return False
        node = _json_lookup(msg.value, spec.json_path)
        if node is None:
            return False
        # No json_value means "the path exists at all"; otherwise compare stringified.
        if spec.json_value:
            rendered = node if isinstance(node, str) else json.dumps(node, ensure_ascii=False)
            if spec.json_value.lower() not in str(rendered).lower():
                return False

    return True


def _plan(consumer: Consumer, spec: ScanSpec) -> tuple[list[TopicPartition], dict[int, int], int]:
    """Work out where each partition's scan starts and ends.

    Returns (assignments, {partition: end_offset}, total_records_in_window).
    """
    md = consumer.list_topics(timeout=settings.kafkascope_request_timeout)
    topic_md = md.topics.get(spec.topic)
    if topic_md is None or topic_md.error is not None:
        raise LookupError(f"Topic {spec.topic!r} not found")

    partitions = sorted(topic_md.partitions.keys())
    if spec.partition is not None:
        if spec.partition not in partitions:
            raise LookupError(f"Partition {spec.partition} not in topic {spec.topic!r}")
        partitions = [spec.partition]

    marks = {
        p: consumer.get_watermark_offsets(
            TopicPartition(spec.topic, p), timeout=settings.kafkascope_request_timeout
        )
        for p in partitions
    }

    starts: dict[int, int] = {}
    if spec.start == "timestamp":
        if spec.timestamp_ms is None:
            raise ValueError("A timestamp is required when starting from a timestamp")
        wanted = [TopicPartition(spec.topic, p, spec.timestamp_ms) for p in partitions]
        for tp in consumer.offsets_for_times(wanted, timeout=settings.kafkascope_request_timeout):
            # No offset at/after the timestamp means everything is older: start at the end.
            starts[tp.partition] = (
                marks[tp.partition][1] if tp.offset < 0 else tp.offset
            )
    elif spec.start == "earliest":
        starts = {p: marks[p][0] for p in partitions}
    else:  # latest: scan a tail window, split across the partitions in play
        per_partition = max(1, spec.lookback // len(partitions))
        starts = {p: max(marks[p][0], marks[p][1] - per_partition) for p in partitions}

    ends = {p: marks[p][1] for p in partitions}
    total = sum(max(0, ends[p] - starts[p]) for p in partitions)
    assignments = [TopicPartition(spec.topic, p, starts[p]) for p in partitions]
    return assignments, ends, total


async def scan(spec: ScanSpec, is_cancelled) -> AsyncIterator[tuple[str, object]]:
    """Yield ('hit', Message), ('progress', ScanStats) and finally ('done', ScanStats).

    `is_cancelled` is an async predicate; the scan checks it between batches so a
    browser closing the connection stops the work rather than orphaning it.
    """
    consumer = Consumer(
        {
            **base_config(),
            "group.id": "kafkascope-scan",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    stats = ScanStats()

    try:
        assignments, ends, total = await asyncio.to_thread(_plan, consumer, spec)
        budget = min(total, spec.max_scan)

        if budget == 0:
            stats.reason = "Nothing in the selected window"
            yield "done", stats
            return

        await asyncio.to_thread(consumer.assign, assignments)
        remaining = {tp.partition: ends[tp.partition] - tp.offset for tp in assignments}

        while True:
            if await is_cancelled():
                stats.reason = "Cancelled"
                break
            if stats.scanned >= budget:
                stats.reason = (
                    f"Scan budget reached ({budget:,} records) — narrow the window or raise KAFKASCOPE_SCAN_LIMIT"
                    if budget < total
                    else "Reached the end of the window"
                )
                break
            if not any(v > 0 for v in remaining.values()):
                stats.reason = "Reached the end of the window"
                break

            # Never fetch past the budget: the batch is capped at what's left of it,
            # otherwise a 500-record fetch overshoots a smaller budget every time.
            batch_size = max(1, min(500, budget - stats.scanned))
            batch = await asyncio.to_thread(consumer.consume, batch_size, 1.0)
            if not batch:
                # The window said there were records but the broker has gone quiet.
                stats.reason = "Reached the end of the window"
                break

            for raw in batch:
                err = raw.error()
                if err:
                    if err.code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(err.str())

                stats.scanned += 1
                remaining[raw.partition()] = ends[raw.partition()] - raw.offset() - 1
                # _build may hit the schema registry (once per uncached id), so keep
                # that network call off the event loop.
                msg = await asyncio.to_thread(_build, raw)

                if matches(msg, spec):
                    stats.hits += 1
                    yield "hit", msg
                    if stats.hits >= spec.limit:
                        stats.reason = f"Result limit reached ({spec.limit})"
                        yield "done", stats
                        return

            yield "progress", stats

        yield "done", stats
    finally:
        await asyncio.to_thread(consumer.close)


def _tail_assignments(consumer: Consumer, spec: ScanSpec) -> list[TopicPartition]:
    """Assign each partition at its current high watermark, so a tail shows only
    messages produced from now on — never a replay of existing history."""
    md = consumer.list_topics(timeout=settings.kafkascope_request_timeout)
    topic_md = md.topics.get(spec.topic)
    if topic_md is None or topic_md.error is not None:
        raise LookupError(f"Topic {spec.topic!r} not found")

    partitions = sorted(topic_md.partitions.keys())
    if spec.partition is not None:
        if spec.partition not in partitions:
            raise LookupError(f"Partition {spec.partition} not in topic {spec.topic!r}")
        partitions = [spec.partition]

    assignments = []
    for p in partitions:
        _, high = consumer.get_watermark_offsets(
            TopicPartition(spec.topic, p), timeout=settings.kafkascope_request_timeout
        )
        assignments.append(TopicPartition(spec.topic, p, high))
    return assignments


async def tail(spec: ScanSpec, is_cancelled) -> AsyncIterator[tuple[str, object]]:
    """Follow a topic's tail: start at the current end and yield new messages as they
    arrive, applying the same filters as scan(). Unlike scan() there is no window or
    budget — it runs until the client disconnects. `spec.partition` and the filter
    fields are honoured; the start/lookback/limit window fields are ignored.
    """
    consumer = Consumer(
        {
            **base_config(),
            "group.id": "kafkascope-tail",
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
        }
    )
    stats = ScanStats()

    try:
        assignments = await asyncio.to_thread(_tail_assignments, consumer, spec)
        await asyncio.to_thread(consumer.assign, assignments)

        while True:
            if await is_cancelled():
                stats.reason = "Stopped"
                break

            batch = await asyncio.to_thread(consumer.consume, 200, 1.0)
            for raw in batch:
                err = raw.error()
                if err:
                    if err.code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(err.str())

                stats.scanned += 1
                msg = await asyncio.to_thread(_build, raw)
                if matches(msg, spec):
                    stats.hits += 1
                    yield "hit", msg

            # A heartbeat even when nothing matched, so the client knows it's alive
            # and the disconnect check runs on a quiet topic.
            yield "progress", stats

        yield "done", stats
    finally:
        await asyncio.to_thread(consumer.close)
