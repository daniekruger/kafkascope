"""Consumer group inspection and offset management.

Offset resets are the sharpest tool in this app: moving a live group's offsets
causes reprocessing or silently skipped messages. Kafka itself refuses to reset a
group that still has members, and that refusal is surfaced rather than worked
around -- stopping the consumer first is the correct workflow, not an obstacle.
"""

from dataclasses import dataclass
from typing import Literal

from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition

from ..config import settings
from ..kafka_client import admin, consumer, is_readonly

ResetTarget = Literal["earliest", "latest", "timestamp", "offset"]

# Kafka reports "no committed offset" as a negative sentinel.
NO_OFFSET = -1001


@dataclass
class MemberInfo:
    member_id: str
    client_id: str
    host: str
    assignment: list[tuple[str, int]]


@dataclass
class GroupSummary:
    id: str
    state: str
    members: int
    is_simple: bool

    @property
    def is_empty(self) -> bool:
        return self.state == "EMPTY"


@dataclass
class PartitionOffset:
    topic: str
    partition: int
    committed: int
    low: int
    high: int

    @property
    def has_offset(self) -> bool:
        return self.committed >= 0

    @property
    def lag(self) -> int | None:
        return self.high - self.committed if self.has_offset else None


@dataclass
class GroupDetail:
    id: str
    state: str
    is_simple: bool
    coordinator: str
    members: list[MemberInfo]
    offsets: list[PartitionOffset]

    @property
    def is_empty(self) -> bool:
        return self.state == "EMPTY"

    @property
    def total_lag(self) -> int:
        return sum(o.lag or 0 for o in self.offsets)

    @property
    def topics(self) -> list[str]:
        return sorted({o.topic for o in self.offsets})


def _state_name(state) -> str:
    return getattr(state, "name", str(state)).upper()


def list_groups() -> list[GroupSummary]:
    result = admin().list_consumer_groups(
        request_timeout=settings.kafkascope_request_timeout
    ).result()

    ids = [g.group_id for g in result.valid]
    if not ids:
        return []

    described = admin().describe_consumer_groups(
        ids, request_timeout=settings.kafkascope_request_timeout
    )

    summaries = []
    for group_id, future in described.items():
        try:
            d = future.result()
        except Exception:
            # A group can vanish between listing and describing; don't fail the page.
            continue
        summaries.append(
            GroupSummary(
                id=group_id,
                state=_state_name(d.state),
                members=len(d.members),
                is_simple=d.is_simple_consumer_group,
            )
        )
    return sorted(summaries, key=lambda g: g.id)


def _committed(group_id: str) -> list[TopicPartition]:
    request = ConsumerGroupTopicPartitions(group_id, None)
    future = admin().list_consumer_group_offsets([request])[group_id]
    return future.result(timeout=settings.kafkascope_request_timeout).topic_partitions or []


def get_group(group_id: str) -> GroupDetail | None:
    described = admin().describe_consumer_groups(
        [group_id], request_timeout=settings.kafkascope_request_timeout
    )
    try:
        d = described[group_id].result()
    except Exception:
        return None

    members = [
        MemberInfo(
            member_id=m.member_id,
            client_id=m.client_id,
            host=m.host,
            assignment=sorted(
                (tp.topic, tp.partition)
                for tp in (m.assignment.topic_partitions if m.assignment else [])
            ),
        )
        for m in d.members
    ]

    offsets = []
    with consumer() as c:
        for tp in sorted(_committed(group_id), key=lambda t: (t.topic, t.partition)):
            low, high = c.get_watermark_offsets(
                TopicPartition(tp.topic, tp.partition),
                timeout=settings.kafkascope_request_timeout,
            )
            offsets.append(
                PartitionOffset(
                    topic=tp.topic,
                    partition=tp.partition,
                    committed=tp.offset,
                    low=low,
                    high=high,
                )
            )

    coordinator = ""
    if d.coordinator is not None:
        coordinator = f"{d.coordinator.host}:{d.coordinator.port} (broker {d.coordinator.id})"

    # A group with no committed offsets and no members isn't listed by describe as
    # missing, so an unknown id still returns a shell. Treat that as not found.
    if not members and not offsets and _state_name(d.state) == "DEAD":
        return None

    return GroupDetail(
        id=group_id,
        state=_state_name(d.state),
        is_simple=d.is_simple_consumer_group,
        coordinator=coordinator,
        members=members,
        offsets=offsets,
    )


def _resolve_targets(
    topic: str,
    partitions: list[int],
    target: ResetTarget,
    timestamp_ms: int | None,
    offset: int | None,
) -> list[TopicPartition]:
    with consumer() as c:
        if target == "timestamp":
            if timestamp_ms is None:
                raise ValueError("A timestamp is required for a timestamp reset")
            wanted = [TopicPartition(topic, p, timestamp_ms) for p in partitions]
            resolved = c.offsets_for_times(
                wanted, timeout=settings.kafkascope_request_timeout
            )
            out = []
            for tp in resolved:
                if tp.offset < 0:
                    # Nothing at/after that time: park at the end of the partition.
                    _, high = c.get_watermark_offsets(
                        TopicPartition(topic, tp.partition),
                        timeout=settings.kafkascope_request_timeout,
                    )
                    tp.offset = high
                out.append(tp)
            return out

        if target == "offset":
            if offset is None:
                raise ValueError("An offset is required for a specific-offset reset")
            if len(partitions) != 1:
                raise ValueError("A specific offset can only be set on a single partition")
            p = partitions[0]
            low, high = c.get_watermark_offsets(
                TopicPartition(topic, p), timeout=settings.kafkascope_request_timeout
            )
            if not low <= offset <= high:
                raise ValueError(
                    f"Offset {offset} is outside partition {p}'s range ({low}–{high})"
                )
            return [TopicPartition(topic, p, offset)]

        marks = {
            p: c.get_watermark_offsets(
                TopicPartition(topic, p), timeout=settings.kafkascope_request_timeout
            )
            for p in partitions
        }

    index = 0 if target == "earliest" else 1
    return [TopicPartition(topic, p, marks[p][index]) for p in partitions]


def _require_empty(group_id: str, action: str) -> None:
    """Kafka rejects offset changes on a group with members, but says so cryptically
    ('Unknown member'). Check first so the user gets told what to actually do."""
    group = get_group(group_id)
    if group is None:
        raise LookupError(f"Consumer group {group_id!r} not found")
    if not group.is_empty:
        clients = ", ".join(sorted({m.client_id for m in group.members})) or "unknown"
        raise ValueError(
            f"Cannot {action}: the group is {group.state.lower()} with "
            f"{len(group.members)} active member(s) ({clients}). Stop the consumer(s) first."
        )


def reset_offsets(
    group_id: str,
    topic: str,
    partition: int | None,
    target: ResetTarget,
    timestamp_ms: int | None = None,
    offset: int | None = None,
) -> list[PartitionOffset]:
    """Move a group's committed offsets. The group must have no active members."""
    if is_readonly():
        raise PermissionError("This cluster is read-only")

    _require_empty(group_id, "reset offsets")

    md = admin().list_topics(timeout=settings.kafkascope_request_timeout)
    topic_md = md.topics.get(topic)
    if topic_md is None or topic_md.error is not None:
        raise LookupError(f"Topic {topic!r} not found")

    all_partitions = sorted(topic_md.partitions.keys())
    if partition is not None:
        if partition not in all_partitions:
            raise LookupError(f"Partition {partition} not in topic {topic!r}")
        partitions = [partition]
    else:
        partitions = all_partitions

    targets = _resolve_targets(topic, partitions, target, timestamp_ms, offset)

    request = ConsumerGroupTopicPartitions(group_id, targets)
    future = admin().alter_consumer_group_offsets([request])[group_id]
    # Kafka rejects this outright if the group still has members — let that surface.
    result = future.result(timeout=settings.kafkascope_request_timeout)

    applied = []
    with consumer() as c:
        for tp in result.topic_partitions:
            if tp.error is not None:
                raise RuntimeError(f"partition {tp.partition}: {tp.error.str()}")
            low, high = c.get_watermark_offsets(
                TopicPartition(topic, tp.partition),
                timeout=settings.kafkascope_request_timeout,
            )
            applied.append(
                PartitionOffset(
                    topic=tp.topic,
                    partition=tp.partition,
                    committed=tp.offset,
                    low=low,
                    high=high,
                )
            )
    return sorted(applied, key=lambda o: o.partition)


def group_total_lag(group_id: str) -> int | None:
    """Total lag for one group, or None if it's gone. Fetched on demand per row so
    the groups list stays a single cheap describe call and only the rows actually on
    screen pay for the offset/watermark round trips."""
    group = get_group(group_id)
    return group.total_lag if group is not None else None


def delete_group(group_id: str) -> None:
    if is_readonly():
        raise PermissionError("This cluster is read-only")

    _require_empty(group_id, "delete the group")

    future = admin().delete_consumer_groups(
        [group_id], request_timeout=settings.kafkascope_request_timeout
    )[group_id]
    future.result(timeout=settings.kafkascope_request_timeout)
