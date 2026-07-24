from dataclasses import dataclass

from confluent_kafka.admin import ConfigResource

from ..config import settings
from ..kafka_client import admin, watermarks


@dataclass
class Broker:
    id: int
    host: str
    port: int
    is_controller: bool


@dataclass
class PartitionInfo:
    id: int
    leader: int
    replicas: list[int]
    isrs: list[int]
    low: int
    high: int

    @property
    def size(self) -> int:
        return self.high - self.low

    @property
    def under_replicated(self) -> bool:
        return len(self.isrs) < len(self.replicas)


@dataclass
class TopicSummary:
    name: str
    partition_count: int
    replication_factor: int
    under_replicated: bool
    internal: bool


@dataclass
class TopicDetail:
    name: str
    partitions: list[PartitionInfo]
    config: dict[str, str]

    @property
    def size(self) -> int:
        return sum(p.size for p in self.partitions)


def _metadata():
    return admin().list_topics(timeout=settings.kafkascope_request_timeout)


def get_brokers() -> tuple[list[Broker], int]:
    md = _metadata()
    brokers = [
        Broker(
            id=b.id,
            host=b.host,
            port=b.port,
            is_controller=(b.id == md.controller_id),
        )
        for b in sorted(md.brokers.values(), key=lambda b: b.id)
    ]
    return brokers, md.controller_id


def list_topics() -> list[TopicSummary]:
    # One metadata round trip, no watermarks. Message counts used to be computed
    # here with a get_watermark_offsets call per partition of every topic — O(topics
    # × partitions) sequential round trips that made the overview unusable on a large
    # cluster. Counts are now fetched lazily per topic (see topic_message_count).
    md = _metadata()
    summaries = []

    for name, t in md.topics.items():
        replicas = [len(p.replicas) for p in t.partitions.values()]
        summaries.append(
            TopicSummary(
                name=name,
                partition_count=len(t.partitions),
                replication_factor=max(replicas) if replicas else 0,
                under_replicated=any(
                    len(p.isrs) < len(p.replicas) for p in t.partitions.values()
                ),
                internal=name.startswith("__"),
            )
        )

    return sorted(summaries, key=lambda t: (t.internal, t.name))


def topic_message_count(name: str) -> int | None:
    """Sum of (high - low) across a topic's partitions, or None if it's gone.

    Fetched on demand for one topic at a time, so the overview's total cost is
    O(visible rows) rather than O(all topics × partitions).
    """
    md = _metadata()
    t = md.topics.get(name)
    if t is None or t.error is not None:
        return None
    marks = watermarks(name, list(t.partitions.keys()))
    return sum(high - low for low, high in marks.values())


def get_topic(name: str) -> TopicDetail | None:
    # Deliberately fetch *all* metadata and look the name up locally. Asking the
    # broker for one topic by name auto-creates it when auto.create.topics.enable
    # is on (the default), so a mistyped URL would silently create a topic.
    md = _metadata()
    t = md.topics.get(name)
    if t is None or t.error is not None:
        return None

    marks = watermarks(name, list(t.partitions.keys()))
    partitions = [
        PartitionInfo(
            id=p.id,
            leader=p.leader,
            replicas=list(p.replicas),
            isrs=list(p.isrs),
            low=marks[p.id][0],
            high=marks[p.id][1],
        )
        for p in sorted(t.partitions.values(), key=lambda p: p.id)
    ]

    return TopicDetail(name=name, partitions=partitions, config=_topic_config(name))


def _topic_config(name: str) -> dict[str, str]:
    resource = ConfigResource(ConfigResource.Type.TOPIC, name)
    future = admin().describe_configs([resource])[resource]
    entries = future.result(timeout=settings.kafkascope_request_timeout)
    # Defaults are noise — only surface what's been explicitly set on the topic.
    return {
        k: v.value for k, v in sorted(entries.items()) if not v.is_default
    }
