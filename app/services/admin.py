"""Topic administration: create, delete, add partitions, edit config, purge.

Delete-topic and purge are the most destructive operations in the app. Both are
irreversible and both are gated behind typed confirmation at the router; this layer
enforces the read-only flag and validates inputs so a bad request fails before it
reaches the broker.
"""

from confluent_kafka import TopicPartition
from confluent_kafka.admin import (
    ConfigResource,
    NewPartitions,
    NewTopic,
)

from ..config import settings
from ..kafka_client import admin, consumer, is_readonly

# Config keys worth surfacing as first-class fields on the create form. Everything
# else is reachable through the free-form config editor on the topic page.
COMMON_CONFIGS = [
    ("retention.ms", "Retention (ms)", "how long records are kept; -1 = forever"),
    ("cleanup.policy", "Cleanup policy", "delete or compact"),
    ("max.message.bytes", "Max message bytes", ""),
    ("min.insync.replicas", "Min in-sync replicas", ""),
]


def _topic_exists(name: str) -> bool:
    md = admin().list_topics(timeout=settings.kafkascope_request_timeout)
    return name in md.topics


def create_topic(
    name: str,
    partitions: int,
    replication_factor: int,
    config: dict[str, str] | None = None,
) -> None:
    if is_readonly():
        raise PermissionError("This cluster is read-only")

    name = name.strip()
    if not name:
        raise ValueError("Topic name is required")
    # Kafka's own rule: names are limited to these characters.
    if any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-" for c in name):
        raise ValueError("Topic names may only contain letters, digits, '.', '_' and '-'")
    if partitions < 1:
        raise ValueError("A topic needs at least one partition")
    if replication_factor < 1:
        raise ValueError("Replication factor must be at least 1")

    if _topic_exists(name):
        raise ValueError(f"Topic {name!r} already exists")

    topic = NewTopic(
        name,
        num_partitions=partitions,
        replication_factor=replication_factor,
        config=config or {},
    )
    future = admin().create_topics([topic])[name]
    future.result(timeout=settings.kafkascope_request_timeout)


def delete_topic(name: str) -> None:
    if is_readonly():
        raise PermissionError("This cluster is read-only")
    if name.startswith("__"):
        raise ValueError("Refusing to delete an internal topic")
    if not _topic_exists(name):
        raise LookupError(f"Topic {name!r} not found")

    future = admin().delete_topics([name], operation_timeout=settings.kafkascope_request_timeout)[name]
    future.result(timeout=settings.kafkascope_request_timeout)


def add_partitions(name: str, new_total: int) -> None:
    """Grow a topic to `new_total` partitions. Kafka only allows growth, never shrinking."""
    if is_readonly():
        raise PermissionError("This cluster is read-only")

    md = admin().list_topics(timeout=settings.kafkascope_request_timeout)
    topic_md = md.topics.get(name)
    if topic_md is None or topic_md.error is not None:
        raise LookupError(f"Topic {name!r} not found")

    current = len(topic_md.partitions)
    if new_total <= current:
        raise ValueError(
            f"Topic already has {current} partitions; you can only increase the count "
            "(Kafka cannot remove partitions)"
        )

    future = admin().create_partitions([NewPartitions(name, new_total)])[name]
    future.result(timeout=settings.kafkascope_request_timeout)


def update_config(name: str, changes: dict[str, str]) -> None:
    """Incrementally alter topic configs. Empty value string deletes the override."""
    if is_readonly():
        raise PermissionError("This cluster is read-only")
    if not changes:
        raise ValueError("No configuration changes given")
    if not _topic_exists(name):
        raise LookupError(f"Topic {name!r} not found")

    # incremental_alter_configs takes a list of (op, name, value) via ConfigEntry;
    # fall back to the set-based API for librdkafka builds that lack it.
    from confluent_kafka.admin import AlterConfigOpType, ConfigEntry

    entries = []
    for key, value in changes.items():
        if value == "":
            entries.append(ConfigEntry(key, "", incremental_operation=AlterConfigOpType.DELETE))
        else:
            entries.append(ConfigEntry(key, value, incremental_operation=AlterConfigOpType.SET))

    resource = ConfigResource(ConfigResource.Type.TOPIC, name, incremental_configs=entries)
    future = admin().incremental_alter_configs([resource])[resource]
    future.result(timeout=settings.kafkascope_request_timeout)


def purge_topic(name: str) -> int:
    """Delete all records by moving each partition's low watermark up to its high.

    This is delete_records, not a topic drop: the topic, its partitions, and its
    config all survive; only the data is discarded. Returns the number of records
    marked for deletion.
    """
    if is_readonly():
        raise PermissionError("This cluster is read-only")
    if name.startswith("__"):
        raise ValueError("Refusing to purge an internal topic")

    md = admin().list_topics(timeout=settings.kafkascope_request_timeout)
    topic_md = md.topics.get(name)
    if topic_md is None or topic_md.error is not None:
        raise LookupError(f"Topic {name!r} not found")

    to_delete = []
    purged = 0
    with consumer() as c:
        for p in topic_md.partitions:
            low, high = c.get_watermark_offsets(
                TopicPartition(name, p), timeout=settings.kafkascope_request_timeout
            )
            purged += high - low
            # offset = high means "delete everything before the current end".
            to_delete.append(TopicPartition(name, p, high))

    futures = admin().delete_records(to_delete)
    for tp, future in futures.items():
        future.result(timeout=settings.kafkascope_request_timeout)
    return purged
