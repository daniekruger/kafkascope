"""Kafka access layer.

confluent-kafka is synchronous and blocking, so every call here is designed to be
invoked via `run_in_threadpool` from the routers rather than awaited directly.

Which cluster a call targets is resolved from a ContextVar rather than threaded
through every service signature. A request sets it once (the select_cluster
dependency, from the /c/<name> URL prefix); it then propagates into the worker
threads that `run_in_threadpool` / `asyncio.to_thread` spin up, because both copy
the current context. Outside a request (tests, startup) it falls back to the
default cluster, so single-cluster code paths are unchanged.
"""

from contextlib import contextmanager
from contextvars import ContextVar

from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient

from .config import ClusterConfig, settings

_current_cluster: ContextVar[ClusterConfig | None] = ContextVar(
    "current_cluster", default=None
)


def set_current_cluster(cluster: ClusterConfig) -> None:
    _current_cluster.set(cluster)


def current_cluster() -> ClusterConfig:
    cluster = _current_cluster.get()
    return cluster if cluster is not None else settings.default_cluster


def is_readonly(cluster: ClusterConfig | None = None) -> bool:
    """A write is blocked if the cluster is marked read-only, or the whole instance
    is (the global KAFKASCOPE_READONLY kill switch)."""
    cluster = cluster or current_cluster()
    return cluster.readonly or settings.kafkascope_readonly


def base_config(cluster: ClusterConfig | None = None) -> dict:
    """The librdkafka config a cluster's clients share: brokers plus security.

    Only non-empty security keys are emitted, so a PLAINTEXT dev broker gets a
    clean two-key config and a secured cluster gets exactly what it was given.
    This is the single place connection security is assembled.
    """
    cluster = cluster or current_cluster()
    conf: dict[str, object] = {
        "bootstrap.servers": cluster.brokers,
        "security.protocol": cluster.security_protocol,
    }
    optional = {
        "sasl.mechanism": cluster.sasl_mechanism,
        "sasl.username": cluster.sasl_username,
        "sasl.password": cluster.sasl_password,
        "ssl.ca.location": cluster.ssl_ca_location,
        "ssl.certificate.location": cluster.ssl_certificate_location,
        "ssl.key.location": cluster.ssl_key_location,
        "ssl.key.password": cluster.ssl_key_password,
        "ssl.endpoint.identification.algorithm": (
            cluster.ssl_endpoint_identification_algorithm
        ),
    }
    conf.update({k: v for k, v in optional.items() if v})
    return conf


# One long-lived AdminClient / Producer per cluster, keyed by name (not lru_cache,
# which a no-arg call can't key on the current cluster).
_admins: dict[str, AdminClient] = {}
_producers: dict[str, Producer] = {}


def admin(cluster: ClusterConfig | None = None) -> AdminClient:
    cluster = cluster or current_cluster()
    client = _admins.get(cluster.name)
    if client is None:
        client = AdminClient(base_config(cluster))
        _admins[cluster.name] = client
    return client


def producer(cluster: ClusterConfig | None = None) -> Producer:
    cluster = cluster or current_cluster()
    client = _producers.get(cluster.name)
    if client is None:
        client = Producer(
            {
                **base_config(cluster),
                "client.id": "kafkascope",
                # Never silently drop or reorder a hand-sent message.
                "acks": "all",
                "enable.idempotence": True,
            }
        )
        _producers[cluster.name] = client
    return client


@contextmanager
def consumer(group_id: str = "kafkascope-inspector", cluster: ClusterConfig | None = None):
    """A consumer that never joins a group or commits — used for watermarks and scans."""
    c = Consumer(
        {
            **base_config(cluster),
            "group.id": group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        yield c
    finally:
        c.close()


def watermarks(topic: str, partitions: list[int]) -> dict[int, tuple[int, int]]:
    """Return {partition: (low, high)} offsets."""
    with consumer() as c:
        return {
            p: c.get_watermark_offsets(
                TopicPartition(topic, p), timeout=settings.kafkascope_request_timeout
            )
            for p in partitions
        }
