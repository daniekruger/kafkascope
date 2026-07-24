"""End-to-end round trip against a real broker.

Skipped automatically when no broker is reachable, so the unit suite still runs
anywhere. Uses a uniquely-named throwaway topic and always cleans it up, so it
never touches anything else on the cluster.
"""

import time

import pytest

from app.services import admin, messages, produce

pytestmark = pytest.mark.integration

TOPIC = "pytest-kafkascope-tmp"


def _wait_until(predicate, timeout=15.0):
    """Kafka topic create/delete propagates asynchronously; poll until it lands."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.2)
    raise AssertionError("cluster metadata did not converge in time")


@pytest.fixture
def temp_topic(skip_without_broker):
    # Start from a clean slate even if a previous run died mid-way, and wait for the
    # delete to actually propagate before recreating — otherwise the create races it.
    try:
        admin.delete_topic(TOPIC)
    except Exception:
        pass
    _wait_until(lambda: not admin._topic_exists(TOPIC))

    admin.create_topic(TOPIC, partitions=2, replication_factor=1, config={"retention.ms": "3600000"})
    _wait_until(lambda: admin._topic_exists(TOPIC))

    yield TOPIC

    try:
        admin.delete_topic(TOPIC)
    except Exception:
        pass
    _wait_until(lambda: not admin._topic_exists(TOPIC))


def _drain(spec):
    """Run a scan to completion and return only the hit messages."""
    import asyncio

    async def go():
        out = []

        async def never():
            return False

        async for kind, item in messages.scan(spec, never):
            if kind == "hit":
                out.append(item)
        return out

    return asyncio.run(go())


def test_create_produce_scan_purge_delete(temp_topic):
    # produce a handful, including headers and a specific partition
    for i in range(5):
        produce.send(
            temp_topic,
            key=f"k{i}",
            value=f'{{"i": {i}}}',
            headers=[("src", "itest")],
            partition=0,
        )
    produce.send(temp_topic, key="other", value='{"i": 99}', partition=1)

    detail = admin_topic(temp_topic)
    assert detail.size == 6

    # a filtered scan finds exactly the JSON we expect
    hits = _drain(messages.ScanSpec(topic=temp_topic, start="earliest", json_path="i", json_value="99"))
    assert len(hits) == 1
    assert hits[0].key == "other"

    # header filter
    hits = _drain(messages.ScanSpec(topic=temp_topic, start="earliest", header_key="src", header_value="itest"))
    assert len(hits) == 5

    # purge empties the data but keeps the topic
    purged = admin.purge_topic(temp_topic)
    assert purged == 6
    assert admin_topic(temp_topic).size == 0


def test_add_partitions_only_grows(temp_topic):
    from app.services import cluster

    before = len(cluster.get_topic(temp_topic).partitions)
    admin.add_partitions(temp_topic, before + 1)
    assert len(cluster.get_topic(temp_topic).partitions) == before + 1

    with pytest.raises(ValueError, match="increase"):
        admin.add_partitions(temp_topic, before)  # shrinking is refused


def test_create_duplicate_refused(temp_topic):
    with pytest.raises(ValueError, match="already exists"):
        admin.create_topic(temp_topic, 1, 1)


def admin_topic(name):
    from app.services import cluster

    return cluster.get_topic(name)
