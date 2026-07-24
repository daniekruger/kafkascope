import pytest


def _broker_reachable() -> bool:
    # Imported lazily so this conftest also loads in environments without the Kafka
    # client installed (e.g. the Playwright image, which only drives HTTP).
    try:
        from confluent_kafka.admin import AdminClient

        from app.kafka_client import base_config

        md = AdminClient(base_config()).list_topics(timeout=3)
        return md is not None
    except Exception:
        return False


BROKER_UP = _broker_reachable()


@pytest.fixture
def skip_without_broker():
    if not BROKER_UP:
        pytest.skip("no Kafka broker reachable")
