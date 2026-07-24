"""Router-level typed-confirmation logic.

These exercise the confirm-guard branch directly (which lives in the routers, not
the services) without needing a broker: a wrong confirmation must be rejected
before any Kafka call happens, so the handler returns the error fragment.
"""

from fastapi.testclient import TestClient

from app.config import settings
from app.main import app

client = TestClient(app)

# Every mutating route now lives under the cluster prefix.
C = settings.default_cluster.name


def test_topic_delete_wrong_confirm_is_rejected():
    r = client.post(f"/c/{C}/topic/some-topic/delete", data={"confirm": "WRONG"})
    assert r.status_code == 200
    assert "Type the topic name" in r.text
    # Nothing should indicate a delete happened.
    assert "Deleted topic" not in r.text


def test_topic_purge_wrong_confirm_is_rejected():
    r = client.post(f"/c/{C}/topic/some-topic/purge", data={"confirm": ""})
    assert "Type the topic name" in r.text
    assert "record(s) removed" not in r.text


def test_group_reset_wrong_confirm_is_rejected():
    r = client.post(
        f"/c/{C}/group/some-group/reset",
        data={"topic": "t", "target": "earliest", "confirm": "nope"},
    )
    assert "Type the group id" in r.text


def test_group_delete_wrong_confirm_is_rejected():
    r = client.post(f"/c/{C}/group/some-group/delete", data={"confirm": ""})
    assert "Type the group id" in r.text


def test_unknown_cluster_is_404():
    r = client.post("/c/nope/topic/some-topic/delete", data={"confirm": "x"})
    assert r.status_code == 404


def test_healthz_reports_broker():
    r = client.get("/healthz")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"
