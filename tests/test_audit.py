"""The audit log records every mutation attempt — success, error, and denied."""

import json
import logging

import pytest
from fastapi.testclient import TestClient

from app.config import settings
from app.main import app

client = TestClient(app)

C = settings.default_cluster.name


class _Capture(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records: list[dict] = []

    def emit(self, record):
        self.records.append(json.loads(record.getMessage()))


@pytest.fixture
def audit_records():
    # The audit logger deliberately does not propagate (so lines aren't double-emitted
    # through uvicorn's root logger), so attach directly to it rather than via caplog.
    handler = _Capture()
    logger = logging.getLogger("kafkascope.audit")
    logger.addHandler(handler)
    try:
        yield handler.records
    finally:
        logger.removeHandler(handler)


def test_denied_delete_is_audited(audit_records):
    client.post(f"/c/{C}/topic/whatever/delete", data={"confirm": "WRONG"})
    assert any(
        r["action"] == "delete_topic" and r["outcome"] == "denied" for r in audit_records
    ), audit_records


def test_denied_group_reset_is_audited(audit_records):
    client.post(f"/c/{C}/group/g/reset", data={"topic": "t", "target": "earliest", "confirm": "no"})
    assert any(
        r["action"] == "reset_offsets" and r["outcome"] == "denied" for r in audit_records
    ), audit_records


def test_audit_record_has_identity_fields(audit_records):
    client.post(f"/c/{C}/topic/whatever/purge", data={"confirm": "WRONG"})
    assert audit_records
    r = audit_records[-1]
    assert {"ts", "user", "role", "cluster", "action", "target", "outcome"} <= set(r)
    assert r["cluster"] == C
