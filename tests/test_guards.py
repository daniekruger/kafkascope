"""The safety-critical guardrails: read-only enforcement, internal-topic refusal,
input validation. These are exactly the checks a future change could silently
break, so they get explicit coverage that runs without a broker — every guard
here short-circuits before any network call.
"""

import pytest

from app.config import settings
from app.services import admin, groups, produce


@pytest.fixture
def readonly(monkeypatch):
    # All service modules share the one Settings singleton, so one patch covers them.
    monkeypatch.setattr(settings, "kafkascope_readonly", True)
    yield


# --- read-only blocks every mutation, in the service layer itself ---

def test_readonly_blocks_create(readonly):
    with pytest.raises(PermissionError):
        admin.create_topic("t", 1, 1)


def test_readonly_blocks_delete(readonly):
    with pytest.raises(PermissionError):
        admin.delete_topic("t")


def test_readonly_blocks_purge(readonly):
    with pytest.raises(PermissionError):
        admin.purge_topic("t")


def test_readonly_blocks_add_partitions(readonly):
    with pytest.raises(PermissionError):
        admin.add_partitions("t", 4)


def test_readonly_blocks_update_config(readonly):
    with pytest.raises(PermissionError):
        admin.update_config("t", {"retention.ms": "1"})


def test_readonly_blocks_produce(readonly):
    with pytest.raises(PermissionError):
        produce.send("t", key="k", value="v")


def test_readonly_blocks_reset_offsets(readonly):
    with pytest.raises(PermissionError):
        groups.reset_offsets("g", "t", None, "earliest")


def test_readonly_blocks_delete_group(readonly):
    with pytest.raises(PermissionError):
        groups.delete_group("g")


# --- internal topics are refused regardless of confirmation ---

def test_delete_internal_topic_refused():
    with pytest.raises(ValueError, match="internal"):
        admin.delete_topic("__consumer_offsets")


def test_purge_internal_topic_refused():
    with pytest.raises(ValueError, match="internal"):
        admin.purge_topic("__consumer_offsets")


# --- create-topic input validation (all before any broker call) ---

def test_create_rejects_empty_name():
    with pytest.raises(ValueError, match="name is required"):
        admin.create_topic("   ", 1, 1)


def test_create_rejects_bad_characters():
    with pytest.raises(ValueError, match="may only contain"):
        admin.create_topic("bad name!", 1, 1)


def test_create_rejects_zero_partitions():
    with pytest.raises(ValueError, match="at least one partition"):
        admin.create_topic("ok", 0, 1)


def test_create_rejects_zero_replication():
    with pytest.raises(ValueError, match="Replication factor"):
        admin.create_topic("ok", 1, 0)
