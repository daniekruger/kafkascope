"""The shipped example schemas must actually encode and decode — so the docs and the
Produce form's copy-paste examples can't silently rot."""

import json
from pathlib import Path

import pytest

from app.services import decode as decode_mod
from app.services.decode import decode
from app.services.encode import encode_avro, encode_json_schema
from app.services.schema_registry import RegisteredSchema

EXAMPLES = Path(__file__).resolve().parent.parent / "examples"
pytestmark = pytest.mark.skipif(not EXAMPLES.exists(), reason="examples/ not available")


class FakeRegistry:
    def __init__(self, schemas):
        self._schemas = schemas

    def schema_by_id(self, schema_id):
        return self._schemas.get(schema_id)


def _read(name: str) -> str:
    return (EXAMPLES / name).read_text()


def test_avro_example_roundtrips(monkeypatch):
    schema, value = _read("orders.avsc"), _read("orders-value.json")
    raw = encode_avro(schema, 1, value)  # also asserts the value matches the schema

    monkeypatch.setattr(decode_mod, "registry_for_cluster", lambda: FakeRegistry({1: RegisteredSchema(1, "AVRO", schema)}))
    decode_mod._avro_cache.clear()
    decoded = decode(raw)

    assert decoded.encoding == "avro"
    assert json.loads(decoded.text) == json.loads(value)


def test_json_schema_example_roundtrips(monkeypatch):
    schema, value = _read("page-view.schema.json"), _read("page-view-value.json")
    raw = encode_json_schema(2, value)

    monkeypatch.setattr(decode_mod, "registry_for_cluster", lambda: FakeRegistry({2: RegisteredSchema(2, "JSON", schema)}))
    decoded = decode(raw)

    assert decoded.encoding == "json-schema"
    assert json.loads(decoded.text) == json.loads(value)
