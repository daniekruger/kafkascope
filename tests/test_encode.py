"""Serializing a JSON value to the registry wire format, and back through decode."""

import json

import pytest

from app.services import decode as decode_mod
from app.services.decode import decode
from app.services.encode import encode_avro, encode_json_schema
from app.services.schema_registry import RegisteredSchema

AVRO_SCHEMA = {
    "type": "record",
    "name": "User",
    "fields": [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}],
}


class FakeRegistry:
    def __init__(self, schemas):
        self._schemas = schemas

    def schema_by_id(self, schema_id):
        return self._schemas.get(schema_id)


def _use(monkeypatch, schemas):
    monkeypatch.setattr(decode_mod, "registry_for_cluster", lambda: FakeRegistry(schemas))
    decode_mod._avro_cache.clear()


def test_encode_avro_roundtrips_through_decode(monkeypatch):
    schema_str = json.dumps(AVRO_SCHEMA)
    raw = encode_avro(schema_str, 7, '{"id": 5, "name": "ava"}')
    assert raw[0] == 0 and int.from_bytes(raw[1:5], "big") == 7

    _use(monkeypatch, {7: RegisteredSchema(7, "AVRO", schema_str)})
    d = decode(raw)
    assert d.encoding == "avro" and d.schema_id == 7
    assert json.loads(d.text) == {"id": 5, "name": "ava"}


def test_encode_json_schema_roundtrips(monkeypatch):
    raw = encode_json_schema(3, '{"x": [1, 2]}')
    _use(monkeypatch, {3: RegisteredSchema(3, "JSON", "{}")})
    d = decode(raw)
    assert d.encoding == "json-schema"
    assert json.loads(d.text) == {"x": [1, 2]}


def test_encode_avro_rejects_bad_json():
    with pytest.raises(ValueError, match="not valid JSON"):
        encode_avro(json.dumps(AVRO_SCHEMA), 7, "this is not json")


def test_encode_avro_rejects_wrong_shape():
    # "id" must be an int; a string fails to encode against the schema.
    with pytest.raises(ValueError, match="does not match the Avro schema"):
        encode_avro(json.dumps(AVRO_SCHEMA), 7, '{"id": "nope", "name": "x"}')


def test_encode_json_rejects_bad_json():
    with pytest.raises(ValueError, match="not valid JSON"):
        encode_json_schema(3, "{broken")


# --- schema resolution (_encode_field: reuse vs register), used for both key and value ---

from app.services import produce as produce_svc  # noqa: E402
from app.services import schema_registry as sr_mod  # noqa: E402


class _Reg:
    def __init__(self, latest=None, reg_id=None):
        self._latest, self._reg_id, self.registered = latest, reg_id, []

    def latest_by_subject(self, subject):
        return self._latest

    def register(self, subject, schema_str, schema_type):
        self.registered.append((subject, schema_type))
        return self._reg_id


def test_encode_field_reuses_registered_schema(monkeypatch):
    reg = _Reg(latest=RegisteredSchema(9, "AVRO", json.dumps(AVRO_SCHEMA)))
    monkeypatch.setattr(sr_mod, "registry_for_cluster", lambda: reg)
    data, sid, enc = produce_svc._encode_field("t-value", "avro", "", False, '{"id": 1, "name": "a"}')
    assert sid == 9 and enc == "avro" and data[0] == 0
    assert reg.registered == []  # reuse must not write to the registry


def test_encode_field_registers_when_asked(monkeypatch):
    reg = _Reg(reg_id=12)
    monkeypatch.setattr(sr_mod, "registry_for_cluster", lambda: reg)
    data, sid, enc = produce_svc._encode_field(
        "t-key", "avro", json.dumps(AVRO_SCHEMA), True, '{"id": 1, "name": "a"}'
    )
    assert sid == 12 and reg.registered == [("t-key", "AVRO")]


def test_encode_field_without_registry(monkeypatch):
    monkeypatch.setattr(sr_mod, "registry_for_cluster", lambda: None)
    with pytest.raises(ValueError, match="No schema registry"):
        produce_svc._encode_field("t-value", "avro", "", False, "{}")


def test_encode_field_missing_schema(monkeypatch):
    monkeypatch.setattr(sr_mod, "registry_for_cluster", lambda: _Reg(latest=None))
    with pytest.raises(ValueError, match="No schema registered"):
        produce_svc._encode_field("t-value", "avro", "", False, "{}")
