"""The Confluent wire-format decoder: Avro, JSON Schema, Protobuf, and fallbacks.

No live registry — a fake one is injected so the decode logic is tested in isolation.
"""

import io
import json

import pytest

from app.services import decode as decode_mod
from app.services.decode import (
    Decoded,
    decode,
    decode_protobuf,
    _read_varint,
    _strip_protobuf_index,
)
from app.services.schema_registry import RegisteredSchema


class FakeRegistry:
    def __init__(self, schemas):
        self._schemas = schemas  # {id: RegisteredSchema}

    def schema_by_id(self, schema_id):
        return self._schemas.get(schema_id)


@pytest.fixture
def use_registry(monkeypatch):
    """Install a fake registry with the given {id: RegisteredSchema}."""
    def install(schemas):
        monkeypatch.setattr(decode_mod, "registry_for_cluster", lambda: FakeRegistry(schemas))
        decode_mod._avro_cache.clear()
    return install


def _wire(schema_id: int, body: bytes) -> bytes:
    return b"\x00" + schema_id.to_bytes(4, "big") + body


# --- plain payloads (no registry involved) ---

def test_plain_utf8(monkeypatch):
    monkeypatch.setattr(decode_mod, "registry_for_cluster", lambda: None)
    d = decode(b'{"a": 1}')
    assert d == Decoded('{"a": 1}', False, "string", None)


def test_none_is_null():
    assert decode(None).encoding == "null"


def test_binary_without_registry_is_base64(monkeypatch):
    monkeypatch.setattr(decode_mod, "registry_for_cluster", lambda: None)
    d = decode(b"\x00\x01\x02\x9f")  # leads with 0x00 but no registry configured
    assert d.binary is True and d.encoding == "binary"


# --- Avro ---

def test_avro_roundtrip(use_registry):
    import fastavro

    schema = {
        "type": "record",
        "name": "Order",
        "fields": [
            {"name": "id", "type": "int"},
            {"name": "customer", "type": "string"},
            {"name": "paid", "type": "boolean"},
        ],
    }
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, schema, {"id": 42, "customer": "ava", "paid": True})
    use_registry({7: RegisteredSchema(7, "AVRO", json.dumps(schema))})

    d = decode(_wire(7, buf.getvalue()))
    assert d.encoding == "avro" and d.schema_id == 7 and d.binary is False
    assert json.loads(d.text) == {"id": 42, "customer": "ava", "paid": True}


def test_avro_decode_failure_is_flagged(use_registry):
    schema = {"type": "record", "name": "R", "fields": [{"name": "id", "type": "int"}]}
    use_registry({7: RegisteredSchema(7, "AVRO", json.dumps(schema))})
    d = decode(_wire(7, b"\xff\xff"))  # not valid Avro for this schema
    assert d.schema_id == 7 and "decode failed" in d.text


# --- JSON Schema ---

def test_json_schema(use_registry):
    use_registry({3: RegisteredSchema(3, "JSON", "{}")})
    d = decode(_wire(3, b'{"x": [1, 2]}'))
    assert d.encoding == "json-schema"
    assert json.loads(d.text) == {"x": [1, 2]}


# --- registry configured but id unknown → graceful fallback ---

def test_unknown_schema_id_falls_back(use_registry):
    use_registry({})  # registry present, but returns None for any id
    d = decode(_wire(999, b"\x01\x02"))
    assert d.encoding == "binary" and d.binary is True


# --- Protobuf wire format ---

def test_varint_and_index_strip():
    assert _read_varint(b"\x96\x01", 0) == (150, 2)
    # Single-message index is a lone 0x00; the body follows.
    assert _strip_protobuf_index(b"\x00\x08\x2a") == b"\x08\x2a"
    # Explicit count=1, index=0, then body.
    assert _strip_protobuf_index(b"\x01\x00\x08\x2a") == b"\x08\x2a"


def test_protobuf_scalar_and_string():
    # field 1 (varint) = 42; field 2 (length-delimited) = "hi"
    data = b"\x08\x2a" + b"\x12\x02hi"
    assert decode_protobuf(data) == {"field_1": 42, "field_2": "hi"}


def test_protobuf_repeated_and_nested():
    # field 1 varint 1, field 1 varint 2 (repeated) → list
    # field 3 length-delimited containing a sub-message {field 1: 7}
    nested = b"\x08\x07"
    data = b"\x08\x01" + b"\x08\x02" + b"\x1a" + bytes([len(nested)]) + nested
    out = decode_protobuf(data)
    assert out["field_1"] == [1, 2]
    assert out["field_3"] == {"field_1": 7}


def test_protobuf_end_to_end(use_registry):
    use_registry({5: RegisteredSchema(5, "PROTOBUF", 'syntax = "proto3";')})
    body = b"\x00" + b"\x08\x2a\x12\x03ava"  # index 0, then field1=42, field2="ava"
    d = decode(_wire(5, body))
    assert d.encoding == "protobuf" and d.schema_id == 5
    assert json.loads(d.text) == {"field_1": 42, "field_2": "ava"}
