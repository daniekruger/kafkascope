"""Serialize a JSON value into the Confluent Schema Registry wire format.

The inverse of decode.py: take the JSON a user typed, encode it against a schema, and
prepend the magic byte + schema id so downstream consumers (and kafkascope's own
decoder) recognise it. Avro and JSON Schema only — Protobuf can't be serialized
generically without the compiled ``.proto``, so it stays decode-only.
"""

import io
import json


def _header(schema_id: int) -> bytes:
    return b"\x00" + schema_id.to_bytes(4, "big")


def encode_avro(schema_str: str, schema_id: int, json_text: str) -> bytes:
    """Encode JSON text as Avro binary under the given schema."""
    import fastavro  # lazy, mirrors decode.py

    try:
        value = json.loads(json_text)
    except ValueError as exc:
        raise ValueError(f"value is not valid JSON: {exc}") from exc

    parsed = fastavro.parse_schema(json.loads(schema_str))
    buf = io.BytesIO()
    try:
        fastavro.schemaless_writer(buf, parsed, value)
    except Exception as exc:
        # Wrong shape / type for the schema — fastavro's message names the offending field.
        raise ValueError(f"value does not match the Avro schema: {exc}") from exc
    return _header(schema_id) + buf.getvalue()


def encode_json_schema(schema_id: int, json_text: str) -> bytes:
    """Encode JSON text as a JSON-Schema payload (the body is the JSON itself)."""
    try:
        value = json.loads(json_text)
    except ValueError as exc:
        raise ValueError(f"value is not valid JSON: {exc}") from exc
    return _header(schema_id) + json.dumps(value, ensure_ascii=False).encode("utf-8")
