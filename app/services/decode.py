"""Decode a message payload for display.

Plain UTF-8 / JSON is shown as-is. Bytes written in the Confluent Schema Registry
wire format — a magic ``0x00`` byte, a 4-byte big-endian schema id, then the encoded
body — are decoded against the registry when one is configured:

* **Avro** and **JSON Schema** decode to their real JSON structure.
* **Protobuf** has no self-describing field names on the wire, and decoding them
  generically would need the ``.proto`` compiled. So this does a best-effort walk of
  the protobuf wire format, keyed by field number (``field_1`` …); the values are
  real, the names are not. The schema id is surfaced so the field map is discoverable.

Anything that isn't valid UTF-8 and isn't registry-encoded is shown base64.
"""

import base64
import io
import json
from dataclasses import dataclass

from .schema_registry import registry_for_cluster

_MAGIC = 0

# Cache parsed Avro schemas by id — parsing is the expensive part, not decoding.
_avro_cache: dict[int, object] = {}


@dataclass
class Decoded:
    text: str | None
    binary: bool  # True when shown base64 rather than as real text
    encoding: str  # "string" | "json" | "avro" | "json-schema" | "protobuf" | "binary" | "null"
    schema_id: int | None = None


def decode(raw: bytes | None) -> Decoded:
    """Best-effort decode of one key or value into displayable text."""
    if raw is None:
        return Decoded(None, False, "null")

    if len(raw) >= 5 and raw[0] == _MAGIC:
        decoded = _decode_registry(raw)
        if decoded is not None:
            return decoded
        # Not actually registry data (or registry unreachable): fall through.

    try:
        return Decoded(raw.decode("utf-8"), False, "string")
    except UnicodeDecodeError:
        return Decoded(base64.b64encode(raw).decode("ascii"), True, "binary")


def _decode_registry(raw: bytes) -> Decoded | None:
    registry = registry_for_cluster()
    if registry is None:
        return None
    schema_id = int.from_bytes(raw[1:5], "big")
    schema = registry.schema_by_id(schema_id)
    if schema is None:
        return None

    body = raw[5:]
    try:
        if schema.schema_type == "AVRO":
            obj = _decode_avro(schema_id, schema.schema_str, body)
            enc = "avro"
        elif schema.schema_type == "JSON":
            obj = json.loads(body)
            enc = "json-schema"
        elif schema.schema_type == "PROTOBUF":
            obj = decode_protobuf(_strip_protobuf_index(body))
            enc = "protobuf"
        else:
            return Decoded(base64.b64encode(body).decode("ascii"), True, "binary", schema_id)
    except Exception as exc:
        # The header claimed a schema but the body didn't decode against it. Say so
        # rather than silently mislabelling — a genuinely useful signal when debugging.
        return Decoded(f"<{schema.schema_type.lower()} decode failed: {exc}>", False, "binary", schema_id)

    return Decoded(json.dumps(obj, ensure_ascii=False, default=_json_default), False, enc, schema_id)


# --- Avro ---

def _decode_avro(schema_id: int, schema_str: str, body: bytes):
    import fastavro  # lazy: only needed when an Avro message is actually seen

    parsed = _avro_cache.get(schema_id)
    if parsed is None:
        parsed = fastavro.parse_schema(json.loads(schema_str))
        _avro_cache[schema_id] = parsed
    return fastavro.schemaless_reader(io.BytesIO(body), parsed)


def _json_default(obj):
    """Render Avro values JSON can't handle natively (bytes, datetimes, Decimal)."""
    if isinstance(obj, (bytes, bytearray)):
        return base64.b64encode(bytes(obj)).decode("ascii")
    # Decimal, datetime, date, time, UUID all round-trip fine through str().
    return str(obj)


# --- Protobuf (generic, field-number keyed) ---

def _read_varint(data: bytes, pos: int) -> tuple[int, int]:
    result = shift = 0
    while True:
        if pos >= len(data):
            raise ValueError("truncated varint")
        b = data[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return result, pos
        shift += 7
        if shift > 63:
            raise ValueError("varint too long")


def _strip_protobuf_index(body: bytes) -> bytes:
    """Drop the Confluent message-index that precedes a protobuf body.

    It's a varint count followed by that many varint indexes; the common single-message
    case is encoded as a lone ``0x00``.
    """
    count, pos = _read_varint(body, 0)
    if count == 0:
        return body[pos:]
    for _ in range(count):
        _, pos = _read_varint(body, pos)
    return body[pos:]


def decode_protobuf(data: bytes) -> dict:
    """Walk protobuf wire bytes into a dict keyed by field number.

    Names aren't on the wire, so keys are ``field_<n>``. Repeated fields collect into
    a list; length-delimited values are shown as string, nested message, or base64.
    """
    result: dict[str, object] = {}
    pos, n = 0, len(data)
    while pos < n:
        tag, pos = _read_varint(data, pos)
        field_no, wire = tag >> 3, tag & 7
        if wire == 0:
            val, pos = _read_varint(data, pos)
        elif wire == 1:
            val = int.from_bytes(data[pos:pos + 8], "little")
            pos += 8
        elif wire == 2:
            length, pos = _read_varint(data, pos)
            chunk = data[pos:pos + length]
            pos += length
            val = _decode_length_delimited(chunk)
        elif wire == 5:
            val = int.from_bytes(data[pos:pos + 4], "little")
            pos += 4
        else:
            raise ValueError(f"unsupported protobuf wire type {wire}")

        key = f"field_{field_no}"
        if key in result:
            if not isinstance(result[key], list):
                result[key] = [result[key]]
            result[key].append(val)
        else:
            result[key] = val
    return result


def _decode_length_delimited(chunk: bytes):
    if not chunk:
        return ""
    # Prefer a clean text reading; fall back to a nested message; then raw bytes.
    try:
        text = chunk.decode("utf-8")
        if text.isprintable() or any(c in text for c in "\n\t "):
            return text
    except UnicodeDecodeError:
        pass
    try:
        nested = decode_protobuf(chunk)
        if nested:
            return nested
    except Exception:
        pass
    return {"__bytes_b64__": base64.b64encode(chunk).decode("ascii")}
