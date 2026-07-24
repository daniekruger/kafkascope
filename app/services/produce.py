"""Producing messages.

Deliberately synchronous: every send is flushed and the broker's delivery report
is surfaced to the user. A UI that says "sent" before the broker has acknowledged
is worse than useless when you're debugging why a message never arrived.
"""

from dataclasses import dataclass

from ..config import settings
from ..kafka_client import is_readonly, producer


@dataclass
class Delivery:
    topic: str
    partition: int
    offset: int
    # How the value / key were encoded, for the confirmation message and audit.
    encoding: str = "raw"
    schema_id: int | None = None
    key_encoding: str = "raw"
    key_schema_id: int | None = None


def parse_headers(raw: str) -> list[tuple[str, str]]:
    """Parse 'name: value' lines. Blank lines are ignored; a bare name gets an empty value."""
    headers = []
    for lineno, line in enumerate(raw.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        name, sep, value = line.partition(":")
        if not name.strip():
            raise ValueError(f"Header on line {lineno} has no name")
        headers.append((name.strip(), value.strip() if sep else ""))
    return headers


def send(
    topic: str,
    key: str | None,
    value: str | None,
    headers: list[tuple[str, str]] | None = None,
    partition: int | None = None,
    *,
    value_bytes: bytes | None = None,
    encoding: str = "raw",
    schema_id: int | None = None,
    key_bytes: bytes | None = None,
    key_encoding: str = "raw",
    key_schema_id: int | None = None,
) -> Delivery:
    """Produce one message and block until the broker acknowledges it.

    `value_bytes` / `key_bytes`, when given, are produced verbatim and `value` / `key`
    are ignored — that's how schema-encoded payloads (already serialized to the wire
    format) are sent.
    """
    if is_readonly():
        raise PermissionError("This cluster is read-only")

    result: dict = {}

    def on_delivery(err, msg):
        result["err"] = err
        result["msg"] = msg

    if value_bytes is not None:
        raw_value = value_bytes
    elif value is not None:
        raw_value = value.encode("utf-8")
    else:
        raw_value = None

    if key_bytes is not None:
        raw_key = key_bytes
    elif key is not None:
        raw_key = key.encode("utf-8")
    else:
        raw_key = None

    kwargs = {
        "topic": topic,
        "key": raw_key,
        "value": raw_value,
        "headers": headers or [],
        "on_delivery": on_delivery,
    }
    if partition is not None:
        kwargs["partition"] = partition

    p = producer()
    p.produce(**kwargs)

    outstanding = p.flush(timeout=settings.kafkascope_request_timeout)
    if outstanding:
        raise TimeoutError(
            f"Broker did not acknowledge within {settings.kafkascope_request_timeout}s — "
            "the message may or may not have been written"
        )

    if result.get("err") is not None:
        raise RuntimeError(result["err"].str())

    msg = result["msg"]
    return Delivery(
        topic=msg.topic(), partition=msg.partition(), offset=msg.offset(),
        encoding=encoding, schema_id=schema_id,
        key_encoding=key_encoding, key_schema_id=key_schema_id,
    )


def _encode_field(subject: str, fmt: str, schema_text: str, register: bool, json_text: str):
    """Resolve a schema for a subject and serialize `json_text` against it.

    Returns (wire_bytes, schema_id, encoding). With `register`, the supplied schema is
    registered under the subject (creating a version); otherwise the subject's latest
    registered schema is reused — the safe default that touches nothing in the registry.
    """
    from . import encode as encode_mod
    from .schema_registry import registry_for_cluster

    registry = registry_for_cluster()
    if registry is None:
        raise ValueError("No schema registry is configured for this cluster")

    schema_type = "AVRO" if fmt == "avro" else "JSON"
    if register:
        if not schema_text.strip():
            raise ValueError("Provide a schema to register")
        schema_id = registry.register(subject, schema_text, schema_type)
        schema_str = schema_text
    else:
        latest = registry.latest_by_subject(subject)
        if latest is None:
            raise ValueError(
                f"No schema registered for subject {subject!r}. "
                "Paste a schema and tick 'Register' to create one."
            )
        schema_id, schema_str, schema_type = latest.schema_id, latest.schema_str, latest.schema_type

    if schema_type == "AVRO":
        return encode_mod.encode_avro(schema_str, schema_id, json_text), schema_id, "avro"
    return encode_mod.encode_json_schema(schema_id, json_text), schema_id, "json-schema"


def send_message(
    topic: str,
    *,
    key_text: str,
    key_format: str,
    key_schema: str,
    key_register: bool,
    value_text: str,
    value_format: str,
    value_schema: str,
    value_register: bool,
    null_key: bool,
    tombstone: bool,
    headers: list[tuple[str, str]] | None = None,
    partition: int | None = None,
) -> Delivery:
    """Produce one message, encoding key and/or value against registry schemas when the
    matching format is 'avro' or 'json'. A raw format sends the text as UTF-8 as before.
    Schema subjects follow the Confluent convention: `<topic>-key` and `<topic>-value`.
    """
    if is_readonly():
        raise PermissionError("This cluster is read-only")

    key = key_bytes = key_schema_id = None
    key_encoding = "raw"
    if not null_key and key_format in ("avro", "json"):
        key_bytes, key_schema_id, key_encoding = _encode_field(
            f"{topic}-key", key_format, key_schema, key_register, key_text
        )
    elif not null_key:
        key = key_text

    value = value_bytes = value_schema_id = None
    value_encoding = "raw"
    if not tombstone and value_format in ("avro", "json"):
        value_bytes, value_schema_id, value_encoding = _encode_field(
            f"{topic}-value", value_format, value_schema, value_register, value_text
        )
    elif not tombstone:
        value = value_text

    return send(
        topic, key, value, headers, partition,
        value_bytes=value_bytes, encoding=value_encoding, schema_id=value_schema_id,
        key_bytes=key_bytes, key_encoding=key_encoding, key_schema_id=key_schema_id,
    )
