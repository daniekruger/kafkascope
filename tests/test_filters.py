"""The search filter logic — the heart of phase 2, and pure, so fully unit-testable."""

import base64

from app.services.messages import Message, ScanSpec, _decode, _json_lookup, matches


def mk(key=None, value=None, headers=None) -> Message:
    return Message(
        partition=0, offset=0, timestamp_ms=0,
        key=key, value=value, headers=headers or [], size=0,
    )


def spec(**kw) -> ScanSpec:
    return ScanSpec(topic="t", **kw)


# --- key / value substring ---

def test_key_contains_is_case_insensitive():
    assert matches(mk(key="OrderKey"), spec(key_contains="orderkey"))
    assert not matches(mk(key="other"), spec(key_contains="orderkey"))


def test_value_contains():
    assert matches(mk(value="hello world"), spec(value_contains="world"))
    assert not matches(mk(value="hello"), spec(value_contains="world"))


def test_value_contains_against_null_value_does_not_match():
    assert not matches(mk(value=None), spec(value_contains="x"))


# --- headers ---

def test_header_key_presence():
    m = mk(headers=[("trace", "abc")])
    assert matches(m, spec(header_key="trace"))
    assert not matches(m, spec(header_key="missing"))


def test_header_key_and_value():
    m = mk(headers=[("h", "v1"), ("h", "v2")])
    assert matches(m, spec(header_key="h", header_value="v2"))
    assert not matches(m, spec(header_key="h", header_value="nope"))


# --- json path ---

def test_json_path_existence_only():
    assert matches(mk(value='{"a": {"b": 1}}'), spec(json_path="a.b"))
    assert not matches(mk(value='{"a": {}}'), spec(json_path="a.b"))


def test_json_path_value_match():
    m = mk(value='{"order": {"status": "shipped"}}')
    assert matches(m, spec(json_path="order.status", json_value="shipped"))
    assert not matches(m, spec(json_path="order.status", json_value="pending"))


def test_json_path_numeric_value_stringified():
    assert matches(mk(value='{"id": 2}'), spec(json_path="id", json_value="2"))


def test_json_path_on_non_json_never_matches():
    assert not matches(mk(value="not json"), spec(json_path="a"))


# --- filters combine with AND ---

def test_all_filters_must_pass():
    m = mk(key="k1", value='{"status": "ok"}', headers=[("src", "svc")])
    good = spec(key_contains="k1", value_contains="status", header_key="src", json_path="status")
    assert matches(m, good)
    # One failing clause fails the whole match.
    assert not matches(m, spec(key_contains="k1", header_key="absent"))


def test_no_filters_matches_everything():
    assert matches(mk(value="anything"), spec())


# --- _json_lookup directly ---

def test_json_lookup_nested_and_list_index():
    payload = '{"items": [{"x": 10}, {"x": 20}]}'
    assert _json_lookup(payload, "items.1.x") == 20
    assert _json_lookup(payload, "items.5.x") is None
    assert _json_lookup(payload, "missing") is None


def test_json_lookup_invalid_json_returns_none():
    assert _json_lookup("{not json", "a") is None


# --- _decode ---

def test_decode_utf8():
    assert _decode("héllo".encode("utf-8")) == ("héllo", False)


def test_decode_none():
    assert _decode(None) == (None, False)


def test_decode_binary_is_base64():
    raw = b"\xff\xfe\x00\x01"
    text, is_binary = _decode(raw)
    assert is_binary is True
    assert base64.b64decode(text) == raw


# --- ScanSpec.has_filters ---

def test_has_filters_flag():
    assert not spec().has_filters
    assert spec(key_contains="x").has_filters
    assert spec(json_path="a").has_filters
