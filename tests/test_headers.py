import pytest

from app.services.produce import parse_headers


def test_basic_pairs():
    assert parse_headers("a: 1\nb: 2") == [("a", "1"), ("b", "2")]


def test_blank_lines_ignored():
    assert parse_headers("\n\na: 1\n\n") == [("a", "1")]


def test_bare_name_gets_empty_value():
    assert parse_headers("flag") == [("flag", "")]


def test_colon_in_value_is_preserved():
    # Only the first colon separates name from value.
    assert parse_headers("url: http://host:8080/x") == [("url", "http://host:8080/x")]


def test_whitespace_trimmed():
    assert parse_headers("  a  :  b  ") == [("a", "b")]


def test_missing_name_raises():
    with pytest.raises(ValueError):
        parse_headers(": orphaned-value")


def test_empty_input_is_no_headers():
    assert parse_headers("") == []
