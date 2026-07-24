"""Authentication modes, role-based authorisation, and the global read-only switch."""

import base64
import hashlib

import pytest
from fastapi import HTTPException, Request

from app import auth
from app.config import settings

SECRET_HASH = hashlib.sha256(b"secret").hexdigest()


def _request(headers: dict | None = None) -> Request:
    raw = [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()]
    return Request({"type": "http", "headers": raw, "client": ("1.2.3.4", 0)})


@pytest.fixture
def none_mode(monkeypatch):
    monkeypatch.setattr(settings, "kafkascope_auth_mode", "none")
    monkeypatch.setattr(settings, "kafkascope_readonly", False)


@pytest.fixture
def basic_mode(monkeypatch):
    monkeypatch.setattr(settings, "kafkascope_auth_mode", "basic")
    monkeypatch.setattr(settings, "kafkascope_readonly", False)
    monkeypatch.setattr(
        settings, "kafkascope_users", f"alice:{SECRET_HASH}:admin bob:{SECRET_HASH}:viewer"
    )


@pytest.fixture
def proxy_mode(monkeypatch):
    monkeypatch.setattr(settings, "kafkascope_auth_mode", "proxy")
    monkeypatch.setattr(settings, "kafkascope_readonly", False)
    monkeypatch.setattr(settings, "kafkascope_admin_groups", "kafka-admins")


# --- none mode: the zero-friction dev default ---

def test_none_mode_is_anonymous_admin(none_mode):
    p = auth.resolve_principal(_request())
    assert p.role == "admin"
    assert p.anonymous
    assert p.can_write


# --- basic mode ---

def _basic_header(user, password):
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    return {"Authorization": f"Basic {token}"}


def test_basic_no_header_challenges(basic_mode):
    with pytest.raises(HTTPException) as exc:
        auth.resolve_principal(_request())
    assert exc.value.status_code == 401
    assert "Basic" in exc.value.headers["WWW-Authenticate"]


def test_basic_valid_admin(basic_mode):
    p = auth.resolve_principal(_request(_basic_header("alice", "secret")))
    assert p.name == "alice" and p.role == "admin" and p.can_write


def test_basic_valid_viewer_cannot_write(basic_mode):
    p = auth.resolve_principal(_request(_basic_header("bob", "secret")))
    assert p.role == "viewer" and not p.can_write


def test_basic_wrong_password_rejected(basic_mode):
    with pytest.raises(HTTPException):
        auth.resolve_principal(_request(_basic_header("alice", "nope")))


def test_basic_unknown_user_rejected(basic_mode):
    with pytest.raises(HTTPException):
        auth.resolve_principal(_request(_basic_header("mallory", "secret")))


# --- proxy mode ---

def test_proxy_missing_header_rejected(proxy_mode):
    with pytest.raises(HTTPException) as exc:
        auth.resolve_principal(_request())
    assert exc.value.status_code == 401


def test_proxy_admin_group_gets_admin(proxy_mode):
    p = auth.resolve_principal(
        _request({"X-Forwarded-Email": "carol@corp", "X-Forwarded-Groups": "kafka-admins,devs"})
    )
    assert p.name == "carol@corp" and p.role == "admin"


def test_proxy_non_admin_group_is_viewer(proxy_mode):
    p = auth.resolve_principal(
        _request({"X-Forwarded-Email": "dave@corp", "X-Forwarded-Groups": "devs"})
    )
    assert p.role == "viewer" and not p.can_write


# --- the global read-only kill switch overrides any role ---

def test_global_readonly_demotes_admin(basic_mode, monkeypatch):
    monkeypatch.setattr(settings, "kafkascope_readonly", True)
    p = auth.resolve_principal(_request(_basic_header("alice", "secret")))
    assert p.role == "admin"
    assert not p.can_write  # readonly wins over the admin role
