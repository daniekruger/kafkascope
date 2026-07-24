"""CSRF protection, security headers, and the /version endpoint."""

import pytest
from fastapi.testclient import TestClient

from app import csrf
from app.config import settings
from app.main import app

C = settings.default_cluster.name

# A write route that fails on typed-confirmation before any broker call, so these
# tests reach the CSRF/auth layers without needing a running Kafka.
WRITE_URL = f"/c/{C}/topic/whatever/delete"
WRITE_DATA = {"confirm": "WRONG"}


# --- /version ---

def test_version_endpoint():
    r = TestClient(app).get("/version")
    assert r.status_code == 200
    body = r.json()
    assert body["name"] == "kafkascope"
    assert body["version"]


# --- security headers ---

def test_security_headers_on_every_response():
    r = TestClient(app).get("/healthz")
    assert r.headers["X-Content-Type-Options"] == "nosniff"
    assert r.headers["X-Frame-Options"] == "DENY"
    assert r.headers["Referrer-Policy"] == "no-referrer"
    assert "frame-ancestors 'none'" in r.headers["Content-Security-Policy"]


# --- CSRF token mechanics ---

def test_issued_token_is_valid_and_tamper_evident():
    token = csrf.issue_token()
    assert csrf._is_valid(token)
    # Flipping the payload invalidates the signature.
    msg, _, sig = token.partition(".")
    assert not csrf._is_valid(f"{msg}x.{sig}")
    assert not csrf._is_valid("nosignature")


# --- CSRF enforcement ---

def test_not_enforced_in_none_mode(monkeypatch):
    monkeypatch.setattr(settings, "kafkascope_auth_mode", "none")
    r = TestClient(app).post(WRITE_URL, data=WRITE_DATA)
    # No token, yet the request is served (the confirm guard rejects it, not CSRF).
    assert r.status_code == 200
    assert "CSRF" not in r.text


def test_missing_token_rejected_when_auth_enabled(monkeypatch):
    monkeypatch.setattr(settings, "kafkascope_auth_mode", "basic")
    r = TestClient(app).post(WRITE_URL, data=WRITE_DATA)
    assert r.status_code == 403
    assert "CSRF" in r.text


def test_valid_token_passes_csrf_gate(monkeypatch):
    monkeypatch.setattr(settings, "kafkascope_auth_mode", "basic")
    token = csrf.issue_token()
    client = TestClient(app)
    client.cookies.set(csrf.COOKIE_NAME, token)
    # CSRF runs outside auth, so a valid token clears it and the request falls
    # through to the auth challenge (401) — proving CSRF did not block it.
    r = client.post(WRITE_URL, data=WRITE_DATA, headers={csrf.HEADER_NAME: token})
    assert r.status_code == 401


def test_unsigned_token_rejected(monkeypatch):
    monkeypatch.setattr(settings, "kafkascope_auth_mode", "basic")
    forged = "attacker.deadbeef"
    client = TestClient(app)
    client.cookies.set(csrf.COOKIE_NAME, forged)
    r = client.post(WRITE_URL, data=WRITE_DATA, headers={csrf.HEADER_NAME: forged})
    assert r.status_code == 403
