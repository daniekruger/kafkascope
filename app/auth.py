"""Authentication, authorisation, and the audit log.

Designed so the default (`auth_mode=none`) is a zero-friction dev tool: every
request is an anonymous admin and nothing is required. Turning on `basic` or
`proxy` layers in real identity and role-based write access without touching any
route — the middleware resolves a Principal onto `request.state`, the
`require_write` dependency enforces the role, and `audit()` records every
mutation regardless of mode.
"""

import base64
import hashlib
import hmac
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from fastapi import HTTPException, Request

from .config import parse_users, settings
from .kafka_client import current_cluster, is_readonly

audit_logger = logging.getLogger("kafkascope.audit")


@dataclass(frozen=True)
class Principal:
    name: str
    role: str  # "admin" | "viewer"
    anonymous: bool = False

    @property
    def can_write(self) -> bool:
        # An admin may write unless the current cluster is read-only, or the whole
        # instance is (the global KAFKASCOPE_READONLY kill switch). Evaluated lazily so
        # it reflects whichever cluster the request landed on.
        return self.role == "admin" and not is_readonly()


ANONYMOUS_ADMIN = Principal(name="anonymous", role="admin", anonymous=True)


def _basic_challenge() -> HTTPException:
    return HTTPException(
        status_code=401,
        detail="Authentication required",
        headers={"WWW-Authenticate": 'Basic realm="kafkascope"'},
    )


def _resolve_basic(request: Request) -> Principal:
    header = request.headers.get("Authorization", "")
    if not header.startswith("Basic "):
        raise _basic_challenge()
    try:
        user, _, password = base64.b64decode(header[6:]).decode("utf-8").partition(":")
    except (ValueError, UnicodeDecodeError):
        raise _basic_challenge()

    record = parse_users(settings.kafkascope_users).get(user)
    # Hash even when the user is unknown, so timing doesn't leak which names exist.
    provided = hashlib.sha256(password.encode("utf-8")).hexdigest()
    expected = record[0] if record else "0" * 64
    if not hmac.compare_digest(provided, expected) or record is None:
        raise _basic_challenge()
    return Principal(name=user, role=record[1])


def _resolve_proxy(request: Request) -> Principal:
    name = request.headers.get(settings.kafkascope_proxy_user_header, "").strip()
    if not name:
        raise HTTPException(401, "Not authenticated: missing proxy identity header")

    admin_groups = settings.admin_groups_set
    if not admin_groups:
        # No admin group configured => any authenticated user is an admin.
        return Principal(name=name, role="admin")

    raw = request.headers.get(settings.kafkascope_proxy_groups_header, "")
    groups = {g.strip() for g in raw.replace(",", " ").split() if g.strip()}
    role = "admin" if groups & admin_groups else "viewer"
    return Principal(name=name, role=role)


def resolve_principal(request: Request) -> Principal:
    mode = settings.kafkascope_auth_mode
    if mode == "basic":
        return _resolve_basic(request)
    if mode == "proxy":
        return _resolve_proxy(request)
    return ANONYMOUS_ADMIN


def principal_of(request: Request) -> Principal:
    return getattr(request.state, "principal", ANONYMOUS_ADMIN)


def require_write(request: Request) -> Principal:
    """Route dependency: allow the request only if the caller may write."""
    principal = principal_of(request)
    if not principal.can_write:
        if is_readonly():
            raise HTTPException(403, f"Cluster {current_cluster().name!r} is read-only")
        raise HTTPException(403, f"{principal.name} does not have write access")
    return principal


def audit(request: Request, action: str, target: str, outcome: str, **detail: object) -> None:
    """Record one mutation attempt. Emitted for ok, error, and denied outcomes."""
    principal = principal_of(request)
    record = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "user": principal.name,
        "role": principal.role,
        "cluster": current_cluster().name,
        "action": action,
        "target": target,
        "outcome": outcome,
    }
    if request.client:
        record["ip"] = request.client.host
    if detail:
        record["detail"] = detail
    audit_logger.info(json.dumps(record))
