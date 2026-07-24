"""CSRF protection for state-changing requests.

Every mutation in this app is an HTMX ``hx-post`` from a page we rendered, so the
scheme is a signed double-submit token delivered two ways at once:

  * a cookie (``kafkascope_csrf``) the browser stores, and
  * the same token echoed back in the ``X-CSRF-Token`` header, injected into every
    HTMX request by ``hx-headers`` on ``<body>`` (see base.html).

A forged cross-site request can carry the victim's ambient credentials (cached
Basic auth, a proxy's identity header) but *cannot* read the cookie to set a
matching header, so it fails the equality check. The token is HMAC-signed too, so
even a cookie planted by a sibling subdomain is rejected without the secret.

Validation only runs when authentication is enabled (``basic``/``proxy``). With
``auth_mode=none`` there are no ambient credentials to abuse — a "forged" request
is indistinguishable from a legitimate anonymous one — so a token would add
friction without adding protection. The cookie is still issued in every mode, so
turning auth on later needs no page change.
"""

import hmac
import secrets
from hashlib import sha256

from starlette.requests import Request
from starlette.responses import Response

from .config import settings

COOKIE_NAME = "kafkascope_csrf"
HEADER_NAME = "X-CSRF-Token"
SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "TRACE"})

# A per-process key when none is configured: fine for a single instance (tokens
# reset on restart); set KAFKASCOPE_SECRET_KEY to share one across replicas.
_FALLBACK_SECRET = secrets.token_urlsafe(32)


def _secret() -> bytes:
    return (settings.kafkascope_secret_key or _FALLBACK_SECRET).encode("utf-8")


def _sign(msg: str) -> str:
    return hmac.new(_secret(), msg.encode("utf-8"), sha256).hexdigest()[:32]


def issue_token() -> str:
    """Mint a fresh signed token of the form ``<random>.<signature>``."""
    msg = secrets.token_urlsafe(16)
    return f"{msg}.{_sign(msg)}"


def _is_valid(token: str) -> bool:
    msg, _, sig = token.partition(".")
    return bool(msg) and bool(sig) and hmac.compare_digest(sig, _sign(msg))


def enforced() -> bool:
    """CSRF only guards authenticated sessions — see the module docstring."""
    return settings.kafkascope_auth_mode in ("basic", "proxy")


def _tokens_match(request: Request) -> bool:
    cookie = request.cookies.get(COOKIE_NAME, "")
    header = request.headers.get(HEADER_NAME, "")
    return (
        bool(cookie)
        and _is_valid(cookie)
        and hmac.compare_digest(cookie, header)
    )


async def csrf_middleware(request: Request, call_next):
    """Attach a token to every request; reject unsafe ones that fail the check."""
    path = request.url.path
    if path == "/healthz" or path == "/version" or path.startswith("/static"):
        return await call_next(request)  # no templates rendered here, no token needed

    cookie = request.cookies.get(COOKIE_NAME, "")
    token = cookie if _is_valid(cookie) else issue_token()
    request.state.csrf_token = token

    if enforced() and request.method not in SAFE_METHODS and not _tokens_match(request):
        return Response("CSRF token missing or invalid", status_code=403)

    response = await call_next(request)
    if token != cookie:
        # New (or replaced) token: persist it. HttpOnly is safe because the page
        # reads the token from server-rendered HTML, never from the cookie.
        response.set_cookie(
            COOKIE_NAME, token, httponly=True, samesite="lax", path="/"
        )
    return response
