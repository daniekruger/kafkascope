import logging
import sys

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.exceptions import HTTPException as StarletteHTTPException

from . import __version__, csrf
from .auth import audit_logger, resolve_principal
from .config import settings
from .kafka_client import set_current_cluster
from .routers import admin, groups, messages, produce, topics
from .templating import templates


def _configure_audit_log() -> None:
    audit_logger.setLevel(logging.INFO)
    audit_logger.propagate = False  # don't double-emit through uvicorn's root logger
    fmt = logging.Formatter("AUDIT %(message)s")

    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(fmt)
    audit_logger.addHandler(stream)

    if settings.kafkascope_audit_log:
        file_handler = logging.FileHandler(settings.kafkascope_audit_log)
        file_handler.setFormatter(fmt)
        audit_logger.addHandler(file_handler)


_configure_audit_log()

app = FastAPI(title="kafkascope")
app.mount("/static", StaticFiles(directory="app/static"), name="static")


async def select_cluster(request: Request) -> None:
    """Resolve /c/<name>/... to a cluster and pin it for this request.

    Sets the ContextVar the kafka_client reads (so service calls, including those run
    in worker threads, target the right broker) and stashes the config on request.state
    for the templates. An unknown name is a 404 — never a silent fall-through to some
    other cluster, which for a tool whose whole job is not confusing prod with dev
    would be the worst possible bug.
    """
    name = request.path_params.get("cluster", "")
    cluster = settings.clusters.get(name)
    if cluster is None:
        known = ", ".join(settings.clusters) or "none"
        raise HTTPException(404, f"Unknown cluster {name!r}. Configured: {known}.")
    set_current_cluster(cluster)
    request.state.cluster = cluster


# Every cluster-scoped page lives under /c/<name>. The path param feeds select_cluster,
# which the routers don't otherwise need, so their handlers stay unchanged.
_CLUSTER_PREFIX = "/c/{cluster}"
_cluster_dep = [Depends(select_cluster)]
app.include_router(topics.router, prefix=_CLUSTER_PREFIX, dependencies=_cluster_dep)
app.include_router(messages.router, prefix=_CLUSTER_PREFIX, dependencies=_cluster_dep)
app.include_router(produce.router, prefix=_CLUSTER_PREFIX, dependencies=_cluster_dep)
app.include_router(groups.router, prefix=_CLUSTER_PREFIX, dependencies=_cluster_dep)
app.include_router(admin.router, prefix=_CLUSTER_PREFIX, dependencies=_cluster_dep)

# Paths served without authentication: liveness probe and static assets.
_PUBLIC_PREFIXES = ("/static",)


@app.middleware("http")
async def authenticate(request: Request, call_next):
    path = request.url.path
    if path in ("/healthz", "/version") or path.startswith(_PUBLIC_PREFIXES):
        return await call_next(request)
    try:
        request.state.principal = resolve_principal(request)
    except StarletteHTTPException as exc:
        # Middleware runs outside the exception-handler stack, so turn the auth
        # challenge into a response here (carries WWW-Authenticate for Basic mode).
        return PlainTextResponse(
            exc.detail, status_code=exc.status_code, headers=exc.headers
        )
    return await call_next(request)


# CSRF runs outside authenticate (registered later = outer), so a token is on
# request.state before any template renders. See app/csrf.py.
app.middleware("http")(csrf.csrf_middleware)


@app.middleware("http")
async def security_headers(request: Request, call_next):
    """Baseline hardening headers. Registered last, so it wraps every response —
    including the auth challenge and the CSRF rejection."""
    response = await call_next(request)
    response.headers.setdefault("X-Content-Type-Options", "nosniff")
    response.headers.setdefault("X-Frame-Options", "DENY")
    response.headers.setdefault("Referrer-Policy", "no-referrer")
    # The app is fully self-contained (vendored htmx, no external assets), so
    # default-src 'self' holds. Inline scripts and handlers need 'unsafe-inline';
    # frame-ancestors/base-uri/object-src close off clickjacking and injection.
    response.headers.setdefault(
        "Content-Security-Policy",
        "default-src 'self'; script-src 'self' 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline'; img-src 'self' data:; "
        "connect-src 'self'; base-uri 'none'; object-src 'none'; "
        "frame-ancestors 'none'",
    )
    return response


@app.get("/")
async def root() -> RedirectResponse:
    # Land on the default cluster's overview; every other page is under /c/<name>.
    return RedirectResponse(f"/c/{settings.default_cluster.name}/")


@app.exception_handler(StarletteHTTPException)
async def http_error(request: Request, exc: StarletteHTTPException) -> HTMLResponse:
    return templates.TemplateResponse(
        request,
        "error.html",
        {"message": exc.detail},
        status_code=exc.status_code,
    )


@app.exception_handler(Exception)
async def unhandled_error(request: Request, exc: Exception) -> HTMLResponse:
    """Kafka errors are routine here (broker down, topic gone). Show them, don't 500 blankly."""
    return templates.TemplateResponse(
        request,
        "error.html",
        {"message": str(exc) or exc.__class__.__name__},
        status_code=500,
    )


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "clusters": ",".join(settings.clusters)}


@app.get("/version")
async def version() -> dict[str, str]:
    return {"name": "kafkascope", "version": __version__}
