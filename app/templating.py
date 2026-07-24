from fastapi import Request
from fastapi.templating import Jinja2Templates

from . import __version__
from .auth import principal_of
from .config import settings
from .kafka_client import current_cluster, is_readonly
from .services.schema_registry import registry_for_cluster


def _page_context(request: Request) -> dict:
    """Per-request context every template gets: who's asking, and which cluster.

    The cluster is read from request.state (set by the select_cluster dependency)
    with a fall-back to the default, so error pages rendered before/without cluster
    resolution still have something coherent to show.
    """
    principal = principal_of(request)
    cluster = getattr(request.state, "cluster", None) or current_cluster()
    return {
        "principal": principal,
        "can_write": principal.can_write,
        # Echoed into hx-headers so every HTMX write carries the CSRF token.
        "csrf_token": getattr(request.state, "csrf_token", ""),
        # `c` is the current cluster name, used to build every /c/<name>/... link.
        "c": cluster.name,
        "cluster_name": cluster.name,
        "broker_connect": cluster.brokers,
        "readonly": is_readonly(cluster),
        "clusters": list(settings.clusters.values()),
        "multi_cluster": settings.multi_cluster,
        # Whether this cluster has a schema registry — gates the schema produce UI.
        "registry_configured": registry_for_cluster() is not None,
    }


templates = Jinja2Templates(directory="app/templates", context_processors=[_page_context])
templates.env.globals["auth_mode"] = settings.kafkascope_auth_mode
templates.env.globals["version"] = __version__
templates.env.filters["thousands"] = lambda n: f"{n:,}"
