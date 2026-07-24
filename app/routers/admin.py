from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from starlette.concurrency import run_in_threadpool

from ..auth import audit, require_write
from ..services import admin as admin_service
from ..templating import templates

router = APIRouter()


def _topic_url(request: Request, name: str) -> str:
    """A cluster-scoped topic URL for reload links in result fragments."""
    return f"/c/{request.state.cluster.name}/topic/{name}"


@router.get("/topics/new", response_class=HTMLResponse)
async def new_form(request: Request, _: object = Depends(require_write)) -> HTMLResponse:
    return templates.TemplateResponse(
        request, "topic_new.html", {"common_configs": admin_service.COMMON_CONFIGS}
    )


@router.post("/topics/new", response_class=HTMLResponse)
async def create(
    request: Request,
    _: object = Depends(require_write),
    name: str = Form(...),
    partitions: int = Form(1),
    replication_factor: int = Form(1),
) -> HTMLResponse:
    # Any form field named cfg:<key> becomes a topic config override.
    form = await request.form()
    config = {
        k[4:]: v.strip()
        for k, v in form.items()
        if k.startswith("cfg:") and str(v).strip()
    }

    def fragment(**ctx):
        return templates.TemplateResponse(request, "_admin_result.html", ctx)

    try:
        await run_in_threadpool(
            admin_service.create_topic, name, partitions, replication_factor, config
        )
    except Exception as exc:
        audit(request, "create_topic", name.strip(), "error", error=str(exc))
        if isinstance(exc, (ValueError, LookupError, PermissionError, RuntimeError)):
            return fragment(error=str(exc))
        return fragment(error=str(exc) or exc.__class__.__name__)

    audit(request, "create_topic", name.strip(), "ok",
          partitions=partitions, replication_factor=replication_factor)
    return fragment(created=name.strip())


@router.post("/topic/{name}/config", response_class=HTMLResponse)
async def edit_config(
    request: Request, name: str, _: object = Depends(require_write)
) -> HTMLResponse:
    form = await request.form()
    changes = {k[4:]: str(v).strip() for k, v in form.items() if k.startswith("cfg:")}

    def fragment(**ctx):
        return templates.TemplateResponse(request, "_admin_result.html", ctx)

    try:
        await run_in_threadpool(admin_service.update_config, name, changes)
    except Exception as exc:
        audit(request, "update_config", name, "error", error=str(exc))
        if isinstance(exc, (ValueError, LookupError, PermissionError, RuntimeError)):
            return fragment(error=str(exc))
        return fragment(error=str(exc) or exc.__class__.__name__)

    audit(request, "update_config", name, "ok", keys=sorted(changes))
    return fragment(configured=name, reload_url=_topic_url(request, name))


@router.post("/topic/{name}/partitions", response_class=HTMLResponse)
async def grow_partitions(
    request: Request, name: str, _: object = Depends(require_write), total: int = Form(...)
) -> HTMLResponse:
    def fragment(**ctx):
        return templates.TemplateResponse(request, "_admin_result.html", ctx)

    try:
        await run_in_threadpool(admin_service.add_partitions, name, total)
    except Exception as exc:
        audit(request, "add_partitions", name, "error", error=str(exc))
        if isinstance(exc, (ValueError, LookupError, PermissionError, RuntimeError)):
            return fragment(error=str(exc))
        return fragment(error=str(exc) or exc.__class__.__name__)

    audit(request, "add_partitions", name, "ok", total=total)
    return fragment(configured=name, reload_url=_topic_url(request, name))


@router.post("/topic/{name}/purge", response_class=HTMLResponse)
async def purge(
    request: Request, name: str, _: object = Depends(require_write), confirm: str = Form("")
) -> HTMLResponse:
    def fragment(**ctx):
        return templates.TemplateResponse(request, "_admin_result.html", ctx)

    if confirm.strip() != name:
        audit(request, "purge_topic", name, "denied", reason="confirmation mismatch")
        return fragment(error=f"Type the topic name ({name}) to confirm the purge")

    try:
        purged = await run_in_threadpool(admin_service.purge_topic, name)
    except Exception as exc:
        audit(request, "purge_topic", name, "error", error=str(exc))
        if isinstance(exc, (ValueError, LookupError, PermissionError, RuntimeError)):
            return fragment(error=str(exc))
        return fragment(error=str(exc) or exc.__class__.__name__)

    audit(request, "purge_topic", name, "ok", records=purged)
    return fragment(purged=name, count=purged, reload_url=_topic_url(request, name))


@router.post("/topic/{name}/delete", response_class=HTMLResponse)
async def delete(
    request: Request, name: str, _: object = Depends(require_write), confirm: str = Form("")
) -> HTMLResponse:
    def fragment(**ctx):
        return templates.TemplateResponse(request, "_admin_result.html", ctx)

    if confirm.strip() != name:
        audit(request, "delete_topic", name, "denied", reason="confirmation mismatch")
        return fragment(error=f"Type the topic name ({name}) to confirm deletion")

    try:
        await run_in_threadpool(admin_service.delete_topic, name)
    except Exception as exc:
        audit(request, "delete_topic", name, "error", error=str(exc))
        if isinstance(exc, (ValueError, LookupError, PermissionError, RuntimeError)):
            return fragment(error=str(exc))
        return fragment(error=str(exc) or exc.__class__.__name__)

    audit(request, "delete_topic", name, "ok")
    return fragment(deleted=name)
