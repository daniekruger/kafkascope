from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from starlette.concurrency import run_in_threadpool

from ..auth import audit, require_write
from ..services import cluster, groups as groups_service
from ..templating import templates

router = APIRouter()


@router.get("/groups", response_class=HTMLResponse)
async def index(request: Request) -> HTMLResponse:
    found = await run_in_threadpool(groups_service.list_groups)
    return templates.TemplateResponse(request, "groups.html", {"groups": found})


@router.get("/groups/refresh", response_class=HTMLResponse)
async def index_refresh(request: Request) -> HTMLResponse:
    """The groups table on its own, for the auto-refresh poll. Cheap: one describe
    call; the per-row lag cells re-fetch lazily only for the rows on screen."""
    found = await run_in_threadpool(groups_service.list_groups)
    return templates.TemplateResponse(request, "_groups_table.html", {"groups": found})


@router.get("/group/{group_id}/lag", response_class=HTMLResponse)
async def lag(group_id: str) -> HTMLResponse:
    """Lazy total-lag cell for the groups list, loaded by HTMX on reveal — same
    O(visible rows) trick the topic overview uses for message counts."""
    total = await run_in_threadpool(groups_service.group_total_lag, group_id)
    if total is None:
        return HTMLResponse("&mdash;")
    return HTMLResponse(f"{total:,}")


@router.get("/group/{group_id}/refresh", response_class=HTMLResponse)
async def refresh(request: Request, group_id: str) -> HTMLResponse:
    """The live region of the group page (state, lag, members), re-fetched for the
    auto-refresh poll. Read-only; the danger-zone forms are never part of this swap."""
    group = await run_in_threadpool(groups_service.get_group, group_id)
    if group is None:
        return HTMLResponse('<p class="muted">This group no longer exists.</p>')
    return templates.TemplateResponse(request, "_group_live.html", {"group": group})


@router.get("/group/{group_id}", response_class=HTMLResponse)
async def detail(request: Request, group_id: str) -> HTMLResponse:
    group = await run_in_threadpool(groups_service.get_group, group_id)
    if group is None:
        raise HTTPException(404, f"Consumer group {group_id!r} not found")

    topics = await run_in_threadpool(cluster.list_topics)
    return templates.TemplateResponse(
        request,
        "group.html",
        {
            "group": group,
            "all_topics": [t.name for t in topics if not t.internal],
        },
    )


@router.post("/group/{group_id}/reset", response_class=HTMLResponse)
async def reset(
    request: Request,
    group_id: str,
    _: None = Depends(require_write),
    topic: str = Form(...),
    partition: str = Form(""),
    target: str = Form("earliest"),
    timestamp: str = Form(""),
    offset: str = Form(""),
    confirm: str = Form(""),
) -> HTMLResponse:
    def fragment(**ctx):
        return templates.TemplateResponse(request, "_group_result.html", ctx)

    # The group id has to be typed back. Resetting the wrong group's offsets is
    # silent and unrecoverable — there's no undo for "which messages did we skip".
    if confirm.strip() != group_id:
        audit(request, "reset_offsets", group_id, "denied", reason="confirmation mismatch")
        return fragment(error=f"Type the group id ({group_id}) to confirm this reset")

    if target not in ("earliest", "latest", "timestamp", "offset"):
        return fragment(error=f"Unknown reset target {target!r}")

    timestamp_ms = None
    if target == "timestamp":
        if not timestamp.strip():
            return fragment(error="Pick a time to reset to")
        try:
            dt = datetime.fromisoformat(timestamp.strip()).replace(tzinfo=timezone.utc)
        except ValueError:
            return fragment(error=f"Could not read timestamp {timestamp!r}")
        timestamp_ms = int(dt.timestamp() * 1000)

    try:
        applied = await run_in_threadpool(
            groups_service.reset_offsets,
            group_id,
            topic,
            int(partition) if partition else None,
            target,
            timestamp_ms,
            int(offset) if offset.strip() else None,
        )
    except (ValueError, LookupError, PermissionError, RuntimeError) as exc:
        audit(request, "reset_offsets", group_id, "error", topic=topic, error=str(exc))
        return fragment(error=str(exc))
    except Exception as exc:
        audit(request, "reset_offsets", group_id, "error", topic=topic, error=str(exc))
        return fragment(error=str(exc) or exc.__class__.__name__)

    audit(request, "reset_offsets", group_id, "ok", topic=topic, target=target,
          partitions=[o.partition for o in applied])
    return fragment(applied=applied, group_id=group_id)


@router.post("/group/{group_id}/delete", response_class=HTMLResponse)
async def delete(
    request: Request,
    group_id: str,
    _: None = Depends(require_write),
    confirm: str = Form(""),
) -> HTMLResponse:
    def fragment(**ctx):
        return templates.TemplateResponse(request, "_group_result.html", ctx)

    if confirm.strip() != group_id:
        audit(request, "delete_group", group_id, "denied", reason="confirmation mismatch")
        return fragment(error=f"Type the group id ({group_id}) to confirm deletion")

    try:
        await run_in_threadpool(groups_service.delete_group, group_id)
    except Exception as exc:
        audit(request, "delete_group", group_id, "error", error=str(exc))
        return fragment(error=str(exc) or exc.__class__.__name__)

    audit(request, "delete_group", group_id, "ok")
    return fragment(deleted=group_id)
