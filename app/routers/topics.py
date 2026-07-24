from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse
from starlette.concurrency import run_in_threadpool

from ..config import settings
from ..services import cluster
from ..templating import templates

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def overview(request: Request) -> HTMLResponse:
    brokers, controller_id = await run_in_threadpool(cluster.get_brokers)
    topics = await run_in_threadpool(cluster.list_topics)

    return templates.TemplateResponse(
        request,
        "overview.html",
        {
            "brokers": brokers,
            "controller_id": controller_id,
            "topics": topics,
            "show_counts": settings.kafkascope_show_counts,
        },
    )


@router.get("/topics/refresh", response_class=HTMLResponse)
async def overview_refresh(request: Request) -> HTMLResponse:
    """The topics table on its own, for the overview auto-refresh poll."""
    topics = await run_in_threadpool(cluster.list_topics)
    return templates.TemplateResponse(
        request,
        "_overview_topics.html",
        {"topics": topics, "show_counts": settings.kafkascope_show_counts},
    )


@router.get("/topic/{name}/count", response_class=HTMLResponse)
async def topic_count(name: str) -> HTMLResponse:
    """Lazy per-row message count for the overview, loaded by HTMX on reveal."""
    count = await run_in_threadpool(cluster.topic_message_count, name)
    if count is None:
        return HTMLResponse("&mdash;")
    return HTMLResponse(f"{count:,}")


@router.get("/topic/{name}/refresh", response_class=HTMLResponse)
async def topic_refresh(request: Request, name: str) -> HTMLResponse:
    """The live region of a topic page (message count + partition offsets), for the
    auto-refresh poll. Config/admin forms are not part of this and never get swapped."""
    topic = await run_in_threadpool(cluster.get_topic, name)
    if topic is None:
        return HTMLResponse('<p class="muted">This topic no longer exists.</p>')
    return templates.TemplateResponse(request, "_topic_live.html", {"topic": topic})


@router.get("/topic/{name}", response_class=HTMLResponse)
async def topic_detail(request: Request, name: str) -> HTMLResponse:
    topic = await run_in_threadpool(cluster.get_topic, name)
    if topic is None:
        raise HTTPException(status_code=404, detail=f"Topic {name!r} not found")

    return templates.TemplateResponse(request, "topic.html", {"topic": topic})
