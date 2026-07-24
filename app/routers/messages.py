import json
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from starlette.concurrency import run_in_threadpool

from ..config import settings
from ..services import cluster, messages
from ..templating import templates

router = APIRouter()


def _spec_from_query(topic: str, request: Request) -> messages.ScanSpec:
    q = request.query_params
    raw_ts = q.get("timestamp", "").strip()
    timestamp_ms = None
    if raw_ts:
        try:
            # datetime-local gives naive local time; treat it as UTC to match the display.
            dt = datetime.fromisoformat(raw_ts).replace(tzinfo=timezone.utc)
        except ValueError:
            raise HTTPException(400, f"Could not read timestamp {raw_ts!r}")
        timestamp_ms = int(dt.timestamp() * 1000)

    raw_partition = q.get("partition", "").strip()
    start = q.get("start", "latest")
    if start not in ("latest", "earliest", "timestamp"):
        raise HTTPException(400, f"Unknown start mode {start!r}")

    return messages.ScanSpec(
        topic=topic,
        partition=int(raw_partition) if raw_partition else None,
        start=start,
        lookback=int(q.get("lookback") or 1000),
        timestamp_ms=timestamp_ms,
        limit=int(q.get("limit") or 100),
        key_contains=q.get("key_contains", "").strip(),
        value_contains=q.get("value_contains", "").strip(),
        header_key=q.get("header_key", "").strip(),
        header_value=q.get("header_value", "").strip(),
        json_path=q.get("json_path", "").strip(),
        json_value=q.get("json_value", "").strip(),
    )


@router.get("/topic/{name}/messages", response_class=HTMLResponse)
async def browse(request: Request, name: str) -> HTMLResponse:
    topic = await run_in_threadpool(cluster.get_topic, name)
    if topic is None:
        raise HTTPException(status_code=404, detail=f"Topic {name!r} not found")

    return templates.TemplateResponse(
        request,
        "messages.html",
        {
            "topic": topic,
            "params": dict(request.query_params),
            "scan_limit": settings.kafkascope_scan_limit,
        },
    )


@router.get("/topic/{name}/scan")
async def scan(request: Request, name: str, limit: int = Query(100, le=1000)) -> StreamingResponse:
    spec = _spec_from_query(name, request)
    spec.limit = limit

    def sse(event: str, payload: dict) -> str:
        # JSON-encode the payload so rendered HTML can contain newlines safely.
        return f"event: {event}\ndata: {json.dumps(payload)}\n\n"

    # Resolved once here rather than per hit; the dependency/middleware set these.
    can_write = request.state.principal.can_write
    cluster_name = request.state.cluster.name

    async def stream():
        try:
            async for kind, item in messages.scan(spec, request.is_disconnected):
                if kind == "hit":
                    html = templates.get_template("_message.html").render(
                        msg=item, topic=name, can_write=can_write, c=cluster_name
                    )
                    yield sse("hit", {"html": html})
                else:
                    yield sse(
                        kind,
                        {"scanned": item.scanned, "hits": item.hits, "reason": item.reason},
                    )
        except (LookupError, ValueError) as exc:
            yield sse("failed", {"message": str(exc)})
        except Exception as exc:  # broker trouble mid-scan
            yield sse("failed", {"message": str(exc) or exc.__class__.__name__})

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/topic/{name}/tail")
async def tail(request: Request, name: str) -> StreamingResponse:
    """Live tail: stream new messages as they arrive until the browser disconnects."""
    spec = _spec_from_query(name, request)

    def sse(event: str, payload: dict) -> str:
        return f"event: {event}\ndata: {json.dumps(payload)}\n\n"

    can_write = request.state.principal.can_write
    cluster_name = request.state.cluster.name

    async def stream():
        try:
            async for kind, item in messages.tail(spec, request.is_disconnected):
                if kind == "hit":
                    html = templates.get_template("_message.html").render(
                        msg=item, topic=name, can_write=can_write, c=cluster_name
                    )
                    yield sse("hit", {"html": html})
                else:
                    yield sse(
                        kind,
                        {"scanned": item.scanned, "hits": item.hits, "reason": item.reason},
                    )
        except (LookupError, ValueError) as exc:
            yield sse("failed", {"message": str(exc)})
        except Exception as exc:  # broker trouble mid-tail
            yield sse("failed", {"message": str(exc) or exc.__class__.__name__})

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
