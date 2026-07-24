from fastapi import APIRouter, Depends, Form, HTTPException, Request
from fastapi.responses import HTMLResponse
from starlette.concurrency import run_in_threadpool

from ..auth import audit, require_write
from ..services import cluster, messages, produce as produce_service
from ..services.schema_registry import registry_for_cluster
from ..templating import templates

router = APIRouter()


@router.get("/topic/{name}/produce", response_class=HTMLResponse)
async def form(
    request: Request, name: str, source: str = "", _: object = Depends(require_write)
) -> HTMLResponse:
    topic = await run_in_threadpool(cluster.get_topic, name)
    if topic is None:
        raise HTTPException(404, f"Topic {name!r} not found")

    # `source` is a "partition:offset" coordinate to prefill from — the resend path.
    prefill = None
    if source:
        try:
            raw_partition, _, raw_offset = source.partition(":")
            partition, offset = int(raw_partition), int(raw_offset)
        except ValueError:
            raise HTTPException(400, f"Could not read message coordinate {source!r}")

        prefill = await run_in_threadpool(messages.fetch_one, name, partition, offset)
        if prefill is None:
            raise HTTPException(404, f"No message at {name} p{partition}@{offset}")

    return templates.TemplateResponse(
        request, "produce.html", {"topic": topic, "prefill": prefill}
    )


@router.get("/topic/{name}/produce/schema", response_class=HTMLResponse)
async def schema_panel(
    request: Request,
    name: str,
    field: str = "value",  # "key" | "value"
    value_format: str = "raw",
    key_format: str = "raw",
    _: object = Depends(require_write),
) -> HTMLResponse:
    """Fragment shown under a key/value encoding selector: the subject's current schema
    (if any), an editable schema box, and the register toggle."""
    field = "key" if field == "key" else "value"
    fmt = key_format if field == "key" else value_format
    subject = f"{name}-{field}"
    registered = None
    error = None
    if fmt in ("avro", "json"):
        registry = registry_for_cluster()
        if registry is None:
            error = "No schema registry is configured for this cluster."
        else:
            try:
                registered = await run_in_threadpool(registry.latest_by_subject, subject)
            except Exception as exc:  # registry unreachable / errored
                error = str(exc) or exc.__class__.__name__
    return templates.TemplateResponse(
        request,
        "_produce_schema.html",
        {"field": field, "fmt": fmt, "subject": subject, "registered": registered, "error": error},
    )


@router.post("/topic/{name}/produce", response_class=HTMLResponse)
async def send(
    request: Request,
    name: str,
    _: object = Depends(require_write),
    key: str = Form(""),
    value: str = Form(""),
    headers: str = Form(""),
    partition: str = Form(""),
    null_key: str = Form(""),
    tombstone: str = Form(""),
    key_format: str = Form("raw"),
    key_schema_text: str = Form(""),
    key_register: str = Form(""),
    value_format: str = Form("raw"),
    value_schema_text: str = Form(""),
    value_register: str = Form(""),
) -> HTMLResponse:
    def fragment(**ctx):
        return templates.TemplateResponse(request, "_produce_result.html", ctx)

    part = int(partition) if partition else None

    try:
        parsed_headers = produce_service.parse_headers(headers)
        delivery = await run_in_threadpool(
            lambda: produce_service.send_message(
                name,
                key_text=key,
                key_format=key_format,
                key_schema=key_schema_text,
                key_register=bool(key_register),
                value_text=value,
                value_format=value_format,
                value_schema=value_schema_text,
                value_register=bool(value_register),
                null_key=bool(null_key),
                tombstone=bool(tombstone),
                headers=parsed_headers,
                partition=part,
            )
        )
    except (ValueError, RuntimeError, TimeoutError, PermissionError) as exc:
        audit(request, "produce", name, "error", error=str(exc), encoding=value_format)
        return fragment(error=str(exc))
    except Exception as exc:
        audit(request, "produce", name, "error", error=str(exc) or exc.__class__.__name__)
        return fragment(error=str(exc) or exc.__class__.__name__)

    audit(
        request, "produce", name, "ok",
        partition=delivery.partition, offset=delivery.offset, tombstone=bool(tombstone),
        encoding=delivery.encoding, schema_id=delivery.schema_id,
        key_encoding=delivery.key_encoding, key_schema_id=delivery.key_schema_id,
    )
    return fragment(delivery=delivery)
