from fastapi import FastAPI, Header, Request, status

import json
from config.redis import lifespan
from domain import IngestionInProgressError, DownstreamProcessingError
from domain.process_ticket_ingestion import ProcessTicketIngestion
from schema import TicketIngestPayload
from dlq.service import dlq_service
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from dlq.router import dlq_router

app = FastAPI(
    title="Resilient Ingestion Gateway",
    version="1.0.0",
    lifespan=lifespan,
)

app.include_router(dlq_router)

@app.get("/healthz")
async def health_check(request: Request):
    redis_healthy = False
    try:
        redis_healthy = bool(await request.app.state.redis_client.ping())
    except Exception:
        redis_healthy = False

    return {
        "status": "ok",
        "redis_connected": redis_healthy,
    }


@app.post("/api/v1/ingest", status_code=status.HTTP_201_CREATED)
async def ingest_ticket(
    request: Request,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
):
    # 1. Read raw body safely
    body_bytes = await request.body()
    raw_str = body_bytes.decode("utf-8", errors="replace")

    # 2. Extract tenant ID hint
    tenant_id = request.headers.get("X-Tenant-ID")

    # 3. Parse & Validate
    try:
        raw_json = json.loads(raw_str) if raw_str else {}
        if not tenant_id and isinstance(raw_json, dict):
            tenant_id = raw_json.get("tenant_id")

        payload = TicketIngestPayload.model_validate(raw_json)

    except (json.JSONDecodeError, ValidationError) as err:
        errors = (
            err.errors(include_context=False)
            if isinstance(err, ValidationError)
            else [{"msg": "Malformed JSON format"}]
        )
        #  DLQ quarantine
        return await dlq_service.quarantine_and_respond(
            raw_body=raw_str,
            errors=errors,
            request=request,
            tenant_id=tenant_id
        )

    # 4. Execute with Idempotency
    use_case = ProcessTicketIngestion()
    try:
        result = await use_case.execute(payload, idempotency_key)
    except IngestionInProgressError as err:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"detail": str(err)}
        )
    except DownstreamProcessingError as err:
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"detail": str(err)}
        )

    if result.is_replay:
        return JSONResponse(
            content=result.response,
            status_code=status.HTTP_200_OK,
            headers={"X-Cache": "HIT", "X-Idempotent-Replay": "true"},
        )

    return JSONResponse(
        content=result.response,
        status_code=status.HTTP_201_CREATED,
        headers={"X-Cache": "MISS"},
    )
