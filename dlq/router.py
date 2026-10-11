import json
from fastapi import APIRouter, HTTPException, Query, status
from pydantic import ValidationError

from dlq.schemas import ReplayDLQRequest, DLQListResponse, DLQReplayResponse
from dlq.service import dlq_service
from schema import TicketIngestPayload
from domain.process_ticket_ingestion import ProcessTicketIngestion
from domain import IngestionInProgressError, DownstreamProcessingError

dlq_router = APIRouter(prefix="/api/v1/dlq", tags=["DLQ Operations"])


@router_get := dlq_router.get(
    "",
    response_model=DLQListResponse,
    status_code=status.HTTP_200_OK,
    summary="List quarantined poison pills"
)
async def list_dlq_records(
    limit: int = Query(default=20, ge=1, le=100, description="Max items to retrieve"),
):
    """
    Returns unresolved quarantined payloads ordered from newest to oldest.
    """
    records = await dlq_service.list_quarantined(limit=limit)
    return DLQListResponse(
        total_unresolved=len(records),
        items=[record.model_dump() for record in records],
    )


@dlq_router.post(
    "/{quarantine_id}/replay",
    response_model=DLQReplayResponse,
    status_code=status.HTTP_200_OK,
    summary="Replay a quarantined payload"
)
async def replay_dlq_record(
    quarantine_id: str,
    body: ReplayDLQRequest = ReplayDLQRequest(),
):
    """
    Attempts to re-validate and process a quarantined record.
    If successful, removes the record from the DLQ.
    """
    # 1. Fetch record from Redis
    record = await dlq_service.get_by_id(quarantine_id)
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Quarantine record '{quarantine_id}' not found.",
        )

    # 2. Determine target payload data
    target_data = body.repaired_payload
    if target_data is None:
        try:
            target_data = json.loads(record.raw_body)
        except json.JSONDecodeError as err:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=f"Original payload is malformed JSON ({str(err)}). Provide 'repaired_payload' in request body.",
            )

    # 3. Validate against Pydantic schema
    try:
        validated_payload = TicketIngestPayload.model_validate(target_data)
    except ValidationError as err:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={"message": "Payload still fails validation", "errors": err.errors()},
        )

    # 4. Re-execute through domain use case
    use_case = ProcessTicketIngestion()
    try:
        # Replays use the original event_id/tenant_id to guarantee idempotency
        result = await use_case.execute(validated_payload)
    except IngestionInProgressError as err:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Replay lock conflict: {str(err)}",
        )
    except DownstreamProcessingError as err:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Downstream failure during replay: {str(err)}",
        )

    # 5. Remove from unresolved DLQ once successfully processed
    await dlq_service.mark_resolved(quarantine_id)

    return DLQReplayResponse(
        status="resolved",
        quarantine_id=quarantine_id,
        message="Payload successfully validated, processed, and marked resolved.",
        downstream_result=result.response,
    )