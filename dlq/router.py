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


@dlq_router.post("/{quarantine_id}/replay", response_model=DLQReplayResponse)
async def replay_dlq_record(
    quarantine_id: str,
    body: ReplayDLQRequest = ReplayDLQRequest(),
):
    record = await dlq_service.get_by_id(quarantine_id)
    if not record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Quarantine record '{quarantine_id}' not found.",
        )

    # 1. Parse JSON body
    target_data = body.repaired_payload
    if target_data is None:
        try:
            target_data = json.loads(record.raw_body)
        except json.JSONDecodeError as err:
            err_msg = f"Original payload is malformed JSON: {str(err)}"
            await dlq_service.record_failed_replay(record, err_msg)
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=err_msg
            )

    # 2. Schema Validation
    try:
        validated_payload = TicketIngestPayload.model_validate(target_data)
    except ValidationError as err:
        validation_errors = err.errors(include_context=False)
        await dlq_service.record_failed_replay(
            record,
            f"Validation error: {validation_errors}",
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "message": "Payload still fails validation",
                "errors": validation_errors,
            },
        ) from err

    use_case = ProcessTicketIngestion()
    try:
        result = await use_case.execute(validated_payload)
    except (IngestionInProgressError, DownstreamProcessingError) as err:
        await dlq_service.record_failed_replay(record, f"Execution failed: {str(err)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(err)
        )

    # 4. Success -> Record the retry and mark resolved
    updated_record = await dlq_service.record_successful_replay(record)

    return DLQReplayResponse(
        status="resolved",
        quarantine_id=quarantine_id,
        message=f"Payload replayed successfully on attempt {updated_record.retry_count}.",
        downstream_result=result.response,
    )
