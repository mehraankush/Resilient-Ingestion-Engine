import json
from redis_client import redis_mgr
from domain import IngestionInProgressError, IngestionResult
from schema import TicketIngestPayload
from constents import IDEMPOTENCY_CACHE_TTL, IDEMPOTENCY_LOCK_TTL


class ProcessTicketIngestion:

    def __init__(self):
        self.redis = redis_mgr.client

    async def execute(
        self,
        payload: TicketIngestPayload,
        idempotency_key: str | None = None
    ) -> IngestionResult:

        effective_key = idempotency_key or f"evt:{payload.tenant_id}:{payload.event_id}"
        redis_key = f"idempotency:{effective_key}"

        acquired = await self.redis.set(
            redis_key,
            json.dumps({"status": "PROCESSING"}),
            nx=True,
            ex=IDEMPOTENCY_LOCK_TTL
        )

        if not acquired:
            raw_val = await self.redis.get(redis_key)
            if raw_val:
                record = json.loads(raw_val)
                if record.get("status") == "COMPLETED":
                    return IngestionResult(
                        response=record.get("response", {}),
                        is_replay=True
                    )
            raise IngestionInProgressError("Operation currently processing.")


        result_data = {"status": "accepted", "event_id": payload.event_id}

        await self.redis.set(
            redis_key,
            json.dumps({"status": "COMPLETED", "response": result_data}),
            ex=IDEMPOTENCY_CACHE_TTL
        )

        return IngestionResult(response=result_data, is_replay=False)
