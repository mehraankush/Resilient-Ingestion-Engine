import json
import time
from typing import List, Optional

import redis.asyncio as aioredis
from fastapi import Request, status
from fastapi.responses import JSONResponse

from dlq.models import DLQRecord
from redis_client import redis_mgr


class DeadLetterQueueService:
    DLQ_PREFIX = "dlq:record:"
    DLQ_INDEX_KEY = "dlq:index:unresolved"

    @property
    def redis(self) -> aioredis.Redis:
        """Resolve Redis at call time, after application startup."""
        return redis_mgr.client

    async def quarantine(
        self,
        raw_body: str,
        errors: List[dict],
        headers: dict,
        tenant_id: Optional[str] = None,
    ) -> DLQRecord:
        # Pydantic validation errors can contain exception objects in `ctx`.
        # Convert any such values to strings before persisting the record.
        serializable_errors = json.loads(json.dumps(errors, default=str))

        # Create DLQ record, excluding sensitive headers
        record = DLQRecord(
            tenant_id=tenant_id,
            raw_body=raw_body,
            errors=serializable_errors,
            headers={
                k: v
                for k, v in headers.items()
                if k.lower() not in ("authorization", "cookie")
            },
        )

        record_json = record.model_dump_json()

        # Atomically store the record and index it by timestamp
        async with self.redis.pipeline(transaction=True) as pipe:
            # Store serialized record under its unique ID
            pipe.set(
                f"{self.DLQ_PREFIX}{record.quarantine_id}",
                record_json,
            )

            # Add ID to sorted set (score = current timestamp)
            pipe.zadd(
                self.DLQ_INDEX_KEY,
                {record.quarantine_id: time.time()},
            )

            # Execute both Redis commands together
            await pipe.execute()

        return record

    async def quarantine_and_respond(
        self,
        raw_body: str,
        errors: List[dict],
        request: Request,
        tenant_id: Optional[str] = None,
        reason: str = "Payload schema violation. Quarantined for review."
    ) -> JSONResponse:
        """Reusable method: Saves to DLQ and returns standard 202 response."""
        record = await self.quarantine(
            raw_body=raw_body,
            errors=errors,
            headers=dict(request.headers),
            tenant_id=tenant_id,
        )

        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={
                "status": "quarantined",
                "quarantine_id": record.quarantine_id,
                "message": reason,
                "validation_errors": record.errors,
            },
            headers={
                "X-Ingest-Status": "QUARANTINED",
                "X-Quarantine-ID": record.quarantine_id,
            },
        )

    async def list_quarantined(self, limit: int = 50) -> List[DLQRecord]:
        # Fetch latest unresolved IDs (highest timestamp first)
        ids = await self.redis.zrevrange(
            self.DLQ_INDEX_KEY, 0, limit - 1
        )

        if not ids:
            return []

        # Fetch all records in one Redis call instead of N GET calls
        keys = [f"{self.DLQ_PREFIX}{q_id}" for q_id in ids]
        values = await self.redis.mget(keys)

        # Deserialize JSON into validated Pydantic models
        return [
            DLQRecord.model_validate_json(raw)
            for raw in values
            if raw is not None
        ]

    async def get_by_id(self, quarantine_id: str) -> Optional[DLQRecord]:
        # Fetch record directly using its unique ID
        raw = await self.redis.get(
            f"{self.DLQ_PREFIX}{quarantine_id}"
        )

        return DLQRecord.model_validate_json(raw) if raw else None

    async def mark_resolved(self, quarantine_id: str) -> bool:
        # Atomically remove the ID from index and delete stored data
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.zrem(self.DLQ_INDEX_KEY, quarantine_id)
            pipe.delete(f"{self.DLQ_PREFIX}{quarantine_id}")

            # results[0] = ZREM count, results[1] = DELETE count
            results = await pipe.execute()

        # True if the ID existed in the unresolved index
        return results[0] > 0


dlq_service = DeadLetterQueueService()
