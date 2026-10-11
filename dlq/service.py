import json
import time
from datetime import datetime, timezone
from typing import List, Optional

import redis.asyncio as aioredis
from fastapi import Request, status
from fastapi.responses import JSONResponse

from dlq.models import DLQRecord
from redis_client import redis_mgr


class DeadLetterQueueService:
    DLQ_PREFIX = "dlq:record:"
    DLQ_UNRESOLVED_INDEX = "dlq:index:unresolved"
    DLQ_RESOLVED_INDEX = "dlq:index:resolved"

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
                self.DLQ_UNRESOLVED_INDEX,
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

    async def record_failed_replay(self, record: DLQRecord, error_msg: str) -> DLQRecord:
        """Updates the DLQ record when a replay attempt fails."""
        record.retry_count += 1
        record.last_retry_at = datetime.now(timezone.utc).isoformat()
        record.last_replay_error = error_msg

        # Update Redis hash
        await self.redis.set(
            f"{self.DLQ_PREFIX}{record.quarantine_id}",
            record.model_dump_json()
        )
        return record

    async def record_successful_replay(self, record: DLQRecord) -> DLQRecord:
        """Marks the record resolved and moves it from unresolved to resolved index."""
        record.retry_count += 1
        record.resolved = True
        record.resolved_at = datetime.now(timezone.utc).isoformat()
        record.last_retry_at = record.resolved_at
        record.last_replay_error = None

        record_json = record.model_dump_json()

        # Atomic transaction: update record, remove from unresolved, add to resolved
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.set(f"{self.DLQ_PREFIX}{record.quarantine_id}", record_json)
            pipe.zrem(self.DLQ_UNRESOLVED_INDEX, record.quarantine_id)
            pipe.zadd(self.DLQ_RESOLVED_INDEX, {record.quarantine_id: time.time()})
            await pipe.execute()

        return record

    async def list_quarantined(self, limit: int = 50, resolved: bool = False) -> List[DLQRecord]:
        """Fetch unresolved or resolved records."""
        index_key = self.DLQ_RESOLVED_INDEX if resolved else self.DLQ_UNRESOLVED_INDEX
        ids = await self.redis.zrevrange(index_key, 0, limit - 1)
        records = []
        for q_id in ids:
            raw = await self.redis.get(f"{self.DLQ_PREFIX}{q_id}")
            if raw:
                records.append(DLQRecord.model_validate_json(raw))
        return records

    async def get_by_id(self, quarantine_id: str) -> Optional[DLQRecord]:
        # Fetch record directly using its unique ID
        raw = await self.redis.get(
            f"{self.DLQ_PREFIX}{quarantine_id}"
        )

        return DLQRecord.model_validate_json(raw) if raw else None

    async def mark_resolved(self, quarantine_id: str) -> bool:
        # Atomically remove the ID from index and delete stored data
        async with self.redis.pipeline(transaction=True) as pipe:
            pipe.zrem(self.DLQ_UNRESOLVED_INDEX, quarantine_id)
            pipe.delete(f"{self.DLQ_PREFIX}{quarantine_id}")

            # results[0] = ZREM count, results[1] = DELETE count
            results = await pipe.execute()

        # True if the ID existed in the unresolved index
        return results[0] > 0


dlq_service = DeadLetterQueueService()
