from datetime import datetime, timezone
import uuid
from typing import Any, Dict, List
from pydantic import BaseModel, Field


class DLQRecord(BaseModel):
    quarantine_id: str = Field(default_factory=lambda: f"dlq_{uuid.uuid4().hex[:12]}")
    received_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    tenant_id: str | None = None
    raw_body: str
    errors: List[Dict[str, Any]]
    headers: Dict[str, str]
    retry_count: int = 0
    resolved: bool = False