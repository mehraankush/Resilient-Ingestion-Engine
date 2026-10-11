from datetime import datetime, timezone
import uuid
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class DLQRecord(BaseModel):
    quarantine_id: str = Field(default_factory=lambda: f"dlq_{uuid.uuid4().hex[:12]}")
    received_at: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    tenant_id: Optional[str] = None
    raw_body: str
    errors: List[Dict[str, Any]]
    headers: Dict[str, str]
    retry_count: int = 0
    resolved: bool = False
    resolved_at: Optional[str] = None
    last_retry_at: Optional[str] = None
    last_replay_error: Optional[str] = None
