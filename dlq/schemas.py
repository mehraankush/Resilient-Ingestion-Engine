from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class ReplayDLQRequest(BaseModel):
    repaired_payload: Optional[Dict[str, Any]] = Field(
        default=None,
        description="Optional corrected JSON payload to replace the malformed original."
    )


class DLQListResponse(BaseModel):
    total_unresolved: int
    items: List[Dict[str, Any]]


class DLQReplayResponse(BaseModel):
    status: str
    quarantine_id: str
    message: str
    downstream_result: Dict[str, Any]