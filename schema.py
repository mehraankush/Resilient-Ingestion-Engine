from datetime import datetime
from typing import Any
from pydantic import BaseModel, Field, field_validator


class TicketIngestPayload(BaseModel):
    event_id: str
    tenant_id: str = Field(min_length=3, max_length=64)
    event_type: str
    timestamp: datetime
    data: dict[str, Any]

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, value: str) -> str:
        allowed = {"ticket.created", "ticket.updated", "ticket.resolved"}
        if value not in allowed:
            raise ValueError(f"Unsupported event_type: {value}")
        return value

    model_config = {
        "json_schema_extra": {
            "example": {
                "event_id": "evt_9872a3b1",
                "tenant_id": "org_corp_42",
                "event_type": "ticket.created",
                "timestamp": "2026-10-11T12:00:00Z",
                "data": {
                    "ticket_id": "TCK-1049",
                    "subject": "VPN connection dropping intermittently",
                    "priority": "P2"
                }
            }
        }
    }
