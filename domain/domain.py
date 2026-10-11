from dataclasses import dataclass
from typing import Any, Dict


class IngestionError(Exception):
    """Base exception for ticket ingestion."""
    pass


class IngestionInProgressError(IngestionError):
    """Raised when an operation for the given idempotency key is already running."""
    pass


class DownstreamProcessingError(IngestionError):
    """Raised when the downstream persistence or queueing fails."""
    pass


@dataclass(frozen=True)
class IngestionResult:
    response: Dict[str, Any]
    is_replay: bool