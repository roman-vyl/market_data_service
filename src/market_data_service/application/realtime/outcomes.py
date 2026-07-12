"""Observable realtime ingestion outcomes."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from market_data_service.domain.identity import StreamKey


class RealtimeIngestionClassification(StrEnum):
    COMMITTED = "committed"
    DUPLICATE = "duplicate"
    CORRECTED = "corrected"
    REJECTED = "rejected"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class RealtimeIngestionOutcome:
    stream: StreamKey
    open_time_ms: int
    classification: RealtimeIngestionClassification
    issue_codes: tuple[str, ...] = ()
    error_code: str | None = None
    error_detail: str | None = None
