"""Startup reconciliation contracts."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum

from market_data_service.application.repair_types import RepairProgressMarker
from market_data_service.domain.continuity import ContinuityReport
from market_data_service.domain.identity import StreamKey


class StartupClassification(StrEnum):
    CONNECTING = "connecting"
    INCOMPLETE = "incomplete"
    RECOVERABLE_FAILURE = "recoverable_failure"
    FATAL_FAILURE = "fatal_failure"


@dataclass(frozen=True, slots=True)
class ReconciliationWindow:
    start_time_ms: int
    end_time_ms: int

    def __post_init__(self) -> None:
        if self.start_time_ms < 0 or self.end_time_ms <= self.start_time_ms:
            raise ValueError("invalid reconciliation window")


@dataclass(frozen=True, slots=True)
class LowerBoundProgressMarker:
    next_search_start_time_ms: int


HistoricalProgressMarker = LowerBoundProgressMarker | RepairProgressMarker


@dataclass(frozen=True, slots=True)
class BoundedWorkCounts:
    attempted_windows: int = 0
    completed_windows: int = 0
    committed: int = 0
    duplicates: int = 0
    corrected: int = 0
    rejected: int = 0
    unexpected: int = 0


@dataclass(frozen=True, slots=True)
class StartupStreamOutcome:
    stream: StreamKey
    classification: StartupClassification
    audit: ContinuityReport | None = None
    window: ReconciliationWindow | None = None
    error_code: str | None = None
    error_detail: str | None = None
    progress_marker: HistoricalProgressMarker | None = None
    counts: BoundedWorkCounts = field(default_factory=BoundedWorkCounts)

    @property
    def connecting(self) -> bool:
        return self.classification is StartupClassification.CONNECTING
