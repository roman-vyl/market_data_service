"""Data contracts for bounded gap repair."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Literal

from market_data_service.domain.continuity import ContinuityReport
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.timeframes import get_timeframe
from market_data_service.domain.windows import TimeWindow


class RepairStatus(StrEnum):
    COMPLETE = "complete"
    INCOMPLETE = "incomplete"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class RepairStreamGapsRequest:
    stream: StreamKey
    start_time_ms: int
    end_time_ms: int
    max_windows: int

    def __post_init__(self) -> None:
        if self.start_time_ms >= self.end_time_ms:
            raise ValueError("repair range must satisfy start_time_ms < end_time_ms")
        if self.max_windows <= 0:
            raise ValueError("max_windows must be positive")
        step_ms = get_timeframe(self.stream.timeframe).duration_ms
        if self.start_time_ms % step_ms or self.end_time_ms % step_ms:
            raise ValueError("repair range must be aligned to stream timeframe")


@dataclass(frozen=True, slots=True)
class RepairWindowResult:
    window: TimeWindow
    observed: int
    committed: int
    duplicates: int
    corrected: int
    rejected: int
    unexpected: int


@dataclass(frozen=True, slots=True)
class RepairProgressMarker:
    requested_window: TimeWindow
    remaining_missing_candles: int
    earliest_remaining_gap_start_ms: int | None


@dataclass(frozen=True, slots=True)
class RepairStreamGapsResult:
    stream: StreamKey
    requested_window: TimeWindow
    status: RepairStatus
    pre_repair_audit: ContinuityReport
    post_repair_audit: ContinuityReport | None
    attempted_windows: int
    completed_windows: int
    window_results: tuple[RepairWindowResult, ...]
    error_code: str | None = None
    error_detail: str | None = None
    failure_disposition: Literal["recoverable", "fatal"] | None = None

    @property
    def complete(self) -> bool:
        return self.status is RepairStatus.COMPLETE

    @property
    def progress_marker(self) -> RepairProgressMarker:
        report = self.post_repair_audit or self.pre_repair_audit
        step_ms = get_timeframe(self.stream.timeframe).duration_ms
        return RepairProgressMarker(
            requested_window=self.requested_window,
            remaining_missing_candles=sum(
                (gap.end_ms - gap.start_ms) // step_ms for gap in report.gaps
            ),
            earliest_remaining_gap_start_ms=(report.gaps[0].start_ms if report.gaps else None),
        )
