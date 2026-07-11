"""Data contracts for bounded stream backfill."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from market_data_service.domain.identity import StreamKey
from market_data_service.domain.timeframes import get_timeframe
from market_data_service.domain.windows import TimeWindow


@dataclass(frozen=True, slots=True)
class BackfillStreamRequest:
    stream: StreamKey
    start_time_ms: int
    end_time_ms: int
    max_windows: int

    def __post_init__(self) -> None:
        if self.start_time_ms >= self.end_time_ms:
            raise ValueError("backfill range must satisfy start_time_ms < end_time_ms")
        if self.max_windows <= 0:
            raise ValueError("max_windows must be positive")
        step_ms = get_timeframe(self.stream.timeframe).duration_ms
        if self.start_time_ms % step_ms or self.end_time_ms % step_ms:
            raise ValueError("backfill range must be aligned to stream timeframe")


@dataclass(frozen=True, slots=True)
class BackfillWindowResult:
    window: TimeWindow
    observed: int
    committed: int
    duplicates: int
    corrected: int
    rejected: int


@dataclass(frozen=True, slots=True)
class BackfillStreamResult:
    stream: StreamKey
    requested_window: TimeWindow
    attempted_windows: int
    completed_windows: int
    reached_end: bool
    next_start_time_ms: int
    window_results: tuple[BackfillWindowResult, ...]
    error_code: str | None = None
    error_detail: str | None = None


class Clock(Protocol):
    def now_ms(self) -> int: ...
