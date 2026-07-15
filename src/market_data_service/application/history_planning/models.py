"""Transport-neutral models for research history planning reads."""

from __future__ import annotations

from dataclasses import dataclass

from market_data_service.domain.gaps import Gap
from market_data_service.domain.identity import StreamKey


@dataclass(frozen=True, slots=True)
class StreamBoundsResult:
    stream: StreamKey
    state: str
    earliest_committed_open_time_ms: int | None
    latest_committed_open_time_ms: int | None


@dataclass(frozen=True, slots=True)
class ContinuityAuditResult:
    stream: StreamKey
    checked_start_ms: int
    checked_end_ms: int
    candle_count: int
    is_continuous: bool
    gaps: tuple[Gap, ...]
    state: str
    market_data_hash: str | None = None
