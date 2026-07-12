"""Pure continuity audit contracts."""

from __future__ import annotations

from dataclasses import dataclass

from market_data_service.domain.gaps import Gap, find_gaps
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.windows import TimeWindow


@dataclass(frozen=True, slots=True)
class ContinuityReport:
    stream: StreamKey
    checked_start_ms: int
    checked_end_ms: int
    candle_count: int
    is_continuous: bool
    gaps: tuple[Gap, ...]


def build_continuity_report(
    *,
    stream: StreamKey,
    audit_window: TimeWindow,
    observed_open_times_ms: tuple[int, ...],
    step_ms: int,
) -> ContinuityReport:
    if not observed_open_times_ms:
        return ContinuityReport(
            stream=stream,
            checked_start_ms=audit_window.start_ms,
            checked_end_ms=audit_window.end_ms,
            candle_count=0,
            is_continuous=False,
            gaps=(Gap(audit_window.start_ms, audit_window.end_ms),),
        )

    gaps = find_gaps(
        observed_open_times_ms,
        audit_window=audit_window,
        step_ms=step_ms,
    )
    return ContinuityReport(
        stream=stream,
        checked_start_ms=audit_window.start_ms,
        checked_end_ms=audit_window.end_ms,
        candle_count=len(set(observed_open_times_ms)),
        is_continuous=not gaps,
        gaps=gaps,
    )
