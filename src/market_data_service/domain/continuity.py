"""Pure continuity audit contracts."""

from __future__ import annotations

from dataclasses import dataclass

from market_data_service.domain.identity import StreamKey
from market_data_service.domain.windows import TimeWindow


@dataclass(frozen=True, slots=True, order=True)
class GapRange:
    start_open_time_ms: int
    end_open_time_ms: int

    def __post_init__(self) -> None:
        if self.start_open_time_ms >= self.end_open_time_ms:
            raise ValueError("gap range must satisfy start_open_time_ms < end_open_time_ms")


@dataclass(frozen=True, slots=True)
class ContinuityReport:
    stream: StreamKey
    checked_start_ms: int
    checked_end_ms: int
    candle_count: int
    is_continuous: bool
    gaps: tuple[GapRange, ...]


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
            gaps=(GapRange(audit_window.start_ms, audit_window.end_ms),),
        )

    gaps = _find_missing_ranges(
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


def _find_missing_ranges(
    observed_open_times_ms: tuple[int, ...],
    *,
    audit_window: TimeWindow,
    step_ms: int,
) -> tuple[GapRange, ...]:
    if step_ms <= 0:
        raise ValueError("step_ms must be positive")
    if audit_window.start_ms % step_ms or audit_window.end_ms % step_ms:
        raise ValueError("audit window must be aligned to step_ms")

    observed = {
        value
        for value in observed_open_times_ms
        if audit_window.start_ms <= value < audit_window.end_ms
    }
    misaligned = [value for value in observed if value % step_ms]
    if misaligned:
        raise ValueError(f"observed timestamps are off-grid: {sorted(misaligned)[:3]}")

    gaps: list[GapRange] = []
    gap_start: int | None = None
    cursor = audit_window.start_ms
    while cursor < audit_window.end_ms:
        if cursor not in observed:
            if gap_start is None:
                gap_start = cursor
        elif gap_start is not None:
            gaps.append(GapRange(gap_start, cursor))
            gap_start = None
        cursor += step_ms

    if gap_start is not None:
        gaps.append(GapRange(gap_start, audit_window.end_ms))
    return tuple(gaps)
