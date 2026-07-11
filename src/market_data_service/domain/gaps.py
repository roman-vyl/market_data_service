"""Pure canonical gap detection over timeframe-grid timestamps."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from market_data_service.domain.windows import TimeWindow


@dataclass(frozen=True, slots=True, order=True)
class Gap:
    """Missing half-open interval on a known timeframe grid."""

    start_ms: int
    end_ms: int

    def __post_init__(self) -> None:
        if self.start_ms >= self.end_ms:
            raise ValueError("gap must satisfy start_ms < end_ms")

    @property
    def window(self) -> TimeWindow:
        return TimeWindow(self.start_ms, self.end_ms)


def find_gaps(
    observed_open_times_ms: Iterable[int],
    *,
    audit_window: TimeWindow,
    step_ms: int,
) -> tuple[Gap, ...]:
    """Find missing contiguous grid intervals in one audited half-open window.

    Input may be unsorted and contain duplicates. Timestamps outside the audit
    window are ignored. Misaligned timestamps are rejected instead of silently
    changing the canonical grid.
    """

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

    gaps: list[Gap] = []
    gap_start: int | None = None
    cursor = audit_window.start_ms
    while cursor < audit_window.end_ms:
        if cursor not in observed:
            if gap_start is None:
                gap_start = cursor
        elif gap_start is not None:
            gaps.append(Gap(gap_start, cursor))
            gap_start = None
        cursor += step_ms

    if gap_start is not None:
        gaps.append(Gap(gap_start, audit_window.end_ms))
    return tuple(gaps)


def iter_fetch_windows(
    gap: Gap,
    *,
    step_ms: int,
    max_candles: int,
) -> tuple[TimeWindow, ...]:
    """Split one gap into aligned bounded half-open REST fetch windows."""

    if step_ms <= 0:
        raise ValueError("step_ms must be positive")
    if max_candles <= 0:
        raise ValueError("max_candles must be positive")
    if gap.start_ms % step_ms or gap.end_ms % step_ms:
        raise ValueError("gap must be aligned to step_ms")

    max_span_ms = step_ms * max_candles
    windows: list[TimeWindow] = []
    cursor = gap.start_ms
    while cursor < gap.end_ms:
        end_ms = min(cursor + max_span_ms, gap.end_ms)
        windows.append(TimeWindow(cursor, end_ms))
        cursor = end_ms
    return tuple(windows)
