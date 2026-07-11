"""Canonical timeframe registry and grid mathematics.

Semantics are intentionally ported from the old BBB Data Engine while adding
mandatory one-minute support for the new service.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class TimeframeSpec:
    id: str
    duration_ms: int
    bybit_interval: str
    pandas_frequency: str

    def __post_init__(self) -> None:
        if not self.id:
            raise ValueError("timeframe id must not be empty")
        if self.duration_ms <= 0:
            raise ValueError("timeframe duration must be positive")
        if not self.bybit_interval:
            raise ValueError("Bybit interval must not be empty")
        if not self.pandas_frequency:
            raise ValueError("pandas frequency must not be empty")


_TIMEFRAMES: tuple[TimeframeSpec, ...] = (
    TimeframeSpec("1m", 60_000, "1", "1min"),
    TimeframeSpec("5m", 5 * 60_000, "5", "5min"),
    TimeframeSpec("15m", 15 * 60_000, "15", "15min"),
    TimeframeSpec("1h", 60 * 60_000, "60", "1h"),
    TimeframeSpec("4h", 4 * 60 * 60_000, "240", "4h"),
    TimeframeSpec("1d", 24 * 60 * 60_000, "D", "1d"),
)

TIMEFRAMES: dict[str, TimeframeSpec] = {spec.id: spec for spec in _TIMEFRAMES}


def get_timeframe(timeframe_id: str) -> TimeframeSpec:
    """Return one supported timeframe or fail explicitly."""

    normalized = timeframe_id.strip().lower()
    try:
        return TIMEFRAMES[normalized]
    except KeyError as exc:
        supported = ", ".join(TIMEFRAMES)
        raise ValueError(f"unsupported timeframe {timeframe_id!r}; supported: {supported}") from exc


def align_to_grid(timestamp_ms: int, step_ms: int) -> int:
    """Floor a timestamp to its timeframe-grid boundary."""

    if step_ms <= 0:
        raise ValueError("step_ms must be positive")
    return timestamp_ms - (timestamp_ms % step_ms)


def ceil_to_grid(timestamp_ms: int, step_ms: int) -> int:
    """Ceil a timestamp to its timeframe-grid boundary."""

    aligned = align_to_grid(timestamp_ms, step_ms)
    return aligned if aligned == timestamp_ms else aligned + step_ms


def last_closed_open_time_ms(now_ms: int, step_ms: int) -> int:
    """Return the open time of the latest fully closed candle.

    At an exact boundary, the candle opening at that boundary is still current,
    so the prior grid point is returned.
    """

    return align_to_grid(now_ms, step_ms) - step_ms
