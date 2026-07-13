"""Range and ready-stream invariant validation."""

from __future__ import annotations

from market_data_service.application.consumer_read.errors import (
    ContinuityInvariantBroken,
    InvalidRange,
    RangeNotAligned,
    RangeOutOfBounds,
)
from market_data_service.domain.candles import CanonicalCandle


def validate_requested_range(from_ms: int, to_ms: int, step_ms: int) -> None:
    if from_ms < 0 or to_ms < 0 or from_ms >= to_ms:
        raise InvalidRange("from_ms and to_ms must define a positive half-open range")
    if from_ms % step_ms or to_ms % step_ms:
        raise RangeNotAligned("from_ms and to_ms must align to the timeframe grid")


def validate_available_range(
    from_ms: int,
    to_ms: int,
    *,
    available_from_ms: int,
    available_to_ms: int,
) -> None:
    if from_ms < available_from_ms or to_ms > available_to_ms:
        raise RangeOutOfBounds(
            f"requested [{from_ms}, {to_ms}) outside "
            f"[{available_from_ms}, {available_to_ms})"
        )


def validate_complete_grid(
    candles: tuple[CanonicalCandle, ...],
    *,
    from_ms: int,
    to_ms: int,
    step_ms: int,
) -> None:
    expected_count = (to_ms - from_ms) // step_ms
    if len(candles) != expected_count:
        raise ContinuityInvariantBroken("ready stream returned an incomplete candle grid")
    expected = from_ms
    for candle in candles:
        if candle.open_time_ms != expected:
            raise ContinuityInvariantBroken("ready stream returned a gapped or unordered range")
        expected += step_ms
