"""Pure validation for normalized candle values."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from market_data_service.domain.candles import ObservedCandle
from market_data_service.domain.timeframes import get_timeframe


class CandleValidationCode(StrEnum):
    UNCONFIRMED = "unconfirmed"
    NEGATIVE_TIMESTAMP = "negative_timestamp"
    OFF_GRID_OPEN_TIME = "off_grid_open_time"
    INVALID_CLOSE_TIME = "invalid_close_time"
    INVALID_OHLC = "invalid_ohlc"
    NEGATIVE_VOLUME = "negative_volume"


@dataclass(frozen=True, slots=True)
class CandleValidationIssue:
    code: CandleValidationCode
    detail: str


def validate_observed_candle(candle: ObservedCandle) -> tuple[CandleValidationIssue, ...]:
    """Return all deterministic domain validation issues for one observation."""

    issues: list[CandleValidationIssue] = []
    timeframe = get_timeframe(candle.stream.timeframe)

    if not candle.confirmed:
        issues.append(CandleValidationIssue(CandleValidationCode.UNCONFIRMED, "candle is not closed"))
    if candle.open_time_ms < 0 or candle.close_time_ms < 0:
        issues.append(
            CandleValidationIssue(
                CandleValidationCode.NEGATIVE_TIMESTAMP,
                "open_time_ms and close_time_ms must be non-negative",
            )
        )
    if candle.open_time_ms % timeframe.duration_ms != 0:
        issues.append(
            CandleValidationIssue(
                CandleValidationCode.OFF_GRID_OPEN_TIME,
                f"open_time_ms is not aligned to {timeframe.id}",
            )
        )

    expected_close_time_ms = candle.open_time_ms + timeframe.duration_ms - 1
    if candle.close_time_ms != expected_close_time_ms:
        issues.append(
            CandleValidationIssue(
                CandleValidationCode.INVALID_CLOSE_TIME,
                f"close_time_ms must equal {expected_close_time_ms}",
            )
        )

    if candle.high < max(candle.open, candle.close, candle.low):
        issues.append(
            CandleValidationIssue(
                CandleValidationCode.INVALID_OHLC,
                "high must be greater than or equal to open, close, and low",
            )
        )
    if candle.low > min(candle.open, candle.close, candle.high):
        issues.append(
            CandleValidationIssue(
                CandleValidationCode.INVALID_OHLC,
                "low must be less than or equal to open, close, and high",
            )
        )
    if candle.volume < 0:
        issues.append(
            CandleValidationIssue(
                CandleValidationCode.NEGATIVE_VOLUME,
                "volume must be non-negative",
            )
        )

    return tuple(issues)
