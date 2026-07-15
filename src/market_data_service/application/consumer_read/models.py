"""Transport-neutral consumer read models."""

from __future__ import annotations

from dataclasses import dataclass

from market_data_service.domain.candles import CanonicalCandle
from market_data_service.domain.identity import StreamKey


@dataclass(frozen=True, slots=True)
class CandleRangeRequest:
    ticker: str
    timeframe: str
    from_ms: int
    to_ms: int


@dataclass(frozen=True, slots=True)
class HistoricalCandleRangeRequest:
    ticker: str
    timeframe: str
    from_ms: int
    to_ms: int
    expected_market_data_hash: str


@dataclass(frozen=True, slots=True)
class CandleRangeResult:
    stream: StreamKey
    from_ms: int
    to_ms: int
    market_data_hash: str
    candles: tuple[CanonicalCandle, ...]
