"""Transport-neutral observed and canonical candle contracts."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from enum import StrEnum

from market_data_service.domain.decimal_values import (
    DecimalInput,
    decimal_to_canonical_text,
    parse_decimal,
)
from market_data_service.domain.identity import StreamKey


class ObservationSource(StrEnum):
    BYBIT_REST = "bybit_rest"
    BYBIT_WEBSOCKET = "bybit_websocket"


@dataclass(frozen=True, slots=True)
class ObservedCandle:
    """Normalized external observation awaiting validation and classification."""

    stream: StreamKey
    open_time_ms: int
    close_time_ms: int
    open: DecimalInput
    high: DecimalInput
    low: DecimalInput
    close: DecimalInput
    volume: DecimalInput
    confirmed: bool
    observed_at_ms: int
    source: ObservationSource

    def __post_init__(self) -> None:
        object.__setattr__(self, "open", parse_decimal(self.open))
        object.__setattr__(self, "high", parse_decimal(self.high))
        object.__setattr__(self, "low", parse_decimal(self.low))
        object.__setattr__(self, "close", parse_decimal(self.close))
        object.__setattr__(self, "volume", parse_decimal(self.volume))

    @property
    def ohlcv_text(self) -> tuple[str, str, str, str, str]:
        return (
            decimal_to_canonical_text(self.open),
            decimal_to_canonical_text(self.high),
            decimal_to_canonical_text(self.low),
            decimal_to_canonical_text(self.close),
            decimal_to_canonical_text(self.volume),
        )


@dataclass(frozen=True, slots=True)
class CanonicalCandle:
    """Validated candle accepted into canonical storage."""

    stream: StreamKey
    open_time_ms: int
    close_time_ms: int
    open: DecimalInput
    high: DecimalInput
    low: DecimalInput
    close: DecimalInput
    volume: DecimalInput
    source: ObservationSource
    committed_at_ms: int

    def __post_init__(self) -> None:
        object.__setattr__(self, "open", parse_decimal(self.open))
        object.__setattr__(self, "high", parse_decimal(self.high))
        object.__setattr__(self, "low", parse_decimal(self.low))
        object.__setattr__(self, "close", parse_decimal(self.close))
        object.__setattr__(self, "volume", parse_decimal(self.volume))

    @property
    def ohlcv_text(self) -> tuple[str, str, str, str, str]:
        return (
            decimal_to_canonical_text(self.open),
            decimal_to_canonical_text(self.high),
            decimal_to_canonical_text(self.low),
            decimal_to_canonical_text(self.close),
            decimal_to_canonical_text(self.volume),
        )

    @classmethod
    def from_observation(cls, candle: ObservedCandle, *, committed_at_ms: int) -> CanonicalCandle:
        return cls(
            stream=candle.stream,
            open_time_ms=candle.open_time_ms,
            close_time_ms=candle.close_time_ms,
            open=candle.open,
            high=candle.high,
            low=candle.low,
            close=candle.close,
            volume=candle.volume,
            source=candle.source,
            committed_at_ms=committed_at_ms,
        )
