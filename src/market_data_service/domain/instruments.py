"""Instrument configuration and exchange metadata."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

from market_data_service.domain.identity import InstrumentKey, StreamKey


class HistoryPolicy(StrEnum):
    FULL_AVAILABLE = "full_available"


@dataclass(frozen=True, slots=True)
class InstrumentMetadata:
    """Current exchange facts required by the ingestion service."""

    instrument: InstrumentKey
    exchange_symbol: str
    launch_time_ms: int | None = None
    fetched_at_ms: int | None = None

    def __post_init__(self) -> None:
        exchange_symbol = self.exchange_symbol.strip().upper()
        if not exchange_symbol or not exchange_symbol.isascii() or not exchange_symbol.isalnum():
            raise ValueError("exchange_symbol must contain only ASCII letters and digits")
        object.__setattr__(self, "exchange_symbol", exchange_symbol)
        if self.launch_time_ms is not None and self.launch_time_ms < 0:
            raise ValueError("launch_time_ms must be non-negative")
        if self.fetched_at_ms is not None and self.fetched_at_ms < 0:
            raise ValueError("fetched_at_ms must be non-negative")


@dataclass(frozen=True, slots=True)
class InstrumentCoverage:
    """Operator-declared ingestion intent for one instrument."""

    instrument: InstrumentKey
    exchange_symbol: str
    enabled: bool
    canonical_timeframes: tuple[str, ...]
    history_policy: HistoryPolicy

    def __post_init__(self) -> None:
        metadata = InstrumentMetadata(self.instrument, self.exchange_symbol)
        object.__setattr__(self, "exchange_symbol", metadata.exchange_symbol)
        if not self.canonical_timeframes:
            raise ValueError("canonical_timeframes must not be empty")
        normalized = tuple(StreamKey(self.instrument, value).timeframe for value in self.canonical_timeframes)
        if len(set(normalized)) != len(normalized):
            raise ValueError("canonical_timeframes must not contain duplicates")
        if "1m" not in normalized:
            raise ValueError("every configured instrument must include canonical 1m")
        object.__setattr__(self, "canonical_timeframes", normalized)

    @property
    def stream_keys(self) -> tuple[StreamKey, ...]:
        return tuple(StreamKey(self.instrument, timeframe) for timeframe in self.canonical_timeframes)
