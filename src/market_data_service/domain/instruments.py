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
        normalized = tuple(
            StreamKey(self.instrument, value).timeframe for value in self.canonical_timeframes
        )
        if len(set(normalized)) != len(normalized):
            raise ValueError("canonical_timeframes must not contain duplicates")
        object.__setattr__(self, "canonical_timeframes", normalized)

    @property
    def stream_keys(self) -> tuple[StreamKey, ...]:
        return tuple(
            StreamKey(self.instrument, timeframe) for timeframe in self.canonical_timeframes
        )

@dataclass(frozen=True, slots=True)
class ExchangeInstrumentSpecification:
    """Exchange-declared identity facts used to validate operator mappings."""

    instrument: InstrumentKey
    exchange_symbol: str
    category: str
    contract_type: str
    status: str
    settle_coin: str
    launch_time_ms: int

    def __post_init__(self) -> None:
        normalized_symbol = self.exchange_symbol.strip().upper()
        normalized_category = self.category.strip().lower()
        normalized_contract = self.contract_type.strip()
        normalized_status = self.status.strip()
        normalized_settle = self.settle_coin.strip().upper()
        if not normalized_symbol:
            raise ValueError("exchange_symbol must not be empty")
        if not normalized_category:
            raise ValueError("category must not be empty")
        if not normalized_contract:
            raise ValueError("contract_type must not be empty")
        if not normalized_status:
            raise ValueError("status must not be empty")
        if not normalized_settle:
            raise ValueError("settle_coin must not be empty")
        if self.launch_time_ms < 0:
            raise ValueError("launch_time_ms must be non-negative")
        object.__setattr__(self, "exchange_symbol", normalized_symbol)
        object.__setattr__(self, "category", normalized_category)
        object.__setattr__(self, "contract_type", normalized_contract)
        object.__setattr__(self, "status", normalized_status)
        object.__setattr__(self, "settle_coin", normalized_settle)
