"""Vendor-neutral market-data source ports."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from market_data_service.domain.candles import ObservedCandle
from market_data_service.domain.identity import InstrumentKey, StreamKey
from market_data_service.domain.instruments import ExchangeInstrumentSpecification
from market_data_service.domain.windows import TimeWindow


class RecoverableMarketDataFailure:
    """Marker for transient source failures that a later run may recover."""


class InstrumentMetadataSource(Protocol):
    def get_instrument_specification(
        self, instrument: InstrumentKey
    ) -> ExchangeInstrumentSpecification: ...

    def get_launch_time_ms(self, instrument: InstrumentKey) -> int: ...


class HistoricalCandleSource(Protocol):
    def fetch_closed_candles(
        self,
        stream: StreamKey,
        window: TimeWindow,
        *,
        observed_at_ms: int,
    ) -> Sequence[ObservedCandle]: ...
