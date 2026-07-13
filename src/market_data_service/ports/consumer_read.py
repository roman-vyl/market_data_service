"""Focused read port for canonical candle consumers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from market_data_service.domain.candles import CanonicalCandle
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.stream_state import StreamStateSnapshot


@dataclass(frozen=True, slots=True)
class ConsumerReadSnapshot:
    """One SQLite-consistent lifecycle and candle-range snapshot."""

    state: StreamStateSnapshot
    candles: tuple[CanonicalCandle, ...]


class ConsumerCandleReader(Protocol):
    """Read one stream state and half-open candle range atomically."""

    def read_snapshot(
        self,
        stream: StreamKey,
        *,
        start_time_ms: int,
        end_time_ms: int,
    ) -> ConsumerReadSnapshot: ...
