"""Persistence ports used by application use cases."""

from __future__ import annotations

from typing import Protocol

from market_data_service.domain.candles import CanonicalCandle
from market_data_service.domain.identity import InstrumentKey, StreamKey
from market_data_service.domain.instruments import InstrumentMetadata
from market_data_service.domain.stream_state import StreamStateSnapshot


class CanonicalStorageUnitOfWork(Protocol):
    """Atomic storage boundary for one ingestion decision."""

    def __enter__(self) -> CanonicalStorageUnitOfWork: ...

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None: ...

    def stream_exists(self, stream: StreamKey) -> bool: ...

    def get_instrument_metadata(self, instrument: InstrumentKey) -> InstrumentMetadata: ...

    def save_instrument_metadata(self, metadata: InstrumentMetadata) -> None: ...

    def get_candle(self, stream: StreamKey, open_time_ms: int) -> CanonicalCandle | None: ...

    def list_candles(
        self,
        stream: StreamKey,
        *,
        start_time_ms: int,
        end_time_ms: int,
    ) -> tuple[CanonicalCandle, ...]: ...

    def insert_candle(self, candle: CanonicalCandle) -> None: ...

    def replace_candle(self, candle: CanonicalCandle) -> None: ...

    def get_stream_state(self, stream: StreamKey) -> StreamStateSnapshot: ...

    def save_stream_state(self, snapshot: StreamStateSnapshot) -> None: ...

    def record_quarantine(
        self,
        *,
        stream: StreamKey,
        start_ms: int,
        end_ms: int,
        reason_code: str,
        detail: str,
        payload_json: str | None,
        created_at_ms: int,
    ) -> None: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...
