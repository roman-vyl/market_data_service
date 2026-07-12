"""SQLite implementation of the canonical storage unit of work."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from market_data_service.adapters.sqlite.candle_repository import SqliteCandleRepository
from market_data_service.adapters.sqlite.catalog_repository import SqliteCatalogRepository
from market_data_service.adapters.sqlite.connection import open_connection
from market_data_service.adapters.sqlite.quarantine_repository import SqliteQuarantineRepository
from market_data_service.adapters.sqlite.stream_state_repository import SqliteStreamStateRepository
from market_data_service.domain.candles import CanonicalCandle
from market_data_service.domain.identity import InstrumentKey, StreamKey
from market_data_service.domain.instruments import InstrumentMetadata
from market_data_service.domain.stream_state import StreamStateSnapshot


class SqliteUnitOfWork:
    def __init__(self, database_path: Path | str) -> None:
        self._database_path = database_path
        self._connection: sqlite3.Connection | None = None

    def __enter__(self) -> SqliteUnitOfWork:
        self._connection = open_connection(self._database_path)
        self._connection.execute("BEGIN IMMEDIATE")
        self._catalog = SqliteCatalogRepository(self._connection)
        self._candles = SqliteCandleRepository(self._connection, self._catalog)
        self._states = SqliteStreamStateRepository(self._connection, self._catalog)
        self._quarantine = SqliteQuarantineRepository(self._connection, self._catalog)
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self._connection is None:
            return
        if exc_type is not None:
            self._connection.rollback()
        self._connection.close()
        self._connection = None

    def stream_exists(self, stream: StreamKey) -> bool:
        return self._catalog.stream_exists(stream)

    def get_instrument_metadata(self, instrument: InstrumentKey) -> InstrumentMetadata:
        return self._catalog.get_instrument_metadata(instrument)

    def save_instrument_metadata(self, metadata: InstrumentMetadata) -> None:
        self._catalog.save_instrument_metadata(metadata)

    def get_candle(self, stream: StreamKey, open_time_ms: int) -> CanonicalCandle | None:
        return self._candles.get(stream, open_time_ms)

    def list_candles(
        self,
        stream: StreamKey,
        *,
        start_time_ms: int,
        end_time_ms: int,
    ) -> tuple[CanonicalCandle, ...]:
        return self._candles.list_range(
            stream,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )

    def insert_candle(self, candle: CanonicalCandle) -> None:
        self._candles.insert(candle)

    def replace_candle(self, candle: CanonicalCandle) -> None:
        self._candles.replace(candle)

    def get_stream_state(self, stream: StreamKey) -> StreamStateSnapshot:
        return self._states.get(stream)

    def save_stream_state(self, snapshot: StreamStateSnapshot) -> None:
        self._states.save(snapshot)

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
    ) -> None:
        self._quarantine.add(
            stream=stream,
            start_ms=start_ms,
            end_ms=end_ms,
            reason_code=reason_code,
            detail=detail,
            payload_json=payload_json,
            created_at_ms=created_at_ms,
        )

    def commit(self) -> None:
        self._connection_or_raise().commit()

    def rollback(self) -> None:
        self._connection_or_raise().rollback()

    def _connection_or_raise(self) -> sqlite3.Connection:
        if self._connection is None:
            raise RuntimeError("unit of work is not active")
        return self._connection
