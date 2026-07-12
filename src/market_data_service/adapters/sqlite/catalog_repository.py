"""Instrument and stream catalog persistence."""

from __future__ import annotations

import sqlite3

from market_data_service.domain.identity import InstrumentKey, StreamKey
from market_data_service.domain.instruments import InstrumentMetadata
from market_data_service.domain.stream_state import StreamLifecycleState


class SqliteCatalogRepository:
    def __init__(self, connection: sqlite3.Connection) -> None:
        self._connection = connection

    def register_stream(
        self,
        stream: StreamKey,
        *,
        exchange_symbol: str,
        now_ms: int,
    ) -> int:
        self._connection.execute(
            """
            INSERT INTO instruments(ticker, exchange_symbol, created_at_ms, updated_at_ms)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(ticker) DO UPDATE SET
                exchange_symbol = excluded.exchange_symbol,
                updated_at_ms = excluded.updated_at_ms
            """,
            (stream.instrument.ticker, exchange_symbol, now_ms, now_ms),
        )
        instrument_id = self._instrument_id(stream.instrument)
        self._connection.execute(
            """
            INSERT INTO streams(instrument_id, timeframe, created_at_ms)
            VALUES (?, ?, ?)
            ON CONFLICT(instrument_id, timeframe) DO NOTHING
            """,
            (instrument_id, stream.timeframe, now_ms),
        )
        stream_id = self.stream_id(stream)
        self._connection.execute(
            """
            INSERT INTO stream_state(
                stream_id, state, state_changed_at_ms, updated_at_ms
            ) VALUES (?, ?, ?, ?)
            ON CONFLICT(stream_id) DO NOTHING
            """,
            (stream_id, StreamLifecycleState.UNINITIALIZED.value, now_ms, now_ms),
        )
        return stream_id

    def stream_id(self, stream: StreamKey) -> int:
        row = self._connection.execute(
            """
            SELECT s.id
            FROM streams s
            JOIN instruments i ON i.id = s.instrument_id
            WHERE i.ticker = ? AND s.timeframe = ?
            """,
            (stream.instrument.ticker, stream.timeframe),
        ).fetchone()
        if row is None:
            raise KeyError(stream.canonical_id)
        return int(row["id"])

    def stream_exists(self, stream: StreamKey) -> bool:
        try:
            self.stream_id(stream)
        except KeyError:
            return False
        return True

    def get_instrument_metadata(self, instrument: InstrumentKey) -> InstrumentMetadata:
        row = self._connection.execute(
            """
            SELECT ticker, exchange_symbol, launch_time_ms, metadata_fetched_at_ms
            FROM instruments
            WHERE ticker = ?
            """,
            (instrument.ticker,),
        ).fetchone()
        if row is None:
            raise KeyError(instrument.ticker)
        return InstrumentMetadata(
            instrument=InstrumentKey(row["ticker"]),
            exchange_symbol=row["exchange_symbol"],
            launch_time_ms=row["launch_time_ms"],
            fetched_at_ms=row["metadata_fetched_at_ms"],
        )

    def save_instrument_metadata(self, metadata: InstrumentMetadata) -> None:
        if metadata.fetched_at_ms is None:
            raise ValueError("instrument metadata fetched_at_ms is required for persistence")
        self._connection.execute(
            """
            UPDATE instruments SET
                exchange_symbol = ?,
                launch_time_ms = ?,
                metadata_fetched_at_ms = ?,
                updated_at_ms = ?
            WHERE ticker = ?
            """,
            (
                metadata.exchange_symbol,
                metadata.launch_time_ms,
                metadata.fetched_at_ms,
                metadata.fetched_at_ms,
                metadata.instrument.ticker,
            ),
        )

    def stream_key(self, stream_id: int) -> StreamKey:
        row = self._connection.execute(
            """
            SELECT i.ticker, s.timeframe
            FROM streams s
            JOIN instruments i ON i.id = s.instrument_id
            WHERE s.id = ?
            """,
            (stream_id,),
        ).fetchone()
        if row is None:
            raise KeyError(stream_id)
        return StreamKey(InstrumentKey(row["ticker"]), row["timeframe"])

    def _instrument_id(self, instrument: InstrumentKey) -> int:
        row = self._connection.execute(
            "SELECT id FROM instruments WHERE ticker = ?",
            (instrument.ticker,),
        ).fetchone()
        if row is None:
            raise KeyError(instrument.ticker)
        return int(row["id"])
