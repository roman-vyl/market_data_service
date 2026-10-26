"""Canonical candle persistence."""

from __future__ import annotations

import sqlite3

from market_data_service.adapters.sqlite.catalog_repository import SqliteCatalogRepository
from market_data_service.domain.candles import CanonicalCandle, ObservationSource
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.timeframes import get_timeframe


class SqliteCandleRepository:
    def __init__(self, connection: sqlite3.Connection, catalog: SqliteCatalogRepository) -> None:
        self._connection = connection
        self._catalog = catalog

    def get(self, stream: StreamKey, open_time_ms: int) -> CanonicalCandle | None:
        stream_id = self._catalog.stream_id(stream)
        row = self._connection.execute(
            "SELECT * FROM candles WHERE stream_id = ? AND open_time_ms = ?",
            (stream_id, open_time_ms),
        ).fetchone()
        if row is None:
            return None
        return CanonicalCandle(
            stream=stream,
            open_time_ms=row["open_time_ms"],
            close_time_ms=open_time_ms + get_timeframe(stream.timeframe).duration_ms - 1,
            open=row["open_value"],
            high=row["high_value"],
            low=row["low_value"],
            close=row["close_value"],
            volume=row["volume_value"],
            source=ObservationSource(row["source"]),
            committed_at_ms=row["committed_at_ms"],
        )

    def list_range(
        self,
        stream: StreamKey,
        *,
        start_time_ms: int,
        end_time_ms: int,
    ) -> tuple[CanonicalCandle, ...]:
        stream_id = self._catalog.stream_id(stream)
        rows = self._connection.execute(
            """
            SELECT *
            FROM candles
            WHERE stream_id = ? AND open_time_ms >= ? AND open_time_ms < ?
            ORDER BY open_time_ms
            """,
            (stream_id, start_time_ms, end_time_ms),
        ).fetchall()
        step_ms = get_timeframe(stream.timeframe).duration_ms
        return tuple(
            CanonicalCandle(
                stream=stream,
                open_time_ms=row["open_time_ms"],
                close_time_ms=row["open_time_ms"] + step_ms - 1,
                open=row["open_value"],
                high=row["high_value"],
                low=row["low_value"],
                close=row["close_value"],
                volume=row["volume_value"],
                source=ObservationSource(row["source"]),
                committed_at_ms=row["committed_at_ms"],
            )
            for row in rows
        )

    def list_range_with_open_interest(
        self,
        stream: StreamKey,
        *,
        start_time_ms: int,
        end_time_ms: int,
    ) -> tuple[tuple[CanonicalCandle, ...], tuple[str | None, ...]]:
        """Candles of the range and the open interest stored for each one.

        One query joins the open interest of the same stream and open time;
        a bar without a stored value gets ``None``.
        """
        stream_id = self._catalog.stream_id(stream)
        rows = self._connection.execute(
            """
            SELECT candles.*, open_interest.open_interest_value AS open_interest_value
            FROM candles
            LEFT JOIN open_interest
              ON open_interest.stream_id = candles.stream_id
             AND open_interest.open_time_ms = candles.open_time_ms
            WHERE candles.stream_id = ? AND candles.open_time_ms >= ? AND candles.open_time_ms < ?
            ORDER BY candles.open_time_ms
            """,
            (stream_id, start_time_ms, end_time_ms),
        ).fetchall()
        step_ms = get_timeframe(stream.timeframe).duration_ms
        candles = tuple(
            CanonicalCandle(
                stream=stream,
                open_time_ms=row["open_time_ms"],
                close_time_ms=row["open_time_ms"] + step_ms - 1,
                open=row["open_value"],
                high=row["high_value"],
                low=row["low_value"],
                close=row["close_value"],
                volume=row["volume_value"],
                source=ObservationSource(row["source"]),
                committed_at_ms=row["committed_at_ms"],
            )
            for row in rows
        )
        return candles, tuple(row["open_interest_value"] for row in rows)

    def committed_bounds(self, stream: StreamKey) -> tuple[int | None, int | None]:
        stream_id = self._catalog.stream_id(stream)
        row = self._connection.execute(
            """
            SELECT MIN(open_time_ms) AS earliest, MAX(open_time_ms) AS latest
            FROM candles
            WHERE stream_id = ?
            """,
            (stream_id,),
        ).fetchone()
        return row["earliest"], row["latest"]

    def insert(self, candle: CanonicalCandle) -> None:
        self._connection.execute(
            """
            INSERT INTO candles(
                stream_id, open_time_ms, open_value, high_value, low_value,
                close_value, volume_value, source, committed_at_ms
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                self._catalog.stream_id(candle.stream),
                candle.open_time_ms,
                *candle.ohlcv_text,
                candle.source.value,
                candle.committed_at_ms,
            ),
        )

    def replace(self, candle: CanonicalCandle) -> None:
        self._connection.execute(
            """
            UPDATE candles SET
                open_value = ?, high_value = ?, low_value = ?, close_value = ?,
                volume_value = ?, source = ?, committed_at_ms = ?
            WHERE stream_id = ? AND open_time_ms = ?
            """,
            (
                *candle.ohlcv_text,
                candle.source.value,
                candle.committed_at_ms,
                self._catalog.stream_id(candle.stream),
                candle.open_time_ms,
            ),
        )
