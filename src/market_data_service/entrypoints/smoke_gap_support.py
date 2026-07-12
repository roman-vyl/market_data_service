"""Smoke-only helpers for creating gap-repair scenarios."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from market_data_service.domain import StreamKey


def delete_candle_for_smoke(
    database_path: Path,
    stream: StreamKey,
    open_time_ms: int,
) -> None:
    """Remove one canonical candle for smoke-only gap-repair verification."""

    connection = sqlite3.connect(database_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        stream_id = connection.execute(
            """
            SELECT s.id
            FROM streams s
            JOIN instruments i ON i.id = s.instrument_id
            WHERE i.ticker = ? AND s.timeframe = ?
            """,
            (stream.instrument.ticker, stream.timeframe),
        ).fetchone()[0]
        connection.execute(
            "DELETE FROM candles WHERE stream_id = ? AND open_time_ms = ?",
            (stream_id, open_time_ms),
        )
        connection.commit()
    finally:
        connection.close()
