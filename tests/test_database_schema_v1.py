from __future__ import annotations

import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = ROOT / "src/market_data_service/adapters/sqlite/schema_v1.sql"
INSERT_INSTRUMENT_SQL = """
INSERT INTO instruments(ticker, exchange_symbol, created_at_ms, updated_at_ms)
VALUES (?, ?, 1, 1)
"""


def _open_db() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.executescript(SCHEMA.read_text(encoding="utf-8"))
    return connection


def test_schema_v1_creates_only_approved_tables() -> None:
    connection = _open_db()
    tables = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    }
    assert tables == {
        "schema_meta",
        "instruments",
        "streams",
        "candles",
        "stream_state",
        "quarantine",
    }
    assert connection.execute(
        "SELECT value FROM schema_meta WHERE key='schema_version'"
    ).fetchone() == ("1",)


def test_candle_key_is_unique_per_stream_and_open_time() -> None:
    connection = _open_db()
    connection.execute(
        INSERT_INSTRUMENT_SQL,
        ("BTCUSDT.P", "BTCUSDT"),
    )
    instrument_id = connection.execute("SELECT id FROM instruments").fetchone()[0]
    connection.execute(
        "INSERT INTO streams(instrument_id, timeframe, created_at_ms) VALUES (?, '1m', 1)",
        (instrument_id,),
    )
    stream_id = connection.execute("SELECT id FROM streams").fetchone()[0]
    values = (stream_id, 60_000, "1", "2", "0.5", "1.5", "10", "bybit_rest", 1)
    connection.execute("INSERT INTO candles VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", values)

    try:
        connection.execute("INSERT INTO candles VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", values)
    except sqlite3.IntegrityError:
        pass
    else:
        raise AssertionError("duplicate candle key must be rejected by storage")


def test_instrument_ticker_and_exchange_symbol_are_unique() -> None:
    connection = _open_db()
    connection.execute(INSERT_INSTRUMENT_SQL, ("BTCUSDT.P", "BTCUSDT"))
    for values in (
        ("BTCUSDT.P", "OTHER"),
        ("OTHER.P", "BTCUSDT"),
    ):
        try:
            connection.execute(INSERT_INSTRUMENT_SQL, values)
        except sqlite3.IntegrityError:
            continue
        raise AssertionError("ticker and exchange_symbol must each be unique")


def test_schema_v1_does_not_include_deferred_tables() -> None:
    connection = _open_db()
    tables = {
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )
    }
    assert tables.isdisjoint(
        {
            "market_events",
            "consumer_offsets",
            "bootstrap_runs",
            "bootstrap_windows",
            "stream_gaps",
            "candle_corrections",
            "instrument_metadata_history",
        }
    )


def test_stream_state_schema_accepts_connecting_and_tracks_state_change_time() -> None:
    connection = _open_db()
    columns = {row[1] for row in connection.execute("PRAGMA table_info(stream_state)")}
    assert "state_changed_at_ms" in columns

    connection.execute(INSERT_INSTRUMENT_SQL, ("BTCUSDT.P", "BTCUSDT"))
    instrument_id = connection.execute("SELECT id FROM instruments").fetchone()[0]
    connection.execute(
        "INSERT INTO streams(instrument_id, timeframe, created_at_ms) VALUES (?, '1m', 1)",
        (instrument_id,),
    )
    stream_id = connection.execute("SELECT id FROM streams").fetchone()[0]
    connection.execute(
        """
        INSERT INTO stream_state(stream_id, state, state_changed_at_ms, updated_at_ms)
        VALUES (?, 'connecting', 1, 1)
        """,
        (stream_id,),
    )
    assert connection.execute(
        "SELECT state FROM stream_state WHERE stream_id=?", (stream_id,)
    ).fetchone() == ("connecting",)
