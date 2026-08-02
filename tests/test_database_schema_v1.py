from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from market_data_service.adapters.sqlite import initialize_database
from market_data_service.adapters.sqlite.schema import UnsupportedSchemaVersion

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_V1 = ROOT / "src/market_data_service/adapters/sqlite/schema_v1.sql"
SCHEMA_V2 = ROOT / "src/market_data_service/adapters/sqlite/schema_v2.sql"
INSERT_INSTRUMENT_SQL = """
INSERT INTO instruments(ticker, exchange_symbol, created_at_ms, updated_at_ms)
VALUES (?, ?, 1, 1)
"""


def _open_db() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    connection.executescript(SCHEMA_V2.read_text(encoding="utf-8"))
    return connection


def test_schema_v2_creates_only_approved_tables() -> None:
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
    ).fetchone() == ("2",)


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


def test_schema_v2_does_not_include_deferred_tables() -> None:
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
    assert "lower_bound_discovery_next_open_time_ms" in columns

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


def test_schema_v1_migrates_to_v2_without_losing_canonical_data(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    connection = sqlite3.connect(path)
    connection.executescript(SCHEMA_V1.read_text(encoding="utf-8"))
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
        VALUES (?, 'bootstrapping', 1, 1)
        """,
        (stream_id,),
    )
    connection.execute(
        "INSERT INTO candles VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (stream_id, 0, "1", "2", "1", "2", "3", "bybit_rest", 1),
    )
    connection.commit()
    connection.close()

    initialize_database(path)

    migrated = sqlite3.connect(path)
    try:
        assert migrated.execute(
            "SELECT value FROM schema_meta WHERE key='schema_version'"
        ).fetchone() == ("2",)
        assert migrated.execute("SELECT COUNT(*) FROM candles").fetchone() == (1,)
        assert migrated.execute(
            "SELECT state, lower_bound_discovery_next_open_time_ms FROM stream_state"
        ).fetchone() == ("bootstrapping", None)
    finally:
        migrated.close()


def test_unknown_schema_version_fails_closed_and_preserves_file(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    connection = sqlite3.connect(path)
    connection.executescript(SCHEMA_V1.read_text(encoding="utf-8"))
    connection.execute(
        "UPDATE schema_meta SET value='999' WHERE key='schema_version'"
    )
    connection.commit()
    connection.close()

    with pytest.raises(UnsupportedSchemaVersion):
        initialize_database(path)

    preserved = sqlite3.connect(path)
    try:
        assert preserved.execute(
            "SELECT value FROM schema_meta WHERE key='schema_version'"
        ).fetchone() == ("999",)
    finally:
        preserved.close()


def test_failed_v1_migration_rolls_back_schema_version(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    connection = sqlite3.connect(path)
    connection.executescript(SCHEMA_V1.read_text(encoding="utf-8"))
    connection.execute("DROP TABLE stream_state")
    connection.commit()
    connection.close()

    with pytest.raises(sqlite3.OperationalError):
        initialize_database(path)

    preserved = sqlite3.connect(path)
    try:
        assert preserved.execute(
            "SELECT value FROM schema_meta WHERE key='schema_version'"
        ).fetchone() == ("1",)
        tables = {
            row[0]
            for row in preserved.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        assert "stream_state_v2" not in tables
    finally:
        preserved.close()


def test_schema_v2_validation_requires_discovery_cursor_column(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    connection = sqlite3.connect(path)
    connection.executescript(SCHEMA_V1.read_text(encoding="utf-8"))
    connection.execute("UPDATE schema_meta SET value='2' WHERE key='schema_version'")
    connection.commit()
    connection.close()

    with pytest.raises(
        UnsupportedSchemaVersion,
        match="missing lower_bound_discovery_next_open_time_ms",
    ):
        initialize_database(path)
