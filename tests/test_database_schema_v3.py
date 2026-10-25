from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from market_data_service.adapters.sqlite import SqliteUnitOfWork, initialize_database
from market_data_service.adapters.sqlite.schema import UnsupportedSchemaVersion
from market_data_service.domain import InstrumentKey, StreamKey
from market_data_service.domain.open_interest import (
    OPEN_INTEREST_TRACK,
    CanonicalOpenInterest,
    TrackProgress,
)

SQLITE_DIR = Path(__file__).parents[1] / "src" / "market_data_service" / "adapters" / "sqlite"
OLD_TABLES = ("instruments", "streams", "candles", "stream_state", "quarantine")


def _v2_database(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.executescript((SQLITE_DIR / "schema_v2.sql").read_text(encoding="utf-8"))
    connection.execute(
        "INSERT INTO instruments(ticker, exchange_symbol, launch_time_ms, created_at_ms, "
        "updated_at_ms) VALUES ('BTCUSDT.P', 'BTCUSDT', 5, 1, 1)"
    )
    connection.execute(
        "INSERT INTO streams(instrument_id, timeframe, created_at_ms) VALUES (1, '5m', 1)"
    )
    connection.execute(
        "INSERT INTO stream_state(stream_id, state, earliest_available_open_time_ms, "
        "latest_committed_open_time_ms, state_changed_at_ms, updated_at_ms) "
        "VALUES (1, 'ready', 0, 300000, 7, 8)"
    )
    for open_time in (0, 300_000):
        connection.execute(
            "INSERT INTO candles VALUES (1, ?, '1', '2', '0.5', '1.5', '10', 'bybit_websocket', 9)",
            (open_time,),
        )
    connection.execute(
        "INSERT INTO quarantine(stream_id, start_ms, end_ms, reason_code, created_at_ms) "
        "VALUES (1, 0, 1, 'x', 1)"
    )
    connection.commit()
    connection.close()


def _dump(path: Path) -> dict[str, list[tuple[object, ...]]]:
    connection = sqlite3.connect(path)
    try:
        return {
            table: connection.execute(f"SELECT * FROM {table} ORDER BY 1, 2").fetchall()
            for table in OLD_TABLES
        }
    finally:
        connection.close()


def _tables(path: Path) -> dict[str, str]:
    connection = sqlite3.connect(path)
    try:
        return {
            str(name): str(sql)
            for name, sql in connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'table'"
            )
        }
    finally:
        connection.close()


def test_v2_to_v3_migration_keeps_every_existing_row(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    _v2_database(path)
    before = _dump(path)
    tables_before = _tables(path)

    initialize_database(path)

    assert _dump(path) == before
    tables_after = _tables(path)
    for name, sql in tables_before.items():
        assert tables_after[name] == sql
    connection = sqlite3.connect(path)
    try:
        assert connection.execute(
            "SELECT value FROM schema_meta WHERE key = 'schema_version'"
        ).fetchone() == ("3",)
        assert connection.execute("SELECT COUNT(*) FROM open_interest").fetchone() == (0,)
        assert connection.execute("SELECT COUNT(*) FROM track_progress").fetchone() == (0,)
    finally:
        connection.close()


def test_migrated_and_fresh_v3_schemas_match(tmp_path: Path) -> None:
    migrated = tmp_path / "migrated.sqlite3"
    fresh = tmp_path / "fresh.sqlite3"
    _v2_database(migrated)
    initialize_database(migrated)
    initialize_database(fresh)

    assert _tables(migrated) == _tables(fresh)


def test_second_initialize_is_a_no_op(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    _v2_database(path)
    initialize_database(path)
    before = _dump(path)

    initialize_database(path)

    assert _dump(path) == before


def test_unknown_version_still_fails_closed(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    _v2_database(path)
    connection = sqlite3.connect(path)
    connection.execute("UPDATE schema_meta SET value = '4' WHERE key = 'schema_version'")
    connection.commit()
    connection.close()

    with pytest.raises(UnsupportedSchemaVersion):
        initialize_database(path)


def test_open_interest_round_trip_identity_and_progress_isolation(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    _v2_database(path)
    initialize_database(path)
    stream = StreamKey(InstrumentKey("BTCUSDT.P"), "5m")
    with SqliteUnitOfWork(path) as unit_of_work:
        state_before = unit_of_work.get_stream_state(stream)
        unit_of_work.save_open_interest(CanonicalOpenInterest(stream, 0, "82500.35", 1))
        unit_of_work.save_open_interest(CanonicalOpenInterest(stream, 0, "82500.36", 2))
        unit_of_work.save_track_progress(
            TrackProgress(stream, OPEN_INTEREST_TRACK, earliest_available_open_time_ms=0)
        )
        unit_of_work.commit()
    with SqliteUnitOfWork(path) as unit_of_work:
        point = unit_of_work.get_open_interest(stream, 0)
        progress = unit_of_work.get_track_progress(stream, OPEN_INTEREST_TRACK)
        assert unit_of_work.get_stream_state(stream) == state_before
    assert point is not None and point.open_interest == "82500.36"
    assert progress.earliest_available_open_time_ms == 0
    connection = sqlite3.connect(path)
    try:
        assert connection.execute("SELECT COUNT(*) FROM open_interest").fetchone() == (1,)
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "INSERT INTO track_progress(stream_id, track, earliest_available_open_time_ms, "
                "discovery_next_open_time_ms, updated_at_ms) VALUES (1, 'open_interest', 0, 5, 1)"
            )
    finally:
        connection.close()
