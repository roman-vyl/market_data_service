from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from dataclasses import replace
from pathlib import Path

import pytest

from market_data_service.adapters.sqlite import (
    SqliteUnitOfWork,
    initialize_database,
    register_stream,
)
from market_data_service.adapters.sqlite.connection import open_read_only_connection
from market_data_service.adapters.sqlite.stream_state_reader import SqliteStreamStateReader
from market_data_service.domain.identity import InstrumentKey, StreamKey
from market_data_service.domain.stream_state import (
    InvalidStreamTransition,
    StreamStateSnapshot,
)
from market_data_service.runtime.lifecycle import RuntimeLifecycleRecorder


def _prepare(path: Path) -> StreamKey:
    initialize_database(path)
    stream = StreamKey(InstrumentKey("SOLUSDT.P"), "5m")
    register_stream(path, stream, exchange_symbol="SOLUSDT", now_ms=1)
    return stream


def _recorder(path: Path) -> RuntimeLifecycleRecorder:
    return RuntimeLifecycleRecorder(
        lambda: SqliteUnitOfWork(path),
        lambda: 10,
        state_reader=SqliteStreamStateReader(path),
    )


@pytest.mark.parametrize("commit", [True, False])
def test_snapshot_finishes_while_writer_held_and_sees_only_commits(
    tmp_path: Path, commit: bool
) -> None:
    path = tmp_path / "market.sqlite"
    stream = _prepare(path)
    recorder = _recorder(path)
    previous = recorder.snapshot(stream)
    writer = sqlite3.connect(path)
    pool = ThreadPoolExecutor(max_workers=1)
    try:
        assert writer.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("UPDATE stream_state SET latest_committed_open_time_ms = 300000")
        result = pool.submit(recorder.snapshot, stream)
        # The writer is not released until the read completes. The deadline is
        # only a failure bound, not a performance assertion.
        assert result.result(timeout=5) == previous
        assert writer.in_transaction
        if commit:
            writer.commit()
        else:
            writer.rollback()
        assert recorder.snapshot(stream).latest_committed_open_time_ms == (
            300000 if commit else None
        )
    finally:
        writer.rollback()
        writer.close()
        pool.shutdown(wait=True)


def test_read_only_connection_enforces_access_and_safety_pragmas(tmp_path: Path) -> None:
    path = tmp_path / "market #?%.sqlite"
    stream = _prepare(path)
    connection = open_read_only_connection(path)
    try:
        assert connection.execute("PRAGMA foreign_keys").fetchone()[0] == 1
        assert connection.execute("PRAGMA busy_timeout").fetchone()[0] == 30000
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            connection.execute("UPDATE stream_state SET updated_at_ms = 2")
    finally:
        connection.close()
    assert SqliteStreamStateReader(path).get_stream_state(stream).stream == stream


def test_reader_does_not_create_missing_database(tmp_path: Path) -> None:
    path = tmp_path / "missing.sqlite"
    stream = StreamKey(InstrumentKey("SOLUSDT.P"), "5m")
    with pytest.raises(sqlite3.OperationalError):
        SqliteStreamStateReader(path).get_stream_state(stream)
    assert not path.exists()


def test_stream_isolation_and_missing_state_errors(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    first = _prepare(path)
    second = StreamKey(InstrumentKey("ETHUSDT.P"), "5m")
    register_stream(path, second, exchange_symbol="ETHUSDT", now_ms=2)
    reader = SqliteStreamStateReader(path)
    assert reader.get_stream_state(first).updated_at_ms == 1
    assert reader.get_stream_state(second).updated_at_ms == 2
    with pytest.raises(KeyError):
        reader.get_stream_state(StreamKey(InstrumentKey("XRPUSDT.P"), "5m"))
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute("DELETE FROM stream_state WHERE updated_at_ms = 2")
    with pytest.raises(KeyError):
        reader.get_stream_state(second)


@pytest.mark.parametrize("missing", [True, False])
def test_reader_closes_connection_on_success_and_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, missing: bool
) -> None:
    path = tmp_path / "market.sqlite"
    stream = _prepare(path)
    connection = open_read_only_connection(path)
    monkeypatch.setattr(
        "market_data_service.adapters.sqlite.stream_state_reader.open_read_only_connection",
        lambda _: connection,
    )
    if missing:
        with pytest.raises(KeyError):
            SqliteStreamStateReader(path).get_stream_state(
                StreamKey(InstrumentKey("ETHUSDT.P"), "5m")
            )
    else:
        SqliteStreamStateReader(path).get_stream_state(stream)
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        connection.execute("SELECT 1")


def test_transition_rereads_state_after_preliminary_snapshot(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _prepare(path)
    reader = SqliteStreamStateReader(path)

    class ReaderWithConcurrentProgress:
        def get_stream_state(self, key: StreamKey) -> StreamStateSnapshot:
            preliminary = reader.get_stream_state(key)
            with SqliteUnitOfWork(path) as unit:
                unit.save_stream_state(
                    replace(preliminary, latest_committed_open_time_ms=300000, updated_at_ms=20)
                )
                unit.commit()
            return preliminary

    recorder = RuntimeLifecycleRecorder(
        lambda: SqliteUnitOfWork(path), lambda: 10, state_reader=ReaderWithConcurrentProgress()
    )
    result = recorder.prepare_for_bootstrap(stream)
    assert result.latest_committed_open_time_ms == 300000
    assert result.updated_at_ms == 20
    assert reader.get_stream_state(stream) == result


def test_transition_still_validates_current_durable_state(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _prepare(path)
    with pytest.raises(InvalidStreamTransition):
        _recorder(path).mark_ready(stream)
    assert SqliteStreamStateReader(path).get_stream_state(stream).state.value == "uninitialized"
