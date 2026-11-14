"""Batch storage contract and lifecycle projection interleavings."""

import asyncio
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest
from tests.test_lifecycle_sync_triggers import _harness, _ready

from market_data_service.adapters.sqlite import SqliteUnitOfWork
from market_data_service.adapters.sqlite.connection import open_read_only_connection
from market_data_service.adapters.sqlite.stream_state_reader import SqliteStreamStateReader
from market_data_service.application.realtime.events import Disconnected
from market_data_service.application.realtime.recovery_state import RealtimeRecoveryStateRecorder
from market_data_service.domain.identity import InstrumentKey, StreamKey
from market_data_service.domain.stream_state import InvalidStreamTransition, StreamLifecycleState


def test_batch_is_one_connection_select_and_preserves_order_duplicates(tmp_path, monkeypatch):
    h = _harness(tmp_path, count=40)
    connections = []
    statements = []

    def connect(path):
        c = open_read_only_connection(path)
        c.set_trace_callback(statements.append)
        connections.append(c)
        return c

    monkeypatch.setattr(
        "market_data_service.adapters.sqlite.stream_state_reader.open_read_only_connection", connect
    )
    requested = (*reversed(h.streams), h.streams[0])
    result = h.reader.get_stream_states(requested)
    assert tuple(s.stream for s in result) == requested
    assert len(connections) == 1
    assert sum(s.lstrip().upper().startswith(("SELECT", "WITH")) for s in statements) == 1
    assert not any("IMMEDIATE" in s for s in statements)
    with pytest.raises(sqlite3.ProgrammingError):
        connections[0].execute("SELECT 1")
    h.reader.get_stream_states(())
    assert len(connections) == 1


@pytest.mark.parametrize("missing_state", [False, True])
def test_batch_missing_identity_or_state_closes_and_fails(tmp_path, monkeypatch, missing_state):
    h = _harness(tmp_path)
    requested = h.streams
    if missing_state:
        with SqliteUnitOfWork(tmp_path / "market.sqlite3") as u:
            u._connection.execute(
                "DELETE FROM stream_state WHERE stream_id = (SELECT max(id) FROM streams)"
            )
            u.commit()
    else:
        requested += (StreamKey(InstrumentKey("MISSINGUSDT.P"), "5m"),)
    conns = []

    def connect(path):
        c = open_read_only_connection(path)
        conns.append(c)
        return c

    monkeypatch.setattr(
        "market_data_service.adapters.sqlite.stream_state_reader.open_read_only_connection", connect
    )
    with pytest.raises(KeyError):
        h.reader.get_stream_states(requested)
    with pytest.raises(sqlite3.ProgrammingError):
        conns[0].execute("SELECT 1")


def test_batch_does_not_create_database(tmp_path):
    path = tmp_path / "missing.sqlite3"
    with pytest.raises(sqlite3.OperationalError):
        SqliteStreamStateReader(path).get_stream_states(
            (StreamKey(InstrumentKey("TESTUSDT.P"), "5m"),)
        )
    assert not path.exists()


@pytest.mark.parametrize("commit", [False, True])
def test_batch_reads_prior_commit_while_writer_held_then_refreshes(tmp_path, commit):
    h = _harness(tmp_path)
    previous = h.reader.get_stream_states(h.streams)
    with sqlite3.connect(tmp_path / "market.sqlite3") as writer, ThreadPoolExecutor(1) as pool:
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("UPDATE stream_state SET latest_committed_open_time_ms=300000")
        assert pool.submit(h.reader.get_stream_states, h.streams).result(timeout=5) == previous
        assert writer.in_transaction
        if commit:
            writer.commit()
        else:
            writer.rollback()
    result = h.reader.get_stream_states(h.streams)
    assert all(s.latest_committed_open_time_ms == (300000 if commit else 0) for s in result)


def test_full_sweep_batches_and_scoped_refresh_keeps_point_read(tmp_path, monkeypatch):
    h = _harness(tmp_path)
    original = h.reader.get_stream_states
    batches = []

    def batch(streams):
        batches.append(tuple(streams))
        return original(streams)

    monkeypatch.setattr(h.reader, "get_stream_states", batch)
    h.runtime._sync_lifecycle()
    assert batches == [h.streams]
    assert h.reader.reads == list(h.streams)
    h.reader.reads.clear()
    h.runtime.on_open_interest_changed(h.streams[0])
    assert len(batches) == 1 and h.reader.reads == [h.streams[0]]


def test_facts_are_captured_before_clock_crosses_grace_inside_batch(tmp_path, monkeypatch):
    h = _harness(tmp_path)
    original = h.reader.get_stream_states

    def batch(streams):
        h.now[0] = 615001
        return original(streams)

    monkeypatch.setattr(h.reader, "get_stream_states", batch)
    h.runtime._sync_lifecycle()
    assert _ready(h) == [True, True]
    asyncio.run(h.sweep())
    assert _ready(h) == [False, False]


def test_no_write_uses_batch_projection_until_next_sweep(tmp_path, monkeypatch):
    h = _harness(tmp_path)
    path = tmp_path / "market.sqlite3"
    h.supervisor.observe_event(Disconnected(None, None, h.now[0]))
    recovery = RealtimeRecoveryStateRecorder(lambda: SqliteUnitOfWork(path), lambda: h.now[0])
    for s in h.streams:
        recovery.mark_unavailable(s, reason="disconnect")
    original = h.reader.get_stream_states
    changed = False

    def batch(streams):
        nonlocal changed
        result = original(streams)
        if not changed:
            recovery.ensure_auditing(h.streams[0])
            changed = True
        return result

    monkeypatch.setattr(h.reader, "get_stream_states", batch)
    h.runtime._sync_lifecycle()
    assert h.status.readiness_document()["streams"][0]["durable_state"] == "degraded"
    assert h.lifecycle.snapshot(h.streams[0]).state is StreamLifecycleState.AUDITING
    assert _ready(h) == [False, False]
    h.runtime._sync_lifecycle()
    assert h.status.readiness_document()["streams"][0]["durable_state"] == "auditing"


def test_transition_keeps_progress_committed_after_batch(tmp_path, monkeypatch):
    h = _harness(tmp_path)
    path = tmp_path / "market.sqlite3"
    stream = h.streams[0]
    with SqliteUnitOfWork(path) as u:
        u.save_stream_state(
            replace(u.get_stream_state(stream), state=StreamLifecycleState.CONNECTING)
        )
        u.commit()
    original = h.reader.get_stream_states

    def batch(streams):
        result = original(streams)
        with SqliteUnitOfWork(path) as u:
            u.save_stream_state(
                replace(u.get_stream_state(stream), latest_committed_open_time_ms=300000)
            )
            u.commit()
        return result

    monkeypatch.setattr(h.reader, "get_stream_states", batch)
    h.runtime._sync_lifecycle()
    state = h.lifecycle.snapshot(stream)
    assert state.state is StreamLifecycleState.READY
    assert state.latest_committed_open_time_ms == 300000


def test_concurrent_lifecycle_change_after_batch_rejects_stale_ready(tmp_path, monkeypatch):
    h = _harness(tmp_path)
    path = tmp_path / "market.sqlite3"
    stream = h.streams[0]
    with SqliteUnitOfWork(path) as u:
        u.save_stream_state(
            replace(u.get_stream_state(stream), state=StreamLifecycleState.CONNECTING)
        )
        u.commit()
    recovery = RealtimeRecoveryStateRecorder(lambda: SqliteUnitOfWork(path), lambda: h.now[0])
    original = h.reader.get_stream_states
    committed = []

    def batch(streams):
        result = original(streams)
        assert result[0].state is StreamLifecycleState.CONNECTING
        assert h.supervisor.facts(stream).data_ready
        recovery.mark_unavailable(stream, reason="concurrent_recovery")
        committed.append(h.lifecycle.snapshot(stream))
        return result

    monkeypatch.setattr(h.reader, "get_stream_states", batch)
    with pytest.raises(
        InvalidStreamTransition, match="illegal stream transition degraded -> ready"
    ):
        h.runtime._sync_lifecycle()
    assert committed[0].state is StreamLifecycleState.DEGRADED
    assert h.lifecycle.snapshot(stream) == committed[0]
