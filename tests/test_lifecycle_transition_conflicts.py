"""Only stale source states suppress realtime lifecycle decisions."""

from dataclasses import replace

import pytest
from tests.test_lifecycle_sync_triggers import _harness, _ready

from market_data_service.adapters.sqlite import SqliteUnitOfWork
from market_data_service.application.realtime.recovery_state import RealtimeRecoveryStateRecorder
from market_data_service.domain.stream_state import InvalidStreamTransition, StreamLifecycleState


@pytest.mark.parametrize(
    ("initial", "concurrent", "mode"),
    [
        ("connecting", "degraded", "ready"),
        ("degraded", "auditing", "ready"),
        ("degraded", "connecting", "ready"),
        ("degraded", "ready", "ready"),
        ("ready", "connecting", "not_ready"),
        ("connecting", "ready", "not_ready"),
        ("ready", "auditing", "fatal"),
    ],
)
def test_changed_state_abandons_decision_without_retry(
    tmp_path, monkeypatch, initial, concurrent, mode
):
    h = _harness(tmp_path)
    path = tmp_path / "market.sqlite3"
    stream = h.streams[0]
    with SqliteUnitOfWork(path) as u:
        u.save_stream_state(
            replace(u.get_stream_state(stream), state=StreamLifecycleState(initial))
        )
        u.commit()
    if mode == "not_ready":
        h.now[0] = 615001
    if mode == "fatal":
        all_facts = h.supervisor.all_facts
        monkeypatch.setattr(
            h.supervisor,
            "all_facts",
            lambda: tuple(replace(f, fatal_error_code="test_fatal") for f in all_facts()),
        )
    original = h.reader.get_stream_states
    batches = []
    committed = []

    def batch(streams):
        batches.append(tuple(streams))
        result = original(streams)
        with SqliteUnitOfWork(path) as u:
            current = replace(
                u.get_stream_state(stream),
                state=StreamLifecycleState(concurrent),
                latest_committed_open_time_ms=300000,
                last_error_code="recovery_owns_state",
            )
            u.save_stream_state(current)
            u.commit()
        committed.append(current)
        return result

    monkeypatch.setattr(h.reader, "get_stream_states", batch)
    h.runtime._sync_lifecycle()
    assert batches == [h.streams]
    assert h.reader.reads == list(h.streams)
    assert h.lifecycle.snapshot(stream) == committed[0]
    projected = h.status.readiness_document()["streams"][0]
    assert projected["durable_state"] == concurrent
    assert _ready(h)[0] is (concurrent == "ready" and mode == "ready")
    if mode == "ready" and concurrent in {"connecting", "degraded"}:
        monkeypatch.setattr(h.reader, "get_stream_states", original)
        h.runtime._sync_lifecycle()
        assert h.lifecycle.snapshot(stream).state is StreamLifecycleState.READY
        assert _ready(h)[0]


def test_conflict_between_connecting_and_ready_does_not_continue(tmp_path, monkeypatch):
    h = _harness(tmp_path)
    stream = h.streams[0]
    path = tmp_path / "market.sqlite3"
    recovery = RealtimeRecoveryStateRecorder(lambda: SqliteUnitOfWork(path), lambda: h.now[0])
    recovery.mark_unavailable(stream, reason="initial_recovery")
    original = h.lifecycle._save_transition

    def save(uow, snapshot, target, **kwargs):
        result = original(uow, snapshot, target, **kwargs)
        if snapshot.stream == stream and target is StreamLifecycleState.CONNECTING:
            recovery.mark_unavailable(stream, reason="another_recovery")
        return result

    monkeypatch.setattr(h.lifecycle, "_save_transition", save)
    h.runtime._sync_lifecycle()
    current = h.lifecycle.snapshot(stream)
    assert current.state is StreamLifecycleState.DEGRADED
    assert current.last_error_detail == "another_recovery"
    assert _ready(h) == [False, True]


@pytest.mark.parametrize(
    ("source", "target"), [("ready", "ready"), ("degraded", "ready"), ("failed", "connecting")]
)
def test_unchanged_state_still_exposes_invalid_transition(tmp_path, source, target):
    h = _harness(tmp_path)
    stream = h.streams[0]
    with SqliteUnitOfWork(tmp_path / "market.sqlite3") as u:
        u.save_stream_state(replace(u.get_stream_state(stream), state=StreamLifecycleState(source)))
        u.commit()
    snapshot = h.lifecycle.snapshot(stream)
    with pytest.raises(InvalidStreamTransition):
        h.lifecycle.transition_if_current(snapshot, StreamLifecycleState(target))
    assert h.lifecycle.snapshot(h.streams[0]) == snapshot
