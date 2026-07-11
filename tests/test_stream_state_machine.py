from __future__ import annotations

import pytest

from market_data_service.domain import (
    InstrumentKey,
    InvalidStreamTransition,
    StreamKey,
    StreamLifecycleState,
    StreamStateSnapshot,
    can_transition,
    project_stream_readiness,
    strict_aggregate_readiness,
    transition_stream_state,
)


def _snapshot(ticker: str, state: StreamLifecycleState) -> StreamStateSnapshot:
    return StreamStateSnapshot(
        stream=StreamKey(InstrumentKey(ticker), "1m"),
        state=state,
        state_changed_at_ms=100,
        updated_at_ms=100,
    )


def test_happy_path_reaches_ready_only_through_required_states() -> None:
    snapshot = _snapshot("BTCUSDT.P", StreamLifecycleState.UNINITIALIZED)
    for at_ms, state in enumerate(
        (
            StreamLifecycleState.BOOTSTRAPPING,
            StreamLifecycleState.AUDITING,
            StreamLifecycleState.CONNECTING,
            StreamLifecycleState.READY,
        ),
        start=101,
    ):
        snapshot = transition_stream_state(snapshot, state, changed_at_ms=at_ms)
    assert snapshot.is_ready


def test_repair_must_return_to_audit_before_connecting() -> None:
    snapshot = _snapshot("BTCUSDT.P", StreamLifecycleState.AUDITING)
    snapshot = transition_stream_state(snapshot, StreamLifecycleState.REPAIRING, changed_at_ms=101)
    with pytest.raises(InvalidStreamTransition):
        transition_stream_state(snapshot, StreamLifecycleState.CONNECTING, changed_at_ms=102)
    snapshot = transition_stream_state(snapshot, StreamLifecycleState.AUDITING, changed_at_ms=102)
    assert can_transition(snapshot.state, StreamLifecycleState.CONNECTING)


def test_uninitialized_and_bootstrapping_cannot_jump_to_ready() -> None:
    for state in (StreamLifecycleState.UNINITIALIZED, StreamLifecycleState.BOOTSTRAPPING):
        with pytest.raises(InvalidStreamTransition):
            transition_stream_state(
                _snapshot("BTCUSDT.P", state),
                StreamLifecycleState.READY,
                changed_at_ms=101,
            )


def test_degraded_error_is_cleared_after_recovery_transition() -> None:
    snapshot = _snapshot("BTCUSDT.P", StreamLifecycleState.READY)
    snapshot = transition_stream_state(
        snapshot,
        StreamLifecycleState.DEGRADED,
        changed_at_ms=101,
        error_code="ws_disconnected",
        error_detail="connection closed",
    )
    assert snapshot.last_error_code == "ws_disconnected"
    snapshot = transition_stream_state(snapshot, StreamLifecycleState.AUDITING, changed_at_ms=102)
    assert snapshot.last_error_code is None
    assert snapshot.last_error_detail is None


def test_failed_requires_explicit_recovery_transition() -> None:
    snapshot = _snapshot("BTCUSDT.P", StreamLifecycleState.FAILED)
    with pytest.raises(InvalidStreamTransition):
        transition_stream_state(snapshot, StreamLifecycleState.READY, changed_at_ms=101)
    recovered = transition_stream_state(
        snapshot,
        StreamLifecycleState.UNINITIALIZED,
        changed_at_ms=101,
    )
    assert recovered.state is StreamLifecycleState.UNINITIALIZED


def test_strict_multi_symbol_readiness_requires_every_stream_ready() -> None:
    btc = _snapshot("BTCUSDT.P", StreamLifecycleState.READY)
    eth = _snapshot("ETHUSDT.P", StreamLifecycleState.BOOTSTRAPPING)
    assert strict_aggregate_readiness((btc, eth)) is False
    assert strict_aggregate_readiness((btc,)) is True
    assert strict_aggregate_readiness(()) is False
    assert project_stream_readiness(eth).reason == "bootstrapping"


def test_timestamp_cannot_move_backwards() -> None:
    with pytest.raises(ValueError):
        transition_stream_state(
            _snapshot("BTCUSDT.P", StreamLifecycleState.READY),
            StreamLifecycleState.DEGRADED,
            changed_at_ms=99,
        )
