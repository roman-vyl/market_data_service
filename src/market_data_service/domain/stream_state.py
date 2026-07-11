"""Per-stream persisted lifecycle state and transition rules."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum

from market_data_service.domain.identity import StreamKey


class StreamLifecycleState(StrEnum):
    """Persisted lifecycle of one canonical candle stream."""

    UNINITIALIZED = "uninitialized"
    BOOTSTRAPPING = "bootstrapping"
    AUDITING = "auditing"
    REPAIRING = "repairing"
    CONNECTING = "connecting"
    READY = "ready"
    DEGRADED = "degraded"
    FAILED = "failed"


_ALLOWED_TRANSITIONS: dict[StreamLifecycleState, frozenset[StreamLifecycleState]] = {
    StreamLifecycleState.UNINITIALIZED: frozenset(
        {StreamLifecycleState.BOOTSTRAPPING, StreamLifecycleState.FAILED}
    ),
    StreamLifecycleState.BOOTSTRAPPING: frozenset(
        {StreamLifecycleState.AUDITING, StreamLifecycleState.DEGRADED, StreamLifecycleState.FAILED}
    ),
    StreamLifecycleState.AUDITING: frozenset(
        {
            StreamLifecycleState.REPAIRING,
            StreamLifecycleState.CONNECTING,
            StreamLifecycleState.DEGRADED,
            StreamLifecycleState.FAILED,
        }
    ),
    StreamLifecycleState.REPAIRING: frozenset(
        {StreamLifecycleState.AUDITING, StreamLifecycleState.DEGRADED, StreamLifecycleState.FAILED}
    ),
    StreamLifecycleState.CONNECTING: frozenset(
        {StreamLifecycleState.READY, StreamLifecycleState.DEGRADED, StreamLifecycleState.FAILED}
    ),
    StreamLifecycleState.READY: frozenset(
        {StreamLifecycleState.DEGRADED, StreamLifecycleState.FAILED}
    ),
    StreamLifecycleState.DEGRADED: frozenset(
        {
            StreamLifecycleState.BOOTSTRAPPING,
            StreamLifecycleState.AUDITING,
            StreamLifecycleState.CONNECTING,
            StreamLifecycleState.FAILED,
        }
    ),
    StreamLifecycleState.FAILED: frozenset(
        {StreamLifecycleState.UNINITIALIZED, StreamLifecycleState.AUDITING}
    ),
}


class InvalidStreamTransition(ValueError):
    """Raised when application code attempts an illegal lifecycle transition."""


@dataclass(frozen=True, slots=True)
class StreamStateSnapshot:
    """Current durable operational snapshot for one stream."""

    stream: StreamKey
    state: StreamLifecycleState
    earliest_available_open_time_ms: int | None = None
    latest_committed_open_time_ms: int | None = None
    last_audit_at_ms: int | None = None
    last_rest_success_at_ms: int | None = None
    last_ws_message_at_ms: int | None = None
    last_error_code: str | None = None
    last_error_detail: str | None = None
    state_changed_at_ms: int = 0
    updated_at_ms: int = 0

    def __post_init__(self) -> None:
        for field_name in (
            "earliest_available_open_time_ms",
            "latest_committed_open_time_ms",
            "last_audit_at_ms",
            "last_rest_success_at_ms",
            "last_ws_message_at_ms",
            "state_changed_at_ms",
            "updated_at_ms",
        ):
            value = getattr(self, field_name)
            if value is not None and value < 0:
                raise ValueError(f"{field_name} must be non-negative")

    @property
    def is_ready(self) -> bool:
        return self.state is StreamLifecycleState.READY


def can_transition(
    current: StreamLifecycleState,
    target: StreamLifecycleState,
) -> bool:
    """Return whether a state change is allowed by the v1 lifecycle contract."""

    return target in _ALLOWED_TRANSITIONS[current]


def transition_stream_state(
    snapshot: StreamStateSnapshot,
    target: StreamLifecycleState,
    *,
    changed_at_ms: int,
    error_code: str | None = None,
    error_detail: str | None = None,
) -> StreamStateSnapshot:
    """Create a new snapshot after validating one lifecycle transition."""

    if target is snapshot.state:
        raise InvalidStreamTransition(f"stream is already in state {target.value}")
    if not can_transition(snapshot.state, target):
        raise InvalidStreamTransition(
            f"illegal stream transition {snapshot.state.value} -> {target.value}"
        )
    if changed_at_ms < snapshot.updated_at_ms:
        raise ValueError("changed_at_ms must not move backwards")

    clear_error = target not in {StreamLifecycleState.DEGRADED, StreamLifecycleState.FAILED}
    return replace(
        snapshot,
        state=target,
        last_error_code=None if clear_error else error_code,
        last_error_detail=None if clear_error else error_detail,
        state_changed_at_ms=changed_at_ms,
        updated_at_ms=changed_at_ms,
    )
