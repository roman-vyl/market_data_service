"""Persist runtime-owned lifecycle transitions through legal state paths."""

from __future__ import annotations

from collections.abc import Callable

from market_data_service.domain.identity import StreamKey
from market_data_service.domain.stream_state import (
    StreamLifecycleState,
    StreamStateSnapshot,
    transition_stream_state,
)
from market_data_service.ports.storage import CanonicalStorageUnitOfWork


class RuntimeLifecycleRecorder:
    def __init__(
        self,
        unit_of_work_factory: Callable[[], CanonicalStorageUnitOfWork],
        now_ms: Callable[[], int],
    ) -> None:
        self._unit_of_work_factory = unit_of_work_factory
        self._now_ms = now_ms

    def snapshot(self, stream: StreamKey) -> StreamStateSnapshot:
        with self._unit_of_work_factory() as unit_of_work:
            return unit_of_work.get_stream_state(stream)

    def prepare_for_bootstrap(self, stream: StreamKey) -> StreamStateSnapshot:
        snapshot = self.snapshot(stream)
        if snapshot.state is StreamLifecycleState.BOOTSTRAPPING:
            return snapshot
        if snapshot.state is StreamLifecycleState.FAILED:
            snapshot = self._transition(stream, StreamLifecycleState.UNINITIALIZED)
        elif snapshot.state not in {
            StreamLifecycleState.UNINITIALIZED,
            StreamLifecycleState.DEGRADED,
        }:
            snapshot = self._transition(
                stream,
                StreamLifecycleState.DEGRADED,
                error_code="startup_reconciliation_required",
                error_detail="persisted state must be reconciled after process start",
            )
        if snapshot.state is not StreamLifecycleState.BOOTSTRAPPING:
            snapshot = self._transition(stream, StreamLifecycleState.BOOTSTRAPPING)
        return snapshot

    def mark_auditing(self, stream: StreamKey) -> StreamStateSnapshot:
        snapshot = self.snapshot(stream)
        if snapshot.state is StreamLifecycleState.AUDITING:
            return snapshot
        if snapshot.state is StreamLifecycleState.REPAIRING:
            return self._transition(stream, StreamLifecycleState.AUDITING)
        if snapshot.state is StreamLifecycleState.FAILED:
            snapshot = self._transition(stream, StreamLifecycleState.AUDITING)
            return snapshot
        if snapshot.state is StreamLifecycleState.DEGRADED:
            return self._transition(stream, StreamLifecycleState.AUDITING)
        if snapshot.state is StreamLifecycleState.BOOTSTRAPPING:
            return self._transition(stream, StreamLifecycleState.AUDITING)
        raise ValueError(f"cannot enter auditing from {snapshot.state.value}")

    def mark_connecting(self, stream: StreamKey) -> StreamStateSnapshot:
        snapshot = self.snapshot(stream)
        if snapshot.state is StreamLifecycleState.CONNECTING:
            return snapshot
        return self._transition(stream, StreamLifecycleState.CONNECTING)

    def mark_ready(self, stream: StreamKey) -> StreamStateSnapshot:
        snapshot = self.snapshot(stream)
        if snapshot.state is StreamLifecycleState.READY:
            return snapshot
        return self._transition(stream, StreamLifecycleState.READY)

    def mark_degraded(self, stream: StreamKey, *, reason: str) -> StreamStateSnapshot:
        snapshot = self.snapshot(stream)
        if snapshot.state is StreamLifecycleState.DEGRADED:
            return snapshot
        return self._transition(
            stream,
            StreamLifecycleState.DEGRADED,
            error_code="runtime_degraded",
            error_detail=reason,
        )

    def mark_failed(self, stream: StreamKey, *, reason: str) -> StreamStateSnapshot:
        snapshot = self.snapshot(stream)
        if snapshot.state is StreamLifecycleState.FAILED:
            return snapshot
        return self._transition(
            stream,
            StreamLifecycleState.FAILED,
            error_code="runtime_failed",
            error_detail=reason,
        )

    def _transition(
        self,
        stream: StreamKey,
        target: StreamLifecycleState,
        *,
        error_code: str | None = None,
        error_detail: str | None = None,
    ) -> StreamStateSnapshot:
        with self._unit_of_work_factory() as unit_of_work:
            snapshot = unit_of_work.get_stream_state(stream)
            snapshot = transition_stream_state(
                snapshot,
                target,
                changed_at_ms=max(self._now_ms(), snapshot.updated_at_ms),
                error_code=error_code,
                error_detail=error_detail,
            )
            unit_of_work.save_stream_state(snapshot)
            unit_of_work.commit()
            return snapshot
