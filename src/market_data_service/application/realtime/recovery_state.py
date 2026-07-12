"""Persist lifecycle transitions around realtime historical recovery."""

from __future__ import annotations

from collections.abc import Callable

from market_data_service.domain.identity import StreamKey
from market_data_service.domain.stream_state import (
    InvalidStreamTransition,
    StreamLifecycleState,
    transition_stream_state,
)
from market_data_service.ports.storage import CanonicalStorageUnitOfWork


class RealtimeRecoveryStateRecorder:
    def __init__(
        self,
        unit_of_work_factory: Callable[[], CanonicalStorageUnitOfWork],
        now_ms: Callable[[], int],
    ) -> None:
        self._unit_of_work_factory = unit_of_work_factory
        self._now_ms = now_ms

    def mark_unavailable(self, stream: StreamKey, *, reason: str) -> None:
        with self._unit_of_work_factory() as unit_of_work:
            snapshot = unit_of_work.get_stream_state(stream)
            if snapshot.state in {
                StreamLifecycleState.READY,
                StreamLifecycleState.CONNECTING,
            }:
                snapshot = transition_stream_state(
                    snapshot,
                    StreamLifecycleState.DEGRADED,
                    changed_at_ms=max(self._now_ms(), snapshot.updated_at_ms),
                    error_code="realtime_recovery_required",
                    error_detail=reason,
                )
                unit_of_work.save_stream_state(snapshot)
            unit_of_work.commit()

    def ensure_auditing(self, stream: StreamKey) -> None:
        with self._unit_of_work_factory() as unit_of_work:
            snapshot = unit_of_work.get_stream_state(stream)
            if snapshot.state is StreamLifecycleState.AUDITING:
                unit_of_work.commit()
                return
            if snapshot.state is StreamLifecycleState.DEGRADED:
                snapshot = transition_stream_state(
                    snapshot,
                    StreamLifecycleState.AUDITING,
                    changed_at_ms=max(self._now_ms(), snapshot.updated_at_ms),
                )
                unit_of_work.save_stream_state(snapshot)
                unit_of_work.commit()
                return
            if snapshot.state is StreamLifecycleState.REPAIRING:
                snapshot = transition_stream_state(
                    snapshot,
                    StreamLifecycleState.AUDITING,
                    changed_at_ms=max(self._now_ms(), snapshot.updated_at_ms),
                )
                unit_of_work.save_stream_state(snapshot)
                unit_of_work.commit()
                return
            raise InvalidStreamTransition(
                f"realtime recovery requires degraded/auditing/repairing state, "
                f"got {snapshot.state.value}"
            )

    def mark_restored(self, stream: StreamKey) -> None:
        with self._unit_of_work_factory() as unit_of_work:
            snapshot = unit_of_work.get_stream_state(stream)
            if snapshot.state is StreamLifecycleState.AUDITING:
                snapshot = transition_stream_state(
                    snapshot,
                    StreamLifecycleState.CONNECTING,
                    changed_at_ms=max(self._now_ms(), snapshot.updated_at_ms),
                )
                unit_of_work.save_stream_state(snapshot)
            unit_of_work.commit()
