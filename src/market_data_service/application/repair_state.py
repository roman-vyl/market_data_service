"""Persist lifecycle and diagnostics for bounded gap repair."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from typing import Protocol

from market_data_service.application.backfill_errors import classify_backfill_failure
from market_data_service.domain.continuity import GapRange
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.stream_state import (
    InvalidStreamTransition,
    StreamLifecycleState,
    transition_stream_state,
)
from market_data_service.ports.storage import CanonicalStorageUnitOfWork


class Clock(Protocol):
    def now_ms(self) -> int: ...


class RepairStateRecorder:
    def __init__(
        self,
        unit_of_work_factory: Callable[[], CanonicalStorageUnitOfWork],
        clock: Clock,
    ) -> None:
        self._unit_of_work_factory = unit_of_work_factory
        self._clock = clock

    def ensure_auditing(self, stream: StreamKey) -> None:
        now_ms = self._clock.now_ms()
        with self._unit_of_work_factory() as unit_of_work:
            snapshot = unit_of_work.get_stream_state(stream)
            if snapshot.state is StreamLifecycleState.AUDITING:
                unit_of_work.commit()
                return
            if snapshot.state not in {
                StreamLifecycleState.REPAIRING,
                StreamLifecycleState.DEGRADED,
                StreamLifecycleState.FAILED,
            }:
                raise InvalidStreamTransition(
                    f"repair requires auditing state, got {snapshot.state.value}"
                )
            snapshot = transition_stream_state(
                snapshot,
                StreamLifecycleState.AUDITING,
                changed_at_ms=max(now_ms, snapshot.updated_at_ms),
            )
            unit_of_work.save_stream_state(snapshot)
            unit_of_work.commit()

    def transition_to_repairing(self, stream: StreamKey) -> None:
        now_ms = self._clock.now_ms()
        with self._unit_of_work_factory() as unit_of_work:
            snapshot = unit_of_work.get_stream_state(stream)
            snapshot = transition_stream_state(
                snapshot,
                StreamLifecycleState.REPAIRING,
                changed_at_ms=max(now_ms, snapshot.updated_at_ms),
            )
            unit_of_work.save_stream_state(snapshot)
            unit_of_work.commit()

    def transition_repairing_to_auditing(self, stream: StreamKey) -> None:
        now_ms = self._clock.now_ms()
        with self._unit_of_work_factory() as unit_of_work:
            snapshot = unit_of_work.get_stream_state(stream)
            if snapshot.state is StreamLifecycleState.REPAIRING:
                snapshot = transition_stream_state(
                    snapshot,
                    StreamLifecycleState.AUDITING,
                    changed_at_ms=max(now_ms, snapshot.updated_at_ms),
                )
                unit_of_work.save_stream_state(snapshot)
            unit_of_work.commit()

    def record_audit(self, stream: StreamKey) -> None:
        now_ms = self._clock.now_ms()
        with self._unit_of_work_factory() as unit_of_work:
            snapshot = unit_of_work.get_stream_state(stream)
            snapshot = replace(
                snapshot,
                last_audit_at_ms=now_ms,
                updated_at_ms=max(now_ms, snapshot.updated_at_ms),
            )
            unit_of_work.save_stream_state(snapshot)
            unit_of_work.commit()

    def record_rest_success(self, stream: StreamKey) -> None:
        now_ms = self._clock.now_ms()
        with self._unit_of_work_factory() as unit_of_work:
            snapshot = unit_of_work.get_stream_state(stream)
            snapshot = replace(
                snapshot,
                last_rest_success_at_ms=now_ms,
                last_error_code=None,
                last_error_detail=None,
                updated_at_ms=max(now_ms, snapshot.updated_at_ms),
            )
            unit_of_work.save_stream_state(snapshot)
            unit_of_work.commit()

    def record_failure(self, stream: StreamKey, exc: Exception) -> None:
        now_ms = self._clock.now_ms()
        decision = classify_backfill_failure(exc)
        with self._unit_of_work_factory() as unit_of_work:
            snapshot = unit_of_work.get_stream_state(stream)
            try:
                snapshot = transition_stream_state(
                    snapshot,
                    decision.target_state,
                    changed_at_ms=max(now_ms, snapshot.updated_at_ms),
                    error_code=decision.code,
                    error_detail=decision.detail,
                )
            except InvalidStreamTransition:
                snapshot = replace(
                    snapshot,
                    last_error_code=decision.code,
                    last_error_detail=decision.detail,
                    updated_at_ms=max(now_ms, snapshot.updated_at_ms),
                )
            unit_of_work.save_stream_state(snapshot)
            unit_of_work.commit()

    def record_unresolved_gaps(
        self,
        stream: StreamKey,
        gaps: tuple[GapRange, ...],
    ) -> None:
        now_ms = self._clock.now_ms()
        with self._unit_of_work_factory() as unit_of_work:
            for gap in gaps:
                unit_of_work.record_quarantine(
                    stream=stream,
                    start_ms=gap.start_open_time_ms,
                    end_ms=gap.end_open_time_ms,
                    reason_code="repair_incomplete_gap",
                    detail="post-repair audit still reports a gap",
                    payload_json=None,
                    created_at_ms=now_ms,
                )
            unit_of_work.commit()
