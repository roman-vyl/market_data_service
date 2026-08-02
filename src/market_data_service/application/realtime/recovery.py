"""Bounded REST-authoritative recovery for one realtime stream."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from market_data_service.application.realtime.recovery_plan import RealtimeRecoveryPlanner
from market_data_service.application.realtime.recovery_state import (
    RealtimeRecoveryStateRecorder,
)
from market_data_service.application.realtime.recovery_types import (
    RealtimeRecoveryRequest,
    RealtimeRecoveryResult,
    RecoveryClassification,
    StreamRepair,
)
from market_data_service.application.repair_types import (
    RepairStatus,
    RepairStreamGapsRequest,
)
from market_data_service.application.source_failure import classify_source_failure
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.timeframes import align_to_grid, get_timeframe
from market_data_service.domain.windows import TimeWindow
from market_data_service.ports.storage import CanonicalStorageUnitOfWork


class RealtimeRecoveryCoordinator:
    """Serialize recovery per stream and compose existing historical workflows."""

    def __init__(
        self,
        *,
        repair: StreamRepair,
        unit_of_work_factory: Callable[[], CanonicalStorageUnitOfWork],
        now_ms: Callable[[], int],
    ) -> None:
        self._repair = repair
        self._unit_of_work_factory = unit_of_work_factory
        self._now_ms = now_ms
        self._state = RealtimeRecoveryStateRecorder(unit_of_work_factory, now_ms)
        self._planner = RealtimeRecoveryPlanner(unit_of_work_factory, now_ms)
        self._locks: dict[StreamKey, asyncio.Lock] = {}

    async def execute(self, request: RealtimeRecoveryRequest) -> RealtimeRecoveryResult:
        stream = request.signal.stream
        lock = self._locks.setdefault(stream, asyncio.Lock())
        async with lock:
            return await asyncio.to_thread(self._execute_sync, request)

    def _execute_sync(self, request: RealtimeRecoveryRequest) -> RealtimeRecoveryResult:
        stream = request.signal.stream
        self._state.mark_unavailable(stream, reason=request.signal.reason.value)
        try:
            recovery_window = request.recovery_window or self._planner.derive_window(request.signal)
            if recovery_window is None:
                return RealtimeRecoveryResult(
                    stream=stream,
                    classification=RecoveryClassification.INCOMPLETE,
                    recovery_window=None,
                    error_code="missing_durable_recovery_anchor",
                    error_detail="stream has no durable latest committed candle",
                )

            self._state.ensure_auditing(stream)
            repair = self._repair.execute(
                RepairStreamGapsRequest(
                    stream=stream,
                    start_time_ms=recovery_window.start_ms,
                    end_time_ms=recovery_window.end_ms,
                    max_windows=request.max_windows,
                )
            )
            if repair.status is RepairStatus.FAILED:
                disposition = repair.failure_disposition or "fatal"
                classification = (
                    RecoveryClassification.RECOVERABLE_FAILURE
                    if disposition == "recoverable"
                    else RecoveryClassification.FATAL_FAILURE
                )
                return RealtimeRecoveryResult(
                    stream=stream,
                    classification=classification,
                    recovery_window=recovery_window,
                    audit=repair.pre_repair_audit,
                    repair=repair,
                    error_code=repair.error_code,
                    error_detail=repair.error_detail,
                )

            post_audit = repair.post_repair_audit
            if repair.status is RepairStatus.INCOMPLETE or not (
                post_audit is not None and post_audit.is_continuous
            ):
                return RealtimeRecoveryResult(
                    stream=stream,
                    classification=RecoveryClassification.INCOMPLETE,
                    recovery_window=recovery_window,
                    audit=repair.pre_repair_audit,
                    repair=repair,
                    post_audit=post_audit,
                    error_code="post_recovery_continuity_incomplete",
                    error_detail="post-recovery audit still reports gaps",
                )
            step_ms = get_timeframe(stream.timeframe).duration_ms
            current_end_ms = align_to_grid(self._now_ms(), step_ms)
            if current_end_ms > recovery_window.end_ms:
                return RealtimeRecoveryResult(
                    stream=stream,
                    classification=RecoveryClassification.INCOMPLETE,
                    recovery_window=recovery_window,
                    audit=repair.pre_repair_audit,
                    repair=repair,
                    post_audit=post_audit,
                    next_recovery_window=TimeWindow(recovery_window.end_ms, current_end_ms),
                    error_code="recovery_target_advanced",
                    error_detail="latest closed boundary advanced during recovery",
                )
            self._state.mark_restored(stream)
            return RealtimeRecoveryResult(
                stream=stream,
                classification=RecoveryClassification.RESTORED,
                recovery_window=recovery_window,
                audit=repair.pre_repair_audit,
                repair=repair,
                post_audit=post_audit,
                restored_through_open_time_ms=(
                    recovery_window.end_ms - step_ms
                ),
            )
        except Exception as exc:
            decision = classify_source_failure(exc)
            classification = (
                RecoveryClassification.RECOVERABLE_FAILURE
                if decision.disposition.value == "recoverable"
                else RecoveryClassification.FATAL_FAILURE
            )
            return RealtimeRecoveryResult(
                stream=stream,
                classification=classification,
                recovery_window=None,
                error_code=decision.code,
                error_detail=decision.detail,
            )
