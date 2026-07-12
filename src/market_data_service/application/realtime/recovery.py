"""Bounded REST-authoritative recovery for one realtime stream."""

from __future__ import annotations

import asyncio
from collections.abc import Callable

from market_data_service.application.audit_continuity import (
    AuditStreamContinuityRequest,
)
from market_data_service.application.backfill_types import (
    BackfillStreamRequest,
    BackfillStreamResult,
)
from market_data_service.application.realtime.recovery_plan import RealtimeRecoveryPlanner
from market_data_service.application.realtime.recovery_state import (
    RealtimeRecoveryStateRecorder,
)
from market_data_service.application.realtime.recovery_types import (
    RealtimeRecoveryRequest,
    RealtimeRecoveryResult,
    RecoveryClassification,
    StreamAudit,
    StreamBackfill,
    StreamRepair,
)
from market_data_service.application.repair_types import (
    RepairStatus,
    RepairStreamGapsRequest,
)
from market_data_service.application.source_failure import classify_source_failure
from market_data_service.domain.continuity import ContinuityReport
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.timeframes import get_timeframe
from market_data_service.domain.windows import TimeWindow
from market_data_service.ports.storage import CanonicalStorageUnitOfWork


class RealtimeRecoveryCoordinator:
    """Serialize recovery per stream and compose existing historical workflows."""

    def __init__(
        self,
        *,
        backfill: StreamBackfill,
        auditor: StreamAudit,
        repair: StreamRepair,
        unit_of_work_factory: Callable[[], CanonicalStorageUnitOfWork],
        now_ms: Callable[[], int],
    ) -> None:
        self._backfill = backfill
        self._auditor = auditor
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
            recovery_window = self._planner.derive_window(request.signal)
            if recovery_window is None:
                return RealtimeRecoveryResult(
                    stream=stream,
                    classification=RecoveryClassification.INCOMPLETE,
                    recovery_window=None,
                    error_code="missing_durable_recovery_anchor",
                    error_detail="stream has no durable latest committed candle",
                )

            backfill_result = self._run_backfill(request, recovery_window)
            if backfill_result is not None and backfill_result.error_code is not None:
                classification = (
                    RecoveryClassification.RECOVERABLE_FAILURE
                    if backfill_result.failure_disposition == "recoverable"
                    else RecoveryClassification.FATAL_FAILURE
                )
                return RealtimeRecoveryResult(
                    stream=stream,
                    classification=classification,
                    recovery_window=recovery_window,
                    backfill=backfill_result,
                    error_code=backfill_result.error_code,
                    error_detail=backfill_result.error_detail,
                )
            if backfill_result is not None and not backfill_result.reached_end:
                return RealtimeRecoveryResult(
                    stream=stream,
                    classification=RecoveryClassification.INCOMPLETE,
                    recovery_window=recovery_window,
                    backfill=backfill_result,
                    error_code="backfill_budget_exhausted",
                    error_detail="bounded trailing backfill did not reach recovery end",
                )

            self._state.ensure_auditing(stream)
            audit = self._audit(stream, recovery_window)
            if audit.is_continuous:
                self._state.mark_restored(stream)
                return RealtimeRecoveryResult(
                    stream=stream,
                    classification=RecoveryClassification.RESTORED,
                    recovery_window=recovery_window,
                    backfill=backfill_result,
                    audit=audit,
                    post_audit=audit,
                    restored_through_open_time_ms=(
                        recovery_window.end_ms
                        - get_timeframe(stream.timeframe).duration_ms
                    ),
                )

            repair = self._repair.execute(
                RepairStreamGapsRequest(
                    stream=stream,
                    start_time_ms=recovery_window.start_ms,
                    end_time_ms=recovery_window.end_ms,
                    max_windows=request.max_repair_windows,
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
                    backfill=backfill_result,
                    audit=audit,
                    repair=repair,
                    error_code=repair.error_code,
                    error_detail=repair.error_detail,
                )

            post_audit = self._audit(stream, recovery_window)
            if not post_audit.is_continuous:
                return RealtimeRecoveryResult(
                    stream=stream,
                    classification=RecoveryClassification.INCOMPLETE,
                    recovery_window=recovery_window,
                    backfill=backfill_result,
                    audit=audit,
                    repair=repair,
                    post_audit=post_audit,
                    error_code="post_recovery_continuity_incomplete",
                    error_detail="post-recovery audit still reports gaps",
                )
            self._state.mark_restored(stream)
            return RealtimeRecoveryResult(
                stream=stream,
                classification=RecoveryClassification.RESTORED,
                recovery_window=recovery_window,
                backfill=backfill_result,
                audit=audit,
                repair=repair,
                post_audit=post_audit,
                restored_through_open_time_ms=(
                    recovery_window.end_ms
                    - get_timeframe(stream.timeframe).duration_ms
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

    def _run_backfill(
        self,
        request: RealtimeRecoveryRequest,
        window: TimeWindow,
    ) -> BackfillStreamResult | None:
        stream = request.signal.stream
        if not self._planner.needs_backfill(stream, window):
            return None
        return self._backfill.execute(
            BackfillStreamRequest(
                stream=stream,
                start_time_ms=window.start_ms,
                end_time_ms=window.end_ms,
                max_windows=request.max_backfill_windows,
                resume_from_latest_committed=False,
            )
        )

    def _audit(self, stream: StreamKey, window: TimeWindow) -> ContinuityReport:
        return self._auditor.execute(
            AuditStreamContinuityRequest(
                stream=stream,
                start_time_ms=window.start_ms,
                end_time_ms=window.end_ms,
            )
        )
