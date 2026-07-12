"""Application workflow for bounded production gap repair."""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

from market_data_service.application.audit_continuity import (
    AuditStreamContinuity,
    AuditStreamContinuityRequest,
)
from market_data_service.application.import_window import ImportHistoricalWindow
from market_data_service.application.repair_state import RepairStateRecorder
from market_data_service.application.repair_types import (
    RepairStatus,
    RepairStreamGapsRequest,
    RepairStreamGapsResult,
    RepairWindowResult,
)
from market_data_service.domain.continuity import ContinuityReport, GapRange
from market_data_service.domain.gaps import Gap, iter_fetch_windows
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.timeframes import get_timeframe
from market_data_service.domain.windows import TimeWindow
from market_data_service.ports.storage import CanonicalStorageUnitOfWork


class Clock(Protocol):
    def now_ms(self) -> int: ...


class RepairStreamGaps:
    """Repair one stream/range through REST-backed canonical ingestion."""

    def __init__(
        self,
        auditor: AuditStreamContinuity,
        window_importer: ImportHistoricalWindow,
        unit_of_work_factory: Callable[[], CanonicalStorageUnitOfWork],
        clock: Clock,
        *,
        max_candles_per_window: int = 1000,
    ) -> None:
        self._auditor = auditor
        self._window_importer = window_importer
        self._state = RepairStateRecorder(unit_of_work_factory, clock)
        self._max_candles_per_window = max_candles_per_window

    def execute(self, request: RepairStreamGapsRequest) -> RepairStreamGapsResult:
        requested_window = TimeWindow(request.start_time_ms, request.end_time_ms)
        self._state.ensure_auditing(request.stream)
        pre_repair_audit = self._audit(request)
        self._state.record_audit(request.stream)
        if pre_repair_audit.is_continuous:
            return RepairStreamGapsResult(
                stream=request.stream,
                requested_window=requested_window,
                status=RepairStatus.COMPLETE,
                pre_repair_audit=pre_repair_audit,
                post_repair_audit=pre_repair_audit,
                attempted_windows=0,
                completed_windows=0,
                window_results=(),
            )

        self._state.transition_to_repairing(request.stream)
        window_results: list[RepairWindowResult] = []
        attempted_windows = 0
        try:
            for window in self._planned_windows(request.stream, pre_repair_audit.gaps):
                if attempted_windows >= request.max_windows:
                    break
                attempted_windows += 1
                imported = self._window_importer.execute(request.stream, window)
                window_results.append(
                    RepairWindowResult(
                        window=window,
                        observed=imported.observed,
                        committed=imported.committed,
                        duplicates=imported.duplicates,
                        corrected=imported.corrected,
                        rejected=imported.rejected,
                        unexpected=imported.unexpected,
                    )
                )
                self._state.record_rest_success(request.stream)
        except Exception as exc:
            self._state.record_failure(request.stream, exc)
            return RepairStreamGapsResult(
                stream=request.stream,
                requested_window=requested_window,
                status=RepairStatus.FAILED,
                pre_repair_audit=pre_repair_audit,
                post_repair_audit=None,
                attempted_windows=attempted_windows,
                completed_windows=len(window_results),
                window_results=tuple(window_results),
                error_code=type(exc).__name__,
                error_detail=str(exc),
            )

        self._state.transition_repairing_to_auditing(request.stream)
        post_repair_audit = self._audit(request)
        self._state.record_audit(request.stream)
        if post_repair_audit.is_continuous:
            status = RepairStatus.COMPLETE
        else:
            status = RepairStatus.INCOMPLETE
            self._state.record_unresolved_gaps(request.stream, post_repair_audit.gaps)

        return RepairStreamGapsResult(
            stream=request.stream,
            requested_window=requested_window,
            status=status,
            pre_repair_audit=pre_repair_audit,
            post_repair_audit=post_repair_audit,
            attempted_windows=attempted_windows,
            completed_windows=len(window_results),
            window_results=tuple(window_results),
        )

    def _audit(self, request: RepairStreamGapsRequest) -> ContinuityReport:
        return self._auditor.execute(
            AuditStreamContinuityRequest(
                stream=request.stream,
                start_time_ms=request.start_time_ms,
                end_time_ms=request.end_time_ms,
            )
        )

    def _planned_windows(
        self,
        stream: StreamKey,
        gaps: tuple[GapRange, ...],
    ) -> tuple[TimeWindow, ...]:
        step_ms = get_timeframe(stream.timeframe).duration_ms
        windows: list[TimeWindow] = []
        for gap in gaps:
            windows.extend(
                iter_fetch_windows(
                    Gap(gap.start_open_time_ms, gap.end_open_time_ms),
                    step_ms=step_ms,
                    max_candles=self._max_candles_per_window,
                )
            )
        return tuple(windows)
