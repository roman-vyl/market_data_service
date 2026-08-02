"""One-stream full-window historical reconciliation through existing repair."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from market_data_service.application.lower_bound import ResolveHistoricalLowerBound
from market_data_service.application.repair_gaps import RepairStreamGaps
from market_data_service.application.repair_types import (
    RepairStatus,
    RepairStreamGapsRequest,
    RepairStreamGapsResult,
)
from market_data_service.application.source_failure import classify_source_failure
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.timeframes import get_timeframe, last_closed_open_time_ms
from market_data_service.runtime.lifecycle import RuntimeLifecycleRecorder
from market_data_service.runtime.startup_types import (
    BoundedWorkCounts,
    LowerBoundProgressMarker,
    ReconciliationWindow,
    StartupClassification,
    StartupStreamOutcome,
)


@dataclass(slots=True)
class HistoricalStreamReconciler:
    lower_bound: ResolveHistoricalLowerBound
    repair: RepairStreamGaps
    lifecycle: RuntimeLifecycleRecorder
    now_ms: Callable[[], int]
    discovery_windows_per_pass: int
    repair_windows_per_pass: int

    def execute(
        self,
        stream: StreamKey,
        window: ReconciliationWindow | None = None,
    ) -> StartupStreamOutcome:
        try:
            self.lifecycle.prepare_for_bootstrap(stream)
            resolved_window = window
            if resolved_window is None:
                lower = self.lower_bound.execute(
                    stream,
                    max_windows=self.discovery_windows_per_pass,
                )
                start_ms = lower.earliest_available_open_time_ms
                if not lower.resolved or start_ms is None:
                    marker = (
                        None
                        if lower.next_search_start_time_ms is None
                        else LowerBoundProgressMarker(lower.next_search_start_time_ms)
                    )
                    return StartupStreamOutcome(
                        stream,
                        StartupClassification.INCOMPLETE,
                        progress_marker=marker,
                    )
                resolved_window = self._window_from_lower_bound(stream, start_ms)
                if resolved_window is None:
                    return StartupStreamOutcome(stream, StartupClassification.INCOMPLETE)
            self.lifecycle.mark_auditing(stream)
            result = self.repair.execute(
                RepairStreamGapsRequest(
                    stream=stream,
                    start_time_ms=resolved_window.start_time_ms,
                    end_time_ms=resolved_window.end_time_ms,
                    max_windows=self.repair_windows_per_pass,
                )
            )
            if result.status is RepairStatus.COMPLETE:
                self.lifecycle.mark_connecting(stream)
                return StartupStreamOutcome(
                    stream,
                    StartupClassification.CONNECTING,
                    audit=result.post_repair_audit,
                    window=resolved_window,
                    counts=self._counts(result),
                )
            if result.status is RepairStatus.INCOMPLETE:
                return StartupStreamOutcome(
                    stream,
                    StartupClassification.INCOMPLETE,
                    audit=result.post_repair_audit,
                    window=resolved_window,
                    error_code="historical_reconciliation_incomplete",
                    progress_marker=result.progress_marker,
                    counts=self._counts(result),
                )
            classification = (
                StartupClassification.RECOVERABLE_FAILURE
                if result.failure_disposition == "recoverable"
                else StartupClassification.FATAL_FAILURE
            )
            self._record_failure_state(stream, classification, result.error_code or "repair_failed")
            return StartupStreamOutcome(
                stream,
                classification,
                audit=result.pre_repair_audit,
                window=resolved_window,
                error_code=result.error_code,
                error_detail=result.error_detail,
                counts=self._counts(result),
            )
        except Exception as exc:
            decision = classify_source_failure(exc)
            classification = (
                StartupClassification.RECOVERABLE_FAILURE
                if decision.disposition.value == "recoverable"
                else StartupClassification.FATAL_FAILURE
            )
            self._record_failure_state(stream, classification, decision.code)
            return StartupStreamOutcome(
                stream,
                classification,
                window=window,
                error_code=type(exc).__name__,
                error_detail=str(exc),
            )

    def _record_failure_state(
        self,
        stream: StreamKey,
        classification: StartupClassification,
        reason: str,
    ) -> None:
        if classification is StartupClassification.RECOVERABLE_FAILURE:
            self.lifecycle.mark_degraded(stream, reason=reason)
        else:
            self.lifecycle.mark_failed(stream, reason=reason)

    def _window_from_lower_bound(
        self,
        stream: StreamKey,
        start_ms: int,
    ) -> ReconciliationWindow | None:
        step_ms = get_timeframe(stream.timeframe).duration_ms
        target_open_ms = last_closed_open_time_ms(self.now_ms(), step_ms)
        end_ms = target_open_ms + step_ms
        if start_ms >= end_ms:
            return None
        return ReconciliationWindow(start_ms, end_ms)

    @staticmethod
    def _counts(result: RepairStreamGapsResult) -> BoundedWorkCounts:
        window_results = result.window_results
        return BoundedWorkCounts(
            attempted_windows=result.attempted_windows,
            completed_windows=result.completed_windows,
            committed=sum(item.committed for item in window_results),
            duplicates=sum(item.duplicates for item in window_results),
            corrected=sum(item.corrected for item in window_results),
            rejected=sum(item.rejected for item in window_results),
            unexpected=sum(item.unexpected for item in window_results),
        )
