"""One-stream full-window historical reconciliation through existing repair."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from market_data_service.application.lower_bound import ResolveHistoricalLowerBound
from market_data_service.application.repair_gaps import RepairStreamGaps
from market_data_service.application.repair_types import RepairStatus, RepairStreamGapsRequest
from market_data_service.application.source_failure import classify_source_failure
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.timeframes import get_timeframe, last_closed_open_time_ms
from market_data_service.runtime.lifecycle import RuntimeLifecycleRecorder
from market_data_service.runtime.startup_types import (
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
            resolved_window = window or self._resolve_window(stream)
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
                )
            if result.status is RepairStatus.INCOMPLETE:
                return StartupStreamOutcome(
                    stream,
                    StartupClassification.INCOMPLETE,
                    audit=result.post_repair_audit,
                    window=resolved_window,
                    error_code="historical_reconciliation_incomplete",
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

    def _resolve_window(self, stream: StreamKey) -> ReconciliationWindow | None:
        lower = self.lower_bound.execute(
            stream,
            max_windows=self.discovery_windows_per_pass,
        )
        start_ms = lower.earliest_available_open_time_ms
        if not lower.resolved or start_ms is None:
            return None
        step_ms = get_timeframe(stream.timeframe).duration_ms
        target_open_ms = last_closed_open_time_ms(self.now_ms(), step_ms)
        end_ms = target_open_ms + step_ms
        if start_ms >= end_ms:
            return None
        return ReconciliationWindow(start_ms, end_ms)
