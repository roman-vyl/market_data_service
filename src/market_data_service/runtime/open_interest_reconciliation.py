"""One bounded pass of the open interest historical track for one stream.

Mirrors ``HistoricalStreamReconciler`` for the second track: own lower bound,
audit-driven repair over the full window, restart-safe preflight. It never
touches the stream lifecycle and never hands a stream to realtime; the
``CONNECTING`` classification here only means "this cycle is complete".
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

from market_data_service.application.lower_bound import ResolveHistoricalLowerBound
from market_data_service.application.open_interest_track_state import OpenInterestTrackState
from market_data_service.application.repair_gaps import RepairStreamGaps
from market_data_service.application.repair_types import (
    RepairStatus,
    RepairStreamGapsRequest,
    RepairStreamGapsResult,
)
from market_data_service.application.source_failure import classify_source_failure
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.timeframes import get_timeframe, last_closed_open_time_ms
from market_data_service.runtime.startup_types import (
    BoundedWorkCounts,
    LowerBoundProgressMarker,
    ReconciliationWindow,
    StartupClassification,
    StartupStreamOutcome,
)

_LOGGER = logging.getLogger("market_data_service.runtime.oi_track")


@dataclass(slots=True)
class OpenInterestTrackReconciler:
    lower_bound: ResolveHistoricalLowerBound
    repair: RepairStreamGaps
    state: OpenInterestTrackState
    now_ms: Callable[[], int]
    discovery_windows_per_pass: int
    repair_windows_per_pass: int

    def execute_stream(
        self,
        stream: StreamKey,
        window: ReconciliationWindow | None = None,
    ) -> StartupStreamOutcome:
        try:
            resolved = window
            if resolved is None:
                lower = self.lower_bound.execute(
                    stream, max_windows=self.discovery_windows_per_pass
                )
                start_ms = lower.earliest_available_open_time_ms
                if not lower.resolved or start_ms is None:
                    marker = (
                        None
                        if lower.next_search_start_time_ms is None
                        else LowerBoundProgressMarker(lower.next_search_start_time_ms)
                    )
                    return StartupStreamOutcome(
                        stream, StartupClassification.INCOMPLETE, progress_marker=marker
                    )
                resolved = self._window(stream, start_ms)
                if resolved is None:
                    return StartupStreamOutcome(stream, StartupClassification.CONNECTING)
            result = self.repair.execute(
                RepairStreamGapsRequest(
                    stream=stream,
                    start_time_ms=resolved.start_time_ms,
                    end_time_ms=resolved.end_time_ms,
                    max_windows=self.repair_windows_per_pass,
                )
            )
            return self._outcome(stream, resolved, result)
        except Exception as exc:
            decision = classify_source_failure(exc)
            self._record_failure(stream, exc)
            classification = (
                StartupClassification.RECOVERABLE_FAILURE
                if decision.disposition.value == "recoverable"
                else StartupClassification.FATAL_FAILURE
            )
            return StartupStreamOutcome(
                stream,
                classification,
                window=window,
                error_code=type(exc).__name__,
                error_detail=str(exc),
            )

    def _outcome(
        self,
        stream: StreamKey,
        window: ReconciliationWindow,
        result: RepairStreamGapsResult,
    ) -> StartupStreamOutcome:
        counts = _counts(result)
        if result.status is RepairStatus.COMPLETE:
            audit = result.post_repair_audit
            gaps = 0 if audit is None else len(audit.gaps)
            self.state.record_audit_result(stream, gaps)
            step_ms = get_timeframe(stream.timeframe).duration_ms
            progress = self.state.progress(stream)
            _LOGGER.info(
                "oi track complete stream=%s lower_bound=%s latest=%s expected_points=%s "
                "stored_points=%s gaps=%s",
                stream.canonical_id,
                progress.earliest_available_open_time_ms,
                progress.latest_committed_open_time_ms,
                (window.end_time_ms - window.start_time_ms) // step_ms,
                None if audit is None else audit.candle_count,
                gaps,
            )
            return StartupStreamOutcome(
                stream,
                StartupClassification.CONNECTING,
                audit=audit,
                window=window,
                counts=counts,
            )
        if result.status is RepairStatus.INCOMPLETE:
            return StartupStreamOutcome(
                stream,
                StartupClassification.INCOMPLETE,
                audit=result.post_repair_audit,
                window=window,
                error_code="oi_track_incomplete",
                progress_marker=result.progress_marker,
                counts=counts,
            )
        classification = (
            StartupClassification.RECOVERABLE_FAILURE
            if result.failure_disposition == "recoverable"
            else StartupClassification.FATAL_FAILURE
        )
        return StartupStreamOutcome(
            stream,
            classification,
            audit=result.pre_repair_audit,
            window=window,
            error_code=result.error_code,
            error_detail=result.error_detail,
            counts=counts,
        )

    def _window(self, stream: StreamKey, start_ms: int) -> ReconciliationWindow | None:
        step_ms = get_timeframe(stream.timeframe).duration_ms
        end_ms = last_closed_open_time_ms(self.now_ms(), step_ms) + step_ms
        return None if start_ms >= end_ms else ReconciliationWindow(start_ms, end_ms)

    def _record_failure(self, stream: StreamKey, exc: Exception) -> None:
        try:
            self.state.record_failure(stream, exc)
        except Exception:
            _LOGGER.exception("oi track could not record failure stream=%s", stream.canonical_id)


def _counts(result: RepairStreamGapsResult) -> BoundedWorkCounts:
    items = result.window_results
    return BoundedWorkCounts(
        attempted_windows=result.attempted_windows,
        completed_windows=result.completed_windows,
        committed=sum(item.committed for item in items),
        duplicates=sum(item.duplicates for item in items),
        corrected=sum(item.corrected for item in items),
        rejected=sum(item.rejected for item in items),
        unexpected=sum(item.unexpected for item in items),
    )
