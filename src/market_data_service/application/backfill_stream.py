"""Application workflow for bounded backfill of one stream."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace

from market_data_service.application.backfill_errors import classify_backfill_failure
from market_data_service.application.backfill_types import (
    BackfillStreamRequest,
    BackfillStreamResult,
    BackfillWindowResult,
    Clock,
)
from market_data_service.application.import_window import ImportHistoricalWindow
from market_data_service.domain.gaps import Gap, iter_fetch_windows
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.stream_state import (
    InvalidStreamTransition,
    StreamLifecycleState,
    transition_stream_state,
)
from market_data_service.domain.timeframes import get_timeframe
from market_data_service.domain.windows import TimeWindow
from market_data_service.ports.storage import CanonicalStorageUnitOfWork


class BackfillStreamHistory:
    """Sequential bounded history loading for one stream."""

    def __init__(
        self,
        window_importer: ImportHistoricalWindow,
        unit_of_work_factory: Callable[[], CanonicalStorageUnitOfWork],
        clock: Clock,
        *,
        max_candles_per_window: int = 1000,
    ) -> None:
        self._window_importer = window_importer
        self._unit_of_work_factory = unit_of_work_factory
        self._clock = clock
        self._max_candles_per_window = max_candles_per_window

    def execute(self, request: BackfillStreamRequest) -> BackfillStreamResult:
        self._ensure_bootstrapping(request.stream)
        cursor = self._resume_start(request)
        requested_window = TimeWindow(request.start_time_ms, request.end_time_ms)
        if cursor >= request.end_time_ms:
            self._transition_to_auditing(request.stream)
            return BackfillStreamResult(
                stream=request.stream,
                requested_window=requested_window,
                attempted_windows=0,
                completed_windows=0,
                reached_end=True,
                next_start_time_ms=request.end_time_ms,
                window_results=(),
            )

        windows = iter_fetch_windows(
            Gap(cursor, request.end_time_ms),
            step_ms=get_timeframe(request.stream.timeframe).duration_ms,
            max_candles=self._max_candles_per_window,
        )[: request.max_windows]

        results: list[BackfillWindowResult] = []
        for window in windows:
            try:
                imported = self._window_importer.execute(request.stream, window)
            except Exception as exc:
                self._record_failure(request.stream, exc)
                return BackfillStreamResult(
                    stream=request.stream,
                    requested_window=requested_window,
                    attempted_windows=len(results) + 1,
                    completed_windows=len(results),
                    reached_end=False,
                    next_start_time_ms=window.start_ms,
                    window_results=tuple(results),
                    error_code=type(exc).__name__,
                    error_detail=str(exc),
                )
            self._record_window_success(request.stream, window.start_ms)
            results.append(
                BackfillWindowResult(
                    window=window,
                    observed=imported.observed,
                    committed=imported.committed,
                    duplicates=imported.duplicates,
                    corrected=imported.corrected,
                    rejected=imported.rejected,
                )
            )

        next_start_time_ms = self._resume_start(request)
        reached_end = next_start_time_ms >= request.end_time_ms
        if reached_end:
            self._transition_to_auditing(request.stream)

        return BackfillStreamResult(
            stream=request.stream,
            requested_window=requested_window,
            attempted_windows=len(results),
            completed_windows=len(results),
            reached_end=reached_end,
            next_start_time_ms=min(next_start_time_ms, request.end_time_ms),
            window_results=tuple(results),
        )

    def _ensure_bootstrapping(self, stream: StreamKey) -> None:
        now_ms = self._clock.now_ms()
        with self._unit_of_work_factory() as unit_of_work:
            snapshot = unit_of_work.get_stream_state(stream)
            if snapshot.state in {
                StreamLifecycleState.UNINITIALIZED,
                StreamLifecycleState.DEGRADED,
            }:
                snapshot = transition_stream_state(
                    snapshot,
                    StreamLifecycleState.BOOTSTRAPPING,
                    changed_at_ms=now_ms,
                )
                unit_of_work.save_stream_state(snapshot)
            unit_of_work.commit()

    def _record_window_success(self, stream: StreamKey, window_start_ms: int) -> None:
        now_ms = self._clock.now_ms()
        with self._unit_of_work_factory() as unit_of_work:
            snapshot = unit_of_work.get_stream_state(stream)
            earliest = snapshot.earliest_available_open_time_ms
            snapshot = replace(
                snapshot,
                earliest_available_open_time_ms=(
                    window_start_ms if earliest is None else min(earliest, window_start_ms)
                ),
                last_rest_success_at_ms=now_ms,
                last_error_code=None,
                last_error_detail=None,
                updated_at_ms=now_ms,
            )
            unit_of_work.save_stream_state(snapshot)
            unit_of_work.commit()

    def _record_failure(self, stream: StreamKey, exc: Exception) -> None:
        now_ms = self._clock.now_ms()
        decision = classify_backfill_failure(exc)
        with self._unit_of_work_factory() as unit_of_work:
            snapshot = unit_of_work.get_stream_state(stream)
            try:
                failed_or_degraded = transition_stream_state(
                    snapshot,
                    decision.target_state,
                    changed_at_ms=now_ms,
                    error_code=decision.code,
                    error_detail=decision.detail,
                )
            except InvalidStreamTransition:
                failed_or_degraded = replace(
                    snapshot,
                    last_error_code=decision.code,
                    last_error_detail=decision.detail,
                    updated_at_ms=now_ms,
                )
            unit_of_work.save_stream_state(failed_or_degraded)
            unit_of_work.commit()

    def _transition_to_auditing(self, stream: StreamKey) -> None:
        now_ms = self._clock.now_ms()
        with self._unit_of_work_factory() as unit_of_work:
            snapshot = unit_of_work.get_stream_state(stream)
            if snapshot.state is StreamLifecycleState.BOOTSTRAPPING:
                snapshot = transition_stream_state(
                    snapshot,
                    StreamLifecycleState.AUDITING,
                    changed_at_ms=now_ms,
                )
                unit_of_work.save_stream_state(snapshot)
            unit_of_work.commit()

    def _resume_start(self, request: BackfillStreamRequest) -> int:
        step_ms = get_timeframe(request.stream.timeframe).duration_ms
        with self._unit_of_work_factory() as unit_of_work:
            snapshot = unit_of_work.get_stream_state(request.stream)
        latest = snapshot.latest_committed_open_time_ms
        if latest is None:
            return request.start_time_ms
        return max(request.start_time_ms, latest + step_ms)
