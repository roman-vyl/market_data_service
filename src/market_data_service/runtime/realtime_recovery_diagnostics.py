"""Structured diagnostics for bounded realtime recovery attempts."""

from __future__ import annotations

import logging

from market_data_service.application.realtime.recovery_types import RealtimeRecoveryResult
from market_data_service.application.repair_types import RepairProgressMarker
from market_data_service.domain.identity import StreamKey


class RealtimeRecoveryDiagnostics:
    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    def record(
        self,
        *,
        stream: StreamKey,
        reason: str,
        before: RepairProgressMarker | None,
        result: RealtimeRecoveryResult,
        delay_seconds: float,
        no_progress_attempts: int,
    ) -> None:
        window = result.recovery_window
        next_window = result.next_recovery_window
        after = result.progress_marker
        totals = self._counts(result)
        self._logger.info(
            "realtime recovery result stream=%s reason=%s classification=%s "
            "start_ms=%s end_ms=%s before_remaining=%s before_gap_ms=%s "
            "after_remaining=%s after_gap_ms=%s next_start_ms=%s next_end_ms=%s "
            "attempted=%s completed=%s committed=%s duplicates=%s corrected=%s "
            "rejected=%s unexpected=%s error=%s detail=%s delay_seconds=%s "
            "no_progress_attempts=%s",
            stream.canonical_id,
            reason,
            result.classification.value,
            None if window is None else window.start_ms,
            None if window is None else window.end_ms,
            None if before is None else before.remaining_missing_candles,
            None if before is None else before.earliest_remaining_gap_start_ms,
            None if after is None else after.remaining_missing_candles,
            None if after is None else after.earliest_remaining_gap_start_ms,
            None if next_window is None else next_window.start_ms,
            None if next_window is None else next_window.end_ms,
            totals[0],
            totals[1],
            totals[2],
            totals[3],
            totals[4],
            totals[5],
            totals[6],
            result.error_code,
            result.error_detail,
            delay_seconds,
            no_progress_attempts,
        )

    @staticmethod
    def _counts(result: RealtimeRecoveryResult) -> tuple[int, ...]:
        repair = result.repair
        if repair is None:
            return (0, 0, 0, 0, 0, 0, 0)
        return (
            repair.attempted_windows,
            repair.completed_windows,
            sum(item.committed for item in repair.window_results),
            sum(item.duplicates for item in repair.window_results),
            sum(item.corrected for item in repair.window_results),
            sum(item.rejected for item in repair.window_results),
            sum(item.unexpected for item in repair.window_results),
        )
