"""Application workflow used by bounded backfill smoke verification."""

from __future__ import annotations

from dataclasses import dataclass

from market_data_service.application.backfill_stream import BackfillStreamHistory
from market_data_service.application.backfill_types import BackfillStreamRequest
from market_data_service.application.import_window import ImportHistoricalWindow
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.windows import TimeWindow


@dataclass(frozen=True, slots=True)
class SmokeBackfillWorkflowResult:
    stream: StreamKey
    window: TimeWindow
    first_observed: int
    first_committed: int
    first_duplicates: int
    first_corrected: int
    first_rejected: int
    duplicate_observed: int
    duplicate_committed: int
    duplicate_duplicates: int
    duplicate_corrected: int
    duplicate_rejected: int


def run_backfill_smoke_workflow(
    *,
    stream: StreamKey,
    window: TimeWindow,
    backfill: BackfillStreamHistory,
    duplicate_replay: ImportHistoricalWindow,
) -> SmokeBackfillWorkflowResult:
    """Run bounded backfill once, then replay the same window for duplicate proof."""

    first = backfill.execute(
        BackfillStreamRequest(
            stream=stream,
            start_time_ms=window.start_ms,
            end_time_ms=window.end_ms,
            max_windows=1,
        )
    )
    if first.error_code is not None:
        raise RuntimeError(f"backfill smoke failed: {first.error_code}: {first.error_detail}")
    if not first.window_results:
        raise RuntimeError("backfill smoke did not import a window")

    first_window = first.window_results[0]
    duplicate = duplicate_replay.execute(stream, window)
    return SmokeBackfillWorkflowResult(
        stream=stream,
        window=window,
        first_observed=first_window.observed,
        first_committed=first_window.committed,
        first_duplicates=first_window.duplicates,
        first_corrected=first_window.corrected,
        first_rejected=first_window.rejected,
        duplicate_observed=duplicate.observed,
        duplicate_committed=duplicate.committed,
        duplicate_duplicates=duplicate.duplicates,
        duplicate_corrected=duplicate.corrected,
        duplicate_rejected=duplicate.rejected,
    )
