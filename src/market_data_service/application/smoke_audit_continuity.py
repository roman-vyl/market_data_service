"""Application workflow for REST backfill plus continuity smoke verification."""

from __future__ import annotations

from dataclasses import dataclass

from market_data_service.application.audit_continuity import (
    AuditStreamContinuity,
    AuditStreamContinuityRequest,
)
from market_data_service.application.backfill_stream import BackfillStreamHistory
from market_data_service.application.backfill_types import BackfillStreamRequest
from market_data_service.domain import ContinuityReport, StreamKey, TimeWindow


@dataclass(frozen=True, slots=True)
class SmokeAuditContinuityResult:
    stream: StreamKey
    window: TimeWindow
    backfill_observed: int
    backfill_committed: int
    backfill_duplicates: int
    backfill_corrected: int
    backfill_rejected: int
    audit: ContinuityReport

    @property
    def ok(self) -> bool:
        return (
            self.backfill_committed > 0
            and self.backfill_duplicates == 0
            and self.audit.is_continuous
            and not self.audit.gaps
            and self.audit.candle_count == self.backfill_committed
        )


def run_smoke_audit_continuity_workflow(
    *,
    stream: StreamKey,
    window: TimeWindow,
    backfill: BackfillStreamHistory,
    auditor: AuditStreamContinuity,
) -> SmokeAuditContinuityResult:
    backfill_result = backfill.execute(
        BackfillStreamRequest(
            stream=stream,
            start_time_ms=window.start_ms,
            end_time_ms=window.end_ms,
            max_windows=1,
        )
    )
    if backfill_result.error_code is not None:
        raise RuntimeError(
            f"backfill audit smoke failed: {backfill_result.error_code}: "
            f"{backfill_result.error_detail}"
        )
    if not backfill_result.window_results:
        raise RuntimeError("backfill audit smoke did not import a window")

    audit = auditor.execute(
        AuditStreamContinuityRequest(
            stream=stream,
            start_time_ms=window.start_ms,
            end_time_ms=window.end_ms,
        )
    )
    window_result = backfill_result.window_results[0]
    return SmokeAuditContinuityResult(
        stream=stream,
        window=window,
        backfill_observed=window_result.observed,
        backfill_committed=window_result.committed,
        backfill_duplicates=window_result.duplicates,
        backfill_corrected=window_result.corrected,
        backfill_rejected=window_result.rejected,
        audit=audit,
    )
