"""One-shot deterministic historical startup reconciliation."""

from __future__ import annotations

from collections.abc import Callable, Sequence

from market_data_service.application.audit_continuity import (
    AuditStreamContinuity,
    AuditStreamContinuityRequest,
)
from market_data_service.application.full_bootstrap import (
    BootstrapFullStreamHistory,
    FullHistoryBootstrapRequest,
)
from market_data_service.application.repair_gaps import RepairStreamGaps
from market_data_service.application.repair_types import (
    RepairStatus,
    RepairStreamGapsRequest,
)
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.timeframes import get_timeframe
from market_data_service.runtime.lifecycle import RuntimeLifecycleRecorder
from market_data_service.runtime.startup_types import (
    StartupClassification,
    StartupStreamOutcome,
)


class StartupCoordinator:
    def __init__(
        self,
        *,
        bootstrap_factory: Callable[[StreamKey], BootstrapFullStreamHistory],
        auditor: AuditStreamContinuity,
        repair: RepairStreamGaps,
        lifecycle: RuntimeLifecycleRecorder,
        backfill_windows_per_stream: int,
        repair_windows_per_stream: int,
    ) -> None:
        self._bootstrap_factory = bootstrap_factory
        self._auditor = auditor
        self._repair = repair
        self._lifecycle = lifecycle
        self._backfill_windows = backfill_windows_per_stream
        self._repair_windows = repair_windows_per_stream

    def execute(self, streams: Sequence[StreamKey]) -> tuple[StartupStreamOutcome, ...]:
        return tuple(self._reconcile(stream) for stream in streams)

    def _reconcile(self, stream: StreamKey) -> StartupStreamOutcome:
        try:
            self._lifecycle.prepare_for_bootstrap(stream)
            bootstrap = self._bootstrap_factory(stream).execute(
                FullHistoryBootstrapRequest(
                    stream=stream,
                    max_windows=self._backfill_windows,
                )
            )
            if bootstrap.error_code is not None:
                classification = (
                    StartupClassification.RECOVERABLE_FAILURE
                    if bootstrap.failure_disposition == "recoverable"
                    else StartupClassification.FATAL_FAILURE
                )
                return StartupStreamOutcome(
                    stream,
                    classification,
                    error_code=bootstrap.error_code,
                    error_detail=bootstrap.error_detail,
                )
            if not bootstrap.reached_target or bootstrap.lower_bound is None:
                return StartupStreamOutcome(stream, StartupClassification.INCOMPLETE)
            start_ms = bootstrap.lower_bound.earliest_available_open_time_ms
            target_ms = bootstrap.target_open_time_ms
            if start_ms is None or target_ms is None:
                return StartupStreamOutcome(stream, StartupClassification.INCOMPLETE)
            step_ms = get_timeframe(stream.timeframe).duration_ms
            end_ms = target_ms + step_ms
            self._lifecycle.mark_auditing(stream)
            audit = self._auditor.execute(
                AuditStreamContinuityRequest(stream, start_ms, end_ms)
            )
            if not audit.is_continuous:
                repair = self._repair.execute(
                    RepairStreamGapsRequest(
                        stream,
                        start_ms,
                        end_ms,
                        max_windows=self._repair_windows,
                    )
                )
                if repair.status is RepairStatus.FAILED:
                    classification = (
                        StartupClassification.RECOVERABLE_FAILURE
                        if repair.failure_disposition == "recoverable"
                        else StartupClassification.FATAL_FAILURE
                    )
                    return StartupStreamOutcome(
                        stream,
                        classification,
                        audit=audit,
                        error_code=repair.error_code,
                        error_detail=repair.error_detail,
                    )
                audit = repair.post_repair_audit or audit
            if not audit.is_continuous:
                return StartupStreamOutcome(
                    stream,
                    StartupClassification.INCOMPLETE,
                    audit=audit,
                    error_code="startup_continuity_incomplete",
                )
            self._lifecycle.mark_connecting(stream)
            return StartupStreamOutcome(
                stream,
                StartupClassification.CONNECTING,
                audit=audit,
            )
        except Exception as exc:
            return StartupStreamOutcome(
                stream,
                StartupClassification.FATAL_FAILURE,
                error_code=type(exc).__name__,
                error_detail=str(exc),
            )
