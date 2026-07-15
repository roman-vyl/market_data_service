"""Expose continuity audit together with lifecycle and provenance facts."""

from __future__ import annotations

from collections.abc import Callable

from market_data_service.application.audit_continuity import (
    AuditStreamContinuity,
    AuditStreamContinuityRequest,
)
from market_data_service.application.consumer_read.provenance import canonical_market_data_hash
from market_data_service.application.history_planning.models import ContinuityAuditResult
from market_data_service.domain.identity import StreamKey
from market_data_service.ports.consumer_read import ConsumerCandleReader
from market_data_service.ports.storage import CanonicalStorageUnitOfWork


class AuditStreamRange:
    def __init__(
        self,
        auditor: AuditStreamContinuity,
        unit_of_work_factory: Callable[[], CanonicalStorageUnitOfWork],
        reader: ConsumerCandleReader | None = None,
    ) -> None:
        self._auditor = auditor
        self._unit_of_work_factory = unit_of_work_factory
        self._reader = reader

    def execute(
        self,
        stream: StreamKey,
        *,
        start_time_ms: int,
        end_time_ms: int,
    ) -> ContinuityAuditResult:
        report = self._auditor.execute(
            AuditStreamContinuityRequest(
                stream=stream,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
        )
        with self._unit_of_work_factory() as unit_of_work:
            state = unit_of_work.get_stream_state(stream)
        market_data_hash = None
        if report.is_continuous and self._reader is not None:
            snapshot = self._reader.read_snapshot(
                stream,
                start_time_ms=start_time_ms,
                end_time_ms=end_time_ms,
            )
            market_data_hash = canonical_market_data_hash(
                stream=stream,
                from_ms=start_time_ms,
                to_ms=end_time_ms,
                candles=snapshot.candles,
            )
        return ContinuityAuditResult(
            stream=stream,
            checked_start_ms=report.checked_start_ms,
            checked_end_ms=report.checked_end_ms,
            candle_count=report.candle_count,
            is_continuous=report.is_continuous,
            gaps=report.gaps,
            state=state.state.value,
            market_data_hash=market_data_hash,
        )
