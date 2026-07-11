"""Application use case for canonical history continuity audit."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from market_data_service.domain import (
    ContinuityReport,
    StreamKey,
    TimeWindow,
    build_continuity_report,
    get_timeframe,
)
from market_data_service.ports.storage import CanonicalStorageUnitOfWork


@dataclass(frozen=True, slots=True)
class AuditStreamContinuityRequest:
    stream: StreamKey
    start_time_ms: int
    end_time_ms: int

    def __post_init__(self) -> None:
        if self.start_time_ms >= self.end_time_ms:
            raise ValueError("audit range must satisfy start_time_ms < end_time_ms")
        step_ms = get_timeframe(self.stream.timeframe).duration_ms
        if self.start_time_ms % step_ms or self.end_time_ms % step_ms:
            raise ValueError("audit range must be aligned to stream timeframe")


class AuditStreamContinuity:
    """Audit one explicit stream/range without changing stream state."""

    def __init__(
        self,
        unit_of_work_factory: Callable[[], CanonicalStorageUnitOfWork],
    ) -> None:
        self._unit_of_work_factory = unit_of_work_factory

    def execute(self, request: AuditStreamContinuityRequest) -> ContinuityReport:
        with self._unit_of_work_factory() as unit_of_work:
            if not unit_of_work.stream_exists(request.stream):
                raise ValueError(f"stream is not registered: {request.stream.canonical_id}")
            candles = unit_of_work.list_candles(
                request.stream,
                start_time_ms=request.start_time_ms,
                end_time_ms=request.end_time_ms,
            )

        return build_continuity_report(
            stream=request.stream,
            audit_window=TimeWindow(request.start_time_ms, request.end_time_ms),
            observed_open_times_ms=tuple(candle.open_time_ms for candle in candles),
            step_ms=get_timeframe(request.stream.timeframe).duration_ms,
        )
