"""Orchestrate resumable full-history bootstrap for one stream."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal, Protocol

from market_data_service.application.backfill_stream import BackfillStreamHistory
from market_data_service.application.backfill_types import (
    BackfillStreamRequest,
    BackfillStreamResult,
)
from market_data_service.application.lower_bound import (
    HistoricalLowerBoundResult,
    HistoricalLowerBoundUnavailable,
    ResolveHistoricalLowerBound,
)
from market_data_service.application.stream_failure import record_stream_failure
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.stream_state import StreamLifecycleState, transition_stream_state
from market_data_service.domain.timeframes import get_timeframe, last_closed_open_time_ms
from market_data_service.ports.storage import CanonicalStorageUnitOfWork

FullBootstrapStatus = Literal["backfilled", "incomplete", "lower_bound_unresolved"]


@dataclass(frozen=True, slots=True)
class FullHistoryBootstrapRequest:
    stream: StreamKey
    max_windows: int

    def __post_init__(self) -> None:
        if self.max_windows <= 0:
            raise ValueError("max_windows must be positive")
        if self.stream.timeframe != "1m":
            raise ValueError("full-history bootstrap is currently defined for canonical 1m streams")


@dataclass(frozen=True, slots=True)
class FullHistoryBootstrapResult:
    stream: StreamKey
    status: FullBootstrapStatus
    max_windows: int
    target_open_time_ms: int | None
    lower_bound: HistoricalLowerBoundResult | None
    backfill: BackfillStreamResult | None
    error_code: str | None = None
    error_detail: str | None = None

    @property
    def reached_target(self) -> bool:
        return self.backfill is not None and self.backfill.reached_end

    @property
    def lower_bound_resolved(self) -> bool:
        return self.lower_bound is not None and self.lower_bound.resolved

    @property
    def discovery_windows_used(self) -> int:
        return 0 if self.lower_bound is None else self.lower_bound.discovery_windows_used

    @property
    def backfill_windows_attempted(self) -> int:
        return 0 if self.backfill is None else self.backfill.attempted_windows

    @property
    def total_windows_used(self) -> int:
        return self.discovery_windows_used + self.backfill_windows_attempted


class BootstrapFullStreamHistory:
    """Resolve lower bound, calculate the current target, and run bounded backfill."""

    def __init__(
        self,
        lower_bound_resolver: ResolveHistoricalLowerBound,
        backfill: BackfillStreamHistory,
        unit_of_work_factory: Callable[[], CanonicalStorageUnitOfWork],
        clock: Clock,
    ) -> None:
        self._lower_bound_resolver = lower_bound_resolver
        self._backfill = backfill
        self._unit_of_work_factory = unit_of_work_factory
        self._clock = clock

    def execute(self, request: FullHistoryBootstrapRequest) -> FullHistoryBootstrapResult:
        self._ensure_bootstrapping(request.stream)
        try:
            lower_bound = self._lower_bound_resolver.execute(
                request.stream,
                max_windows=request.max_windows,
            )
        except HistoricalLowerBoundUnavailable as exc:
            return FullHistoryBootstrapResult(
                stream=request.stream,
                status="lower_bound_unresolved",
                max_windows=request.max_windows,
                target_open_time_ms=None,
                lower_bound=None,
                backfill=None,
                error_code=type(exc).__name__,
                error_detail=str(exc),
            )
        except Exception as exc:
            record_stream_failure(
                self._unit_of_work_factory,
                request.stream,
                exc,
                now_ms=self._clock.now_ms(),
            )
            return FullHistoryBootstrapResult(
                stream=request.stream,
                status="lower_bound_unresolved",
                max_windows=request.max_windows,
                target_open_time_ms=None,
                lower_bound=None,
                backfill=None,
                error_code=type(exc).__name__,
                error_detail=str(exc),
            )

        if not lower_bound.resolved:
            return FullHistoryBootstrapResult(
                stream=request.stream,
                status="incomplete",
                max_windows=request.max_windows,
                target_open_time_ms=None,
                lower_bound=lower_bound,
                backfill=None,
            )

        step_ms = get_timeframe(request.stream.timeframe).duration_ms
        target_open_time_ms = last_closed_open_time_ms(self._clock.now_ms(), step_ms)
        remaining_budget = request.max_windows - lower_bound.discovery_windows_used
        if remaining_budget <= 0:
            return FullHistoryBootstrapResult(
                stream=request.stream,
                status="incomplete",
                max_windows=request.max_windows,
                target_open_time_ms=target_open_time_ms,
                lower_bound=lower_bound,
                backfill=None,
            )
        if lower_bound.earliest_available_open_time_ms is None:
            raise RuntimeError("resolved lower bound missing earliest open time")
        backfill = self._backfill.execute(
            BackfillStreamRequest(
                stream=request.stream,
                start_time_ms=lower_bound.earliest_available_open_time_ms,
                end_time_ms=target_open_time_ms + step_ms,
                max_windows=remaining_budget,
            )
        )
        return FullHistoryBootstrapResult(
            stream=request.stream,
            status="backfilled",
            max_windows=request.max_windows,
            target_open_time_ms=target_open_time_ms,
            lower_bound=lower_bound,
            backfill=backfill,
            error_code=backfill.error_code,
            error_detail=backfill.error_detail,
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


class Clock(Protocol):
    def now_ms(self) -> int: ...
