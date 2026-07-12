"""Derive bounded recovery windows from durable stream progress."""

from __future__ import annotations

from collections.abc import Callable

from market_data_service.application.realtime.events import RecoveryRequired
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.timeframes import align_to_grid, get_timeframe
from market_data_service.domain.windows import TimeWindow
from market_data_service.ports.storage import CanonicalStorageUnitOfWork


class RealtimeRecoveryPlanner:
    def __init__(
        self,
        unit_of_work_factory: Callable[[], CanonicalStorageUnitOfWork],
        now_ms: Callable[[], int],
    ) -> None:
        self._unit_of_work_factory = unit_of_work_factory
        self._now_ms = now_ms

    def derive_window(self, signal: RecoveryRequired) -> TimeWindow | None:
        stream = signal.stream
        step_ms = get_timeframe(stream.timeframe).duration_ms
        with self._unit_of_work_factory() as unit_of_work:
            snapshot = unit_of_work.get_stream_state(stream)
        latest = snapshot.latest_committed_open_time_ms
        if latest is None:
            return None
        end_ms = align_to_grid(self._now_ms(), step_ms)
        default_start = latest + step_ms
        hinted_start = signal.suspected_start_time_ms
        start_ms = default_start if hinted_start is None else min(default_start, hinted_start)
        if snapshot.earliest_available_open_time_ms is not None:
            start_ms = max(start_ms, snapshot.earliest_available_open_time_ms)
        if start_ms >= end_ms:
            start_ms = latest
            end_ms = latest + step_ms
        return TimeWindow(start_ms, end_ms)

    def needs_backfill(self, stream: StreamKey, window: TimeWindow) -> bool:
        step_ms = get_timeframe(stream.timeframe).duration_ms
        with self._unit_of_work_factory() as unit_of_work:
            latest = unit_of_work.get_stream_state(stream).latest_committed_open_time_ms
        return not (
            latest is not None
            and window.start_ms == latest
            and window.end_ms == latest + step_ms
        )
