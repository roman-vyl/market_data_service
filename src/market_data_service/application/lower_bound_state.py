"""Durable progress operations for historical lower-bound discovery."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace

from market_data_service.domain.identity import StreamKey
from market_data_service.ports.storage import CanonicalStorageUnitOfWork


class HistoricalLowerBoundState:
    def __init__(
        self,
        unit_of_work_factory: Callable[[], CanonicalStorageUnitOfWork],
        now_ms: Callable[[], int],
    ) -> None:
        self._unit_of_work_factory = unit_of_work_factory
        self._now_ms = now_ms

    def discovery_start(self, stream: StreamKey, launch_search_start: int) -> int:
        cursor = self.current_discovery_cursor(stream)
        return launch_search_start if cursor is None else max(launch_search_start, cursor)

    def current_discovery_cursor(self, stream: StreamKey) -> int | None:
        with self._unit_of_work_factory() as unit_of_work:
            return unit_of_work.get_stream_state(
                stream
            ).lower_bound_discovery_next_open_time_ms

    def advance_discovery(self, stream: StreamKey, next_open_time_ms: int) -> None:
        now_ms = self._now_ms()
        with self._unit_of_work_factory() as unit_of_work:
            snapshot = unit_of_work.get_stream_state(stream)
            if snapshot.earliest_available_open_time_ms is not None:
                raise RuntimeError("cannot advance discovery cursor after lower-bound resolution")
            unit_of_work.save_stream_state(
                replace(
                    snapshot,
                    lower_bound_discovery_next_open_time_ms=next_open_time_ms,
                    last_error_code=None,
                    last_error_detail=None,
                    updated_at_ms=max(now_ms, snapshot.updated_at_ms),
                )
            )
            unit_of_work.commit()

    def resolve(self, stream: StreamKey, earliest_open_time_ms: int) -> None:
        now_ms = self._now_ms()
        with self._unit_of_work_factory() as unit_of_work:
            snapshot = unit_of_work.get_stream_state(stream)
            unit_of_work.save_stream_state(
                replace(
                    snapshot,
                    earliest_available_open_time_ms=earliest_open_time_ms,
                    lower_bound_discovery_next_open_time_ms=None,
                    last_error_code=None,
                    last_error_detail=None,
                    updated_at_ms=max(now_ms, snapshot.updated_at_ms),
                )
            )
            unit_of_work.commit()
