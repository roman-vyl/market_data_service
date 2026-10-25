"""Durable progress of the open interest track, separate from stream lifecycle.

Implements both the lower-bound progress store and the repair recorder for the
open interest track. Nothing here reads or writes ``stream_state``.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from typing import Any, Protocol

from market_data_service.domain.gaps import Gap
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.open_interest import OPEN_INTEREST_TRACK, TrackProgress
from market_data_service.ports.storage import CanonicalStorageUnitOfWork


class Clock(Protocol):
    def now_ms(self) -> int: ...


class OpenInterestTrackState:
    def __init__(
        self,
        unit_of_work_factory: Callable[[], CanonicalStorageUnitOfWork],
        clock: Clock,
    ) -> None:
        self._unit_of_work_factory = unit_of_work_factory
        self._clock = clock

    def progress(self, stream: StreamKey) -> TrackProgress:
        with self._unit_of_work_factory() as unit_of_work:
            return unit_of_work.get_track_progress(stream, OPEN_INTEREST_TRACK)

    # Lower-bound progress store.
    def cached_earliest(self, stream: StreamKey) -> int | None:
        return self.progress(stream).earliest_available_open_time_ms

    def discovery_start(self, stream: StreamKey, launch_search_start: int) -> int:
        cursor = self.current_discovery_cursor(stream)
        return launch_search_start if cursor is None else max(launch_search_start, cursor)

    def current_discovery_cursor(self, stream: StreamKey) -> int | None:
        return self.progress(stream).discovery_next_open_time_ms

    def advance_discovery(self, stream: StreamKey, next_open_time_ms: int) -> None:
        if self.cached_earliest(stream) is not None:
            raise RuntimeError("cannot advance discovery cursor after lower-bound resolution")
        self._update(stream, discovery_next_open_time_ms=next_open_time_ms, **_no_error())

    def resolve(self, stream: StreamKey, earliest_open_time_ms: int) -> None:
        self._update(
            stream,
            earliest_available_open_time_ms=earliest_open_time_ms,
            discovery_next_open_time_ms=None,
            **_no_error(),
        )

    # Repair recorder: the open interest track has no lifecycle transitions.
    def ensure_auditing(self, stream: StreamKey) -> None:
        return None

    def transition_to_repairing(self, stream: StreamKey) -> None:
        return None

    def transition_repairing_to_auditing(self, stream: StreamKey) -> None:
        return None

    def record_audit(self, stream: StreamKey) -> None:
        self._update(stream, last_audit_at_ms=self._clock.now_ms())

    def record_rest_success(self, stream: StreamKey) -> None:
        self._update(stream, last_success_at_ms=self._clock.now_ms(), **_no_error())

    def record_failure(self, stream: StreamKey, exc: Exception) -> None:
        self._update(stream, last_error_code=type(exc).__name__, last_error_detail=str(exc))

    def record_unresolved_gaps(self, stream: StreamKey, gaps: tuple[Gap, ...]) -> None:
        self._update(stream, last_audit_gap_count=len(gaps))

    def record_audit_result(self, stream: StreamKey, gap_count: int) -> None:
        self._update(stream, last_audit_at_ms=self._clock.now_ms(), last_audit_gap_count=gap_count)

    def _update(self, stream: StreamKey, **changes: Any) -> None:
        now_ms = self._clock.now_ms()
        with self._unit_of_work_factory() as unit_of_work:
            current = unit_of_work.get_track_progress(stream, OPEN_INTEREST_TRACK)
            unit_of_work.save_track_progress(
                replace(current, **changes, updated_at_ms=max(now_ms, current.updated_at_ms))
            )
            unit_of_work.commit()


def _no_error() -> dict[str, None]:
    return {"last_error_code": None, "last_error_detail": None}
