"""Live source of the open interest track: one REST point per committed bar."""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

from market_data_service.application.import_window import ImportWindowResult
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.timeframes import get_timeframe
from market_data_service.domain.windows import TimeWindow
from market_data_service.ports.storage import CanonicalStorageUnitOfWork


class WindowImporter(Protocol):
    def execute(self, stream: StreamKey, window: TimeWindow) -> ImportWindowResult: ...


class AppendLiveOpenInterest:
    """Store open interest for exactly one bar, fetching it only when it is not stored.

    Plays the role WebSocket plays for candles: it brings the next point of the
    track. Continuity, gaps and recovery stay with the realtime supervisor and
    realtime recovery.
    """

    def __init__(
        self,
        importer: WindowImporter,
        unit_of_work_factory: Callable[[], CanonicalStorageUnitOfWork],
    ) -> None:
        self._importer = importer
        self._unit_of_work_factory = unit_of_work_factory

    def execute(self, stream: StreamKey, open_time_ms: int) -> bool:
        if self._stored(stream, open_time_ms):
            return True
        step_ms = get_timeframe(stream.timeframe).duration_ms
        self._importer.execute(stream, TimeWindow(open_time_ms, open_time_ms + step_ms))
        return self._stored(stream, open_time_ms)

    def _stored(self, stream: StreamKey, open_time_ms: int) -> bool:
        with self._unit_of_work_factory() as unit_of_work:
            return unit_of_work.get_open_interest(stream, open_time_ms) is not None
