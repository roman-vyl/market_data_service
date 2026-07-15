"""Read committed candle bounds without lifecycle side effects."""

from __future__ import annotations

from collections.abc import Callable

from market_data_service.application.audit_continuity import UnknownStreamError
from market_data_service.application.history_planning.models import StreamBoundsResult
from market_data_service.domain.identity import StreamKey
from market_data_service.ports.storage import CanonicalStorageUnitOfWork


class GetStreamBounds:
    def __init__(
        self,
        unit_of_work_factory: Callable[[], CanonicalStorageUnitOfWork],
    ) -> None:
        self._unit_of_work_factory = unit_of_work_factory

    def execute(self, stream: StreamKey) -> StreamBoundsResult:
        with self._unit_of_work_factory() as unit_of_work:
            if not unit_of_work.stream_exists(stream):
                raise UnknownStreamError(f"stream is not registered: {stream.canonical_id}")
            earliest, latest = unit_of_work.get_committed_candle_bounds(stream)
            state = unit_of_work.get_stream_state(stream)
        return StreamBoundsResult(
            stream=stream,
            state=state.state.value,
            earliest_committed_open_time_ms=earliest,
            latest_committed_open_time_ms=latest,
        )
