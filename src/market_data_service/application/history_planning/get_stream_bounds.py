"""Read committed history bounds without lifecycle side effects."""

from __future__ import annotations

from collections.abc import Callable, Collection

from market_data_service.application.audit_continuity import UnknownStreamError
from market_data_service.application.history_planning.models import StreamBoundsResult
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.open_interest import OPEN_INTEREST_TRACK
from market_data_service.ports.storage import CanonicalStorageUnitOfWork


class GetStreamBounds:
    """Bounds of a stream: committed candles, intersected with the open interest
    track for a stream that carries it. Bounds only, never a continuity proof."""

    def __init__(
        self,
        unit_of_work_factory: Callable[[], CanonicalStorageUnitOfWork],
        open_interest_streams: Collection[StreamKey] = (),
    ) -> None:
        self._unit_of_work_factory = unit_of_work_factory
        self._open_interest_streams = frozenset(open_interest_streams)

    def execute(self, stream: StreamKey) -> StreamBoundsResult:
        with self._unit_of_work_factory() as unit_of_work:
            if not unit_of_work.stream_exists(stream):
                raise UnknownStreamError(f"stream is not registered: {stream.canonical_id}")
            earliest, latest = unit_of_work.get_committed_candle_bounds(stream)
            if stream in self._open_interest_streams:
                progress = unit_of_work.get_track_progress(stream, OPEN_INTEREST_TRACK)
                earliest, latest = _intersection(
                    (earliest, latest),
                    (
                        progress.earliest_available_open_time_ms,
                        progress.latest_committed_open_time_ms,
                    ),
                )
            state = unit_of_work.get_stream_state(stream)
        return StreamBoundsResult(
            stream=stream,
            state=state.state.value,
            earliest_committed_open_time_ms=earliest,
            latest_committed_open_time_ms=latest,
        )


def _intersection(
    candles: tuple[int | None, int | None],
    open_interest: tuple[int | None, int | None],
) -> tuple[int | None, int | None]:
    """Both tracks must have bounds; the bounds of one track are never returned alone."""

    candle_earliest, candle_latest = candles
    oi_earliest, oi_latest = open_interest
    if candle_earliest is None or candle_latest is None or oi_earliest is None or oi_latest is None:
        return None, None
    earliest = max(candle_earliest, oi_earliest)
    latest = min(candle_latest, oi_latest)
    if earliest > latest:
        return None, None
    return earliest, latest
