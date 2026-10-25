"""Track-specific pieces of lower-bound discovery: window probe and progress."""

from __future__ import annotations

from typing import Protocol

from market_data_service.domain.candle_validation import validate_observed_candle
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.open_interest import open_interest_issue
from market_data_service.domain.windows import TimeWindow
from market_data_service.ports.market_data_source import HistoricalCandleSource, OpenInterestSource


class HistoricalLowerBoundUnavailable(RuntimeError):
    """Raised when a stream has no observable closed candle in the searchable range."""


class LowerBoundProbe(Protocol):
    """Answer "earliest valid point of this track in this window, or none"."""

    max_points_per_probe: int

    def earliest_valid(
        self,
        stream: StreamKey,
        window: TimeWindow,
        *,
        observed_at_ms: int,
    ) -> int | None: ...


class LowerBoundProgress(Protocol):
    def cached_earliest(self, stream: StreamKey) -> int | None: ...

    def discovery_start(self, stream: StreamKey, launch_search_start: int) -> int: ...

    def current_discovery_cursor(self, stream: StreamKey) -> int | None: ...

    def advance_discovery(self, stream: StreamKey, next_open_time_ms: int) -> None: ...

    def resolve(self, stream: StreamKey, earliest_open_time_ms: int) -> None: ...


class CandleLowerBoundProbe:
    def __init__(self, source: HistoricalCandleSource, *, max_points_per_probe: int) -> None:
        self._source = source
        self.max_points_per_probe = max_points_per_probe

    def earliest_valid(
        self,
        stream: StreamKey,
        window: TimeWindow,
        *,
        observed_at_ms: int,
    ) -> int | None:
        candles = self._source.fetch_closed_candles(stream, window, observed_at_ms=observed_at_ms)
        candidates = [
            candle.open_time_ms
            for candle in candles
            if candle.stream == stream
            and window.contains(candle.open_time_ms)
            and not validate_observed_candle(candle)
        ]
        if not candidates:
            if candles:
                raise HistoricalLowerBoundUnavailable(
                    "historical probe returned observations but no valid candle"
                )
            return None
        return min(candidates)


class OpenInterestLowerBoundProbe:
    def __init__(self, source: OpenInterestSource, *, max_points_per_probe: int) -> None:
        self._source = source
        self.max_points_per_probe = max_points_per_probe

    def earliest_valid(
        self,
        stream: StreamKey,
        window: TimeWindow,
        *,
        observed_at_ms: int,
    ) -> int | None:
        points = self._source.fetch_open_interest(stream, window, observed_at_ms=observed_at_ms)
        candidates = [
            point.open_time_ms
            for point in points
            if point.stream == stream
            and window.contains(point.open_time_ms)
            and open_interest_issue(point) is None
        ]
        if not candidates:
            if points:
                raise HistoricalLowerBoundUnavailable(
                    "open interest probe returned observations but no valid point"
                )
            return None
        return min(candidates)
