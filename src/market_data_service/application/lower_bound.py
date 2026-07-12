"""Resolve and cache the observed historical lower bound for one stream."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Protocol

from market_data_service.domain.candle_validation import validate_observed_candle
from market_data_service.domain.gaps import Gap, iter_fetch_windows
from market_data_service.domain.identity import InstrumentKey, StreamKey
from market_data_service.domain.instruments import InstrumentMetadata
from market_data_service.domain.timeframes import (
    ceil_to_grid,
    get_timeframe,
    last_closed_open_time_ms,
)
from market_data_service.ports.market_data_source import (
    HistoricalCandleSource,
    InstrumentMetadataSource,
)
from market_data_service.ports.storage import CanonicalStorageUnitOfWork


class HistoricalLowerBoundUnavailable(RuntimeError):
    """Raised when a stream has no observable closed candle in the searchable range."""


@dataclass(frozen=True, slots=True)
class HistoricalLowerBoundResult:
    stream: StreamKey
    launch_time_ms: int
    search_start_time_ms: int
    earliest_available_open_time_ms: int | None
    metadata_cached: bool
    lower_bound_cached: bool
    resolved: bool
    discovery_windows_used: int
    unresolved_reason: str | None = None


class ResolveHistoricalLowerBound:
    """Cache-aside lower-bound discovery for full-history bootstrap.

    Instrument launch time is metadata and only seeds the search. The persisted
    lower bound is the first actually returned valid candle for this stream.
    """

    def __init__(
        self,
        metadata_source: InstrumentMetadataSource,
        historical_source: HistoricalCandleSource,
        unit_of_work_factory: Callable[[], CanonicalStorageUnitOfWork],
        clock: Clock,
        *,
        max_candles_per_probe: int = 1000,
    ) -> None:
        self._metadata_source = metadata_source
        self._historical_source = historical_source
        self._unit_of_work_factory = unit_of_work_factory
        self._clock = clock
        self._max_candles_per_probe = max_candles_per_probe

    def execute(self, stream: StreamKey, *, max_windows: int) -> HistoricalLowerBoundResult:
        if max_windows <= 0:
            raise ValueError("max_windows must be positive")
        step_ms = get_timeframe(stream.timeframe).duration_ms
        cached = self._cached_result(stream, step_ms)
        if cached is not None:
            return cached

        metadata, metadata_cached = self._resolve_metadata(stream.instrument)
        if metadata.launch_time_ms is None:
            raise HistoricalLowerBoundUnavailable("instrument launch time is unresolved")
        search_start = ceil_to_grid(metadata.launch_time_ms, step_ms)
        search_end = last_closed_open_time_ms(self._clock.now_ms(), step_ms) + step_ms
        if search_start >= search_end:
            return HistoricalLowerBoundResult(
                stream=stream,
                launch_time_ms=metadata.launch_time_ms,
                search_start_time_ms=search_start,
                earliest_available_open_time_ms=None,
                metadata_cached=metadata_cached,
                lower_bound_cached=False,
                resolved=False,
                discovery_windows_used=0,
                unresolved_reason="no closed candles are searchable yet",
            )

        discovery_windows_used = 0
        for window in iter_fetch_windows(
            Gap(search_start, search_end),
            step_ms=step_ms,
            max_candles=self._max_candles_per_probe,
        ):
            if discovery_windows_used >= max_windows:
                return HistoricalLowerBoundResult(
                    stream=stream,
                    launch_time_ms=metadata.launch_time_ms,
                    search_start_time_ms=search_start,
                    earliest_available_open_time_ms=None,
                    metadata_cached=metadata_cached,
                    lower_bound_cached=False,
                    resolved=False,
                    discovery_windows_used=discovery_windows_used,
                    unresolved_reason="discovery window budget exhausted",
                )
            candles = self._historical_source.fetch_closed_candles(
                stream,
                window,
                observed_at_ms=self._clock.now_ms(),
            )
            discovery_windows_used += 1
            candidates = [
                candle
                for candle in candles
                if candle.stream == stream
                and window.contains(candle.open_time_ms)
                and not validate_observed_candle(candle)
            ]
            if not candidates:
                continue
            earliest = min(candle.open_time_ms for candle in candidates)
            self._persist_lower_bound(stream, earliest)
            return HistoricalLowerBoundResult(
                stream=stream,
                launch_time_ms=metadata.launch_time_ms,
                search_start_time_ms=search_start,
                earliest_available_open_time_ms=earliest,
                metadata_cached=metadata_cached,
                lower_bound_cached=False,
                resolved=True,
                discovery_windows_used=discovery_windows_used,
            )

        return HistoricalLowerBoundResult(
            stream=stream,
            launch_time_ms=metadata.launch_time_ms,
            search_start_time_ms=search_start,
            earliest_available_open_time_ms=None,
            metadata_cached=metadata_cached,
            lower_bound_cached=False,
            resolved=False,
            discovery_windows_used=discovery_windows_used,
            unresolved_reason="historical source returned no valid candles",
        )

    def _cached_result(self, stream: StreamKey, step_ms: int) -> HistoricalLowerBoundResult | None:
        with self._unit_of_work_factory() as unit_of_work:
            snapshot = unit_of_work.get_stream_state(stream)
            if snapshot.earliest_available_open_time_ms is None:
                return None
            metadata = unit_of_work.get_instrument_metadata(stream.instrument)
        if metadata.launch_time_ms is None:
            raise HistoricalLowerBoundUnavailable(
                "observed lower bound is cached but instrument launch time is unresolved"
            )
        return HistoricalLowerBoundResult(
            stream=stream,
            launch_time_ms=metadata.launch_time_ms,
            search_start_time_ms=ceil_to_grid(metadata.launch_time_ms, step_ms),
            earliest_available_open_time_ms=snapshot.earliest_available_open_time_ms,
            metadata_cached=True,
            lower_bound_cached=True,
            resolved=True,
            discovery_windows_used=0,
        )

    def _resolve_metadata(
        self,
        instrument: InstrumentKey,
    ) -> tuple[InstrumentMetadata, bool]:
        with self._unit_of_work_factory() as unit_of_work:
            metadata = unit_of_work.get_instrument_metadata(instrument)
        if metadata.launch_time_ms is not None:
            return metadata, True

        launch_time_ms = self._metadata_source.get_launch_time_ms(instrument)
        fetched_at_ms = self._clock.now_ms()
        metadata = replace(
            metadata,
            launch_time_ms=launch_time_ms,
            fetched_at_ms=fetched_at_ms,
        )
        with self._unit_of_work_factory() as unit_of_work:
            unit_of_work.save_instrument_metadata(metadata)
            unit_of_work.commit()
        return metadata, False

    def _persist_lower_bound(self, stream: StreamKey, earliest_open_time_ms: int) -> None:
        now_ms = self._clock.now_ms()
        with self._unit_of_work_factory() as unit_of_work:
            snapshot = unit_of_work.get_stream_state(stream)
            snapshot = replace(
                snapshot,
                earliest_available_open_time_ms=earliest_open_time_ms,
                last_error_code=None,
                last_error_detail=None,
                updated_at_ms=now_ms,
            )
            unit_of_work.save_stream_state(snapshot)
            unit_of_work.commit()


class Clock(Protocol):
    def now_ms(self) -> int: ...
