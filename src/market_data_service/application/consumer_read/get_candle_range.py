"""Read a complete canonical candle range from one ready stream."""

from __future__ import annotations

from market_data_service.application.consumer_read.errors import (
    ConfiguredStreamNotFound,
    StreamNotReady,
)
from market_data_service.application.consumer_read.models import (
    CandleRangeRequest,
    CandleRangeResult,
)
from market_data_service.application.consumer_read.validation import (
    validate_available_range,
    validate_complete_grid,
    validate_requested_range,
)
from market_data_service.config import ValidatedMarketConfig
from market_data_service.domain.identity import InstrumentKey, StreamKey
from market_data_service.domain.stream_state import StreamLifecycleState
from market_data_service.domain.timeframes import get_timeframe
from market_data_service.ports.consumer_read import ConsumerCandleReader


class GetCandleRange:
    def __init__(self, config: ValidatedMarketConfig, reader: ConsumerCandleReader) -> None:
        self._configured = frozenset(config.enabled_streams)
        self._reader = reader

    def execute(self, request: CandleRangeRequest) -> CandleRangeResult:
        try:
            stream = StreamKey(InstrumentKey(request.ticker), request.timeframe)
        except ValueError as exc:
            raise ConfiguredStreamNotFound(str(exc)) from exc
        if stream not in self._configured:
            raise ConfiguredStreamNotFound(stream.canonical_id)

        step_ms = get_timeframe(stream.timeframe).duration_ms
        validate_requested_range(request.from_ms, request.to_ms, step_ms)
        snapshot = self._reader.read_snapshot(
            stream,
            start_time_ms=request.from_ms,
            end_time_ms=request.to_ms,
        )
        state = snapshot.state
        if state.state is not StreamLifecycleState.READY:
            raise StreamNotReady(f"{stream.canonical_id} is {state.state.value}")
        earliest = state.earliest_available_open_time_ms
        latest = state.latest_committed_open_time_ms
        if earliest is None or latest is None:
            raise StreamNotReady(f"{stream.canonical_id} has no proven available window")
        available_to = latest + step_ms
        validate_available_range(
            request.from_ms,
            request.to_ms,
            available_from_ms=earliest,
            available_to_ms=available_to,
        )
        candles = snapshot.candles
        validate_complete_grid(
            candles,
            from_ms=request.from_ms,
            to_ms=request.to_ms,
            step_ms=step_ms,
        )
        return CandleRangeResult(stream, request.from_ms, request.to_ms, candles)
