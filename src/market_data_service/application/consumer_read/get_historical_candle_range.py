"""Read one audited historical range without runtime readiness admission."""

from __future__ import annotations

import re

from market_data_service.application.consumer_read.errors import (
    ConfiguredStreamNotFound,
    CoverageStale,
    InvalidRange,
)
from market_data_service.application.consumer_read.models import (
    CandleRangeResult,
    HistoricalCandleRangeRequest,
)
from market_data_service.application.consumer_read.provenance import canonical_market_data_hash
from market_data_service.application.consumer_read.validation import (
    validate_complete_grid,
    validate_requested_range,
)
from market_data_service.config import ValidatedMarketConfig
from market_data_service.domain.identity import InstrumentKey, StreamKey
from market_data_service.domain.timeframes import get_timeframe
from market_data_service.ports.consumer_read import ConsumerCandleReader

_CANONICAL_HASH_FORMAT = re.compile(r"^[0-9a-f]{64}$")


class GetHistoricalCandleRange:
    def __init__(self, config: ValidatedMarketConfig, reader: ConsumerCandleReader) -> None:
        self._configured = frozenset(config.enabled_streams)
        self._reader = reader

    def execute(self, request: HistoricalCandleRangeRequest) -> CandleRangeResult:
        try:
            stream = StreamKey(InstrumentKey(request.ticker), request.timeframe)
        except ValueError as exc:
            raise ConfiguredStreamNotFound(str(exc)) from exc
        if stream not in self._configured:
            raise ConfiguredStreamNotFound(stream.canonical_id)
        if not _CANONICAL_HASH_FORMAT.match(request.expected_market_data_hash):
            raise InvalidRange(
                "expected_market_data_hash must be a 64-character lowercase hex digest"
            )

        step_ms = get_timeframe(stream.timeframe).duration_ms
        validate_requested_range(request.from_ms, request.to_ms, step_ms)
        snapshot = self._reader.read_snapshot(
            stream,
            start_time_ms=request.from_ms,
            end_time_ms=request.to_ms,
        )
        candles = snapshot.candles
        validate_complete_grid(
            candles,
            from_ms=request.from_ms,
            to_ms=request.to_ms,
            step_ms=step_ms,
        )
        actual_hash = canonical_market_data_hash(
            stream=stream,
            from_ms=request.from_ms,
            to_ms=request.to_ms,
            candles=candles,
        )
        if actual_hash != request.expected_market_data_hash:
            raise CoverageStale(
                "historical candle range no longer matches the audited market_data_hash"
            )
        return CandleRangeResult(
            stream=stream,
            from_ms=request.from_ms,
            to_ms=request.to_ms,
            market_data_hash=actual_hash,
            candles=candles,
        )
