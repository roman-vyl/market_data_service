from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest

from market_data_service.application.consumer_read import CandleRangeRequest, GetCandleRange
from market_data_service.application.consumer_read.errors import (
    ConfiguredStreamNotFound,
    ContinuityInvariantBroken,
    RangeNotAligned,
    RangeOutOfBounds,
    StreamNotReady,
)
from market_data_service.config.markets import MarketSourceConfig, ValidatedMarketConfig
from market_data_service.domain.candles import CanonicalCandle, ObservationSource
from market_data_service.domain.identity import InstrumentKey, StreamKey
from market_data_service.domain.instruments import HistoryPolicy, InstrumentCoverage
from market_data_service.domain.stream_state import StreamLifecycleState, StreamStateSnapshot
from market_data_service.ports.consumer_read import ConsumerReadSnapshot


STREAM = StreamKey(InstrumentKey("BTCUSDT.P"), "5m")
CONFIG = ValidatedMarketConfig(
    1,
    MarketSourceConfig("bybit", "linear"),
    (
        InstrumentCoverage(
            STREAM.instrument,
            "BTCUSDT",
            True,
            ("5m",),
            HistoryPolicy.FULL_AVAILABLE,
        ),
    ),
)


def candle(open_time_ms: int, value: str = "1.2300") -> CanonicalCandle:
    return CanonicalCandle(
        stream=STREAM,
        open_time_ms=open_time_ms,
        close_time_ms=open_time_ms + 299_999,
        open=Decimal(value),
        high=Decimal("2"),
        low=Decimal("1"),
        close=Decimal("1.5"),
        volume=Decimal("10.500"),
        source=ObservationSource.BYBIT_REST,
        committed_at_ms=1,
    )


class Reader:
    def __init__(self) -> None:
        self.state = StreamStateSnapshot(
            STREAM,
            StreamLifecycleState.READY,
            earliest_available_open_time_ms=0,
            latest_committed_open_time_ms=600_000,
        )
        self.candles = (candle(0), candle(300_000), candle(600_000))

    def read_snapshot(self, stream: StreamKey, *, start_time_ms: int, end_time_ms: int):
        assert stream == STREAM
        candles = tuple(
            c for c in self.candles if start_time_ms <= c.open_time_ms < end_time_ms
        )
        return ConsumerReadSnapshot(self.state, candles)


def test_reads_complete_ready_half_open_range() -> None:
    result = GetCandleRange(CONFIG, Reader()).execute(
        CandleRangeRequest("BTCUSDT.P", "5m", 0, 600_000)
    )
    assert [c.open_time_ms for c in result.candles] == [0, 300_000]


def test_rejects_non_ready_unknown_unaligned_and_out_of_bounds() -> None:
    reader = Reader()
    query = GetCandleRange(CONFIG, reader)
    reader.state = replace(reader.state, state=StreamLifecycleState.REPAIRING)
    with pytest.raises(StreamNotReady):
        query.execute(CandleRangeRequest("BTCUSDT.P", "5m", 0, 300_000))
    with pytest.raises(ConfiguredStreamNotFound):
        query.execute(CandleRangeRequest("ETHUSDT.P", "5m", 0, 300_000))
    reader.state = replace(reader.state, state=StreamLifecycleState.READY)
    with pytest.raises(RangeNotAligned):
        query.execute(CandleRangeRequest("BTCUSDT.P", "5m", 1, 300_000))
    with pytest.raises(RangeOutOfBounds):
        query.execute(CandleRangeRequest("BTCUSDT.P", "5m", 0, 1_200_000))


def test_ready_stream_refuses_gapped_result() -> None:
    reader = Reader()
    reader.candles = (candle(0),)
    with pytest.raises(ContinuityInvariantBroken):
        GetCandleRange(CONFIG, reader).execute(
            CandleRangeRequest("BTCUSDT.P", "5m", 0, 600_000)
        )
