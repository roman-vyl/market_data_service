from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from market_data_service.adapters.sqlite import (
    SqliteUnitOfWork,
    initialize_database,
    register_stream,
)
from market_data_service.adapters.sqlite.consumer_candle_reader import SqliteConsumerCandleReader
from market_data_service.application.consumer_read import CandleRangeRequest, GetCandleRange
from market_data_service.application.ingest import IngestObservedCandle
from market_data_service.config.markets import MarketSourceConfig, ValidatedMarketConfig
from market_data_service.domain.candles import ObservationSource, ObservedCandle
from market_data_service.domain.identity import InstrumentKey, StreamKey
from market_data_service.domain.instruments import HistoryPolicy, InstrumentCoverage
from market_data_service.domain.stream_state import StreamLifecycleState


def test_sqlite_reader_reuses_canonical_range_path(tmp_path: Path) -> None:
    stream = StreamKey(InstrumentKey("BTCUSDT.P"), "1m")
    database = tmp_path / "market.sqlite3"
    initialize_database(database)
    register_stream(database, stream, exchange_symbol="BTCUSDT", now_ms=1)
    ingest = IngestObservedCandle(lambda: SqliteUnitOfWork(database))
    for open_time_ms in (0, 60_000):
        ingest.execute(
            ObservedCandle(
                stream=stream,
                open_time_ms=open_time_ms,
                close_time_ms=open_time_ms + 59_999,
                open="1.2300",
                high="2",
                low="1",
                close="1.5",
                volume="10.500",
                confirmed=True,
                observed_at_ms=open_time_ms + 60_000,
                source=ObservationSource.BYBIT_REST,
            ),
            committed_at_ms=open_time_ms + 60_001,
        )
    with SqliteUnitOfWork(database) as unit_of_work:
        state = unit_of_work.get_stream_state(stream)
        unit_of_work.save_stream_state(
            replace(
                state,
                state=StreamLifecycleState.READY,
                earliest_available_open_time_ms=0,
                latest_committed_open_time_ms=60_000,
            )
        )
        unit_of_work.commit()

    config = ValidatedMarketConfig(
        1,
        MarketSourceConfig("bybit", "linear"),
        (
            InstrumentCoverage(
                stream.instrument,
                "BTCUSDT",
                True,
                ("1m",),
                HistoryPolicy.FULL_AVAILABLE,
            ),
        ),
    )
    result = GetCandleRange(config, SqliteConsumerCandleReader(database)).execute(
        CandleRangeRequest("BTCUSDT.P", "1m", 0, 120_000)
    )
    assert [c.open_time_ms for c in result.candles] == [0, 60_000]
    assert result.candles[0].ohlcv_text == ("1.23", "2", "1", "1.5", "10.5")
