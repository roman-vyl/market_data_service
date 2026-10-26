"""Open interest as a nullable candle field (mds-candle-open-interest-field-v1)."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from market_data_service.adapters.http.consumer_read.openapi_schemas import candle_response_schema
from market_data_service.adapters.http.consumer_read.serialization import serialize_result
from market_data_service.adapters.sqlite import (
    SqliteUnitOfWork,
    initialize_database,
    register_stream,
)
from market_data_service.adapters.sqlite.consumer_candle_reader import SqliteConsumerCandleReader
from market_data_service.application.consumer_read import (
    CandleRangeRequest,
    GetCandleRange,
    GetHistoricalCandleRange,
    HistoricalCandleRangeRequest,
)
from market_data_service.application.ingest import IngestObservedCandle
from market_data_service.config.markets import MarketSourceConfig, ValidatedMarketConfig
from market_data_service.domain.candles import ObservationSource, ObservedCandle
from market_data_service.domain.identity import InstrumentKey, StreamKey
from market_data_service.domain.instruments import HistoryPolicy, InstrumentCoverage
from market_data_service.domain.open_interest import CanonicalOpenInterest
from market_data_service.domain.stream_state import StreamLifecycleState
from market_data_service.domain.timeframes import get_timeframe

FLAGGED = InstrumentKey("BTCUSDT.P")
PLAIN = InstrumentKey("XRPUSDT.P")
OI_STREAM = StreamKey(FLAGGED, "5m")
HOUR_STREAM = StreamKey(FLAGGED, "1h")
PLAIN_STREAM = StreamKey(PLAIN, "5m")
STEP = 300_000
HOUR = 3_600_000

CONFIG = ValidatedMarketConfig(
    1,
    MarketSourceConfig("bybit", "linear"),
    (
        InstrumentCoverage(
            FLAGGED, "BTCUSDT", True, ("5m", "1h"), HistoryPolicy.FULL_AVAILABLE, True
        ),
        InstrumentCoverage(PLAIN, "XRPUSDT", True, ("5m",), HistoryPolicy.FULL_AVAILABLE),
    ),
)


def _database(tmp_path: Path) -> Path:
    tmp_path.mkdir(parents=True, exist_ok=True)
    database = tmp_path / "market.sqlite3"
    initialize_database(database)
    for stream in (OI_STREAM, HOUR_STREAM, PLAIN_STREAM):
        register_stream(
            database,
            stream,
            exchange_symbol=stream.instrument.ticker.removesuffix(".P"),
            now_ms=1,
        )
    return database


def _candles(database: Path, stream: StreamKey, count: int) -> None:
    step = get_timeframe(stream.timeframe).duration_ms
    ingest = IngestObservedCandle(lambda: SqliteUnitOfWork(database))
    for index in range(count):
        open_time_ms = index * step
        ingest.execute(
            ObservedCandle(
                stream=stream,
                open_time_ms=open_time_ms,
                close_time_ms=open_time_ms + step - 1,
                open="1",
                high="2",
                low="1",
                close="1.5",
                volume="10",
                confirmed=True,
                observed_at_ms=open_time_ms + step,
                source=ObservationSource.BYBIT_REST,
            ),
            committed_at_ms=open_time_ms + step + 1,
        )
    with SqliteUnitOfWork(database) as unit_of_work:
        state = unit_of_work.get_stream_state(stream)
        unit_of_work.save_stream_state(
            replace(
                state,
                state=StreamLifecycleState.READY,
                earliest_available_open_time_ms=0,
                latest_committed_open_time_ms=(count - 1) * step,
            )
        )
        unit_of_work.commit()


def _open_interest(database: Path, stream: StreamKey, values: dict[int, str]) -> None:
    with SqliteUnitOfWork(database) as unit_of_work:
        for open_time_ms, value in values.items():
            unit_of_work.save_open_interest(
                CanonicalOpenInterest(stream, open_time_ms, value, open_time_ms + 1)
            )
        unit_of_work.commit()


def _read(database: Path, stream: StreamKey, to_ms: int):  # type: ignore[no-untyped-def]
    return GetCandleRange(CONFIG, SqliteConsumerCandleReader(database)).execute(
        CandleRangeRequest(stream.instrument.ticker, stream.timeframe, 0, to_ms)
    )


def test_each_candle_gets_the_open_interest_of_its_own_bar_or_null(tmp_path: Path) -> None:
    database = _database(tmp_path)
    _candles(database, OI_STREAM, 5)
    # Bars 0 and 1 precede the OI history; bar 3 has no stored point between neighbours.
    _open_interest(database, OI_STREAM, {2 * STEP: "82345.67", 4 * STEP: "82400.1"})

    result = _read(database, OI_STREAM, 5 * STEP)

    assert result.open_interest == (None, None, "82345.67", None, "82400.1")
    payload = serialize_result(result)
    assert [candle["open_interest"] for candle in payload["candles"]] == [  # type: ignore[index]
        None,
        None,
        "82345.67",
        None,
        "82400.1",
    ]


def test_streams_without_open_interest_return_null(tmp_path: Path) -> None:
    database = _database(tmp_path)
    _candles(database, PLAIN_STREAM, 3)
    _candles(database, HOUR_STREAM, 2)
    _open_interest(database, OI_STREAM, {0: "1", HOUR: "2"})

    plain = _read(database, PLAIN_STREAM, 3 * STEP)
    hour = _read(database, HOUR_STREAM, 2 * HOUR)

    assert plain.open_interest == (None, None, None)
    assert hour.open_interest == (None, None)
    for result in (plain, hour):
        assert all(
            candle["open_interest"] is None
            for candle in serialize_result(result)["candles"]  # type: ignore[union-attr]
        )


def test_open_interest_does_not_change_the_hash_or_the_historical_read(tmp_path: Path) -> None:
    database = _database(tmp_path)
    _candles(database, OI_STREAM, 3)
    audited_hash = _read(database, OI_STREAM, 3 * STEP).market_data_hash

    _open_interest(database, OI_STREAM, {0: "10", STEP: "11", 2 * STEP: "12"})

    assert _read(database, OI_STREAM, 3 * STEP).market_data_hash == audited_hash
    historical = GetHistoricalCandleRange(CONFIG, SqliteConsumerCandleReader(database)).execute(
        HistoricalCandleRangeRequest("BTCUSDT.P", "5m", 0, 3 * STEP, audited_hash)
    )
    assert historical.market_data_hash == audited_hash
    assert historical.open_interest == ("10", "11", "12")


def test_openapi_candle_schema_declares_nullable_open_interest() -> None:
    schema = candle_response_schema()
    candle = schema["properties"]["candles"]["items"]  # type: ignore[index]
    assert "open_interest" in candle["required"]
    assert candle["properties"]["open_interest"]["type"] == ["string", "null"]
