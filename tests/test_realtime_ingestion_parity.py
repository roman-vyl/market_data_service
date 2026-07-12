from __future__ import annotations

from pathlib import Path

from market_data_service.adapters.sqlite import (
    SqliteUnitOfWork,
    initialize_database,
    register_stream,
)
from market_data_service.application.ingest import IngestObservedCandle
from market_data_service.application.realtime.events import CandleObserved
from market_data_service.application.realtime.handler import RealtimeCandleHandler
from market_data_service.application.realtime.outcomes import RealtimeIngestionClassification
from market_data_service.domain import InstrumentKey, StreamKey
from market_data_service.domain.candles import ObservationSource, ObservedCandle


def _candle(
    stream: StreamKey,
    *,
    source: ObservationSource,
    close: str = "101",
    high: str = "102",
) -> ObservedCandle:
    return ObservedCandle(
        stream=stream,
        open_time_ms=0,
        close_time_ms=59_999,
        open="100",
        high=high,
        low="99",
        close=close,
        volume="10",
        confirmed=True,
        observed_at_ms=60_000,
        source=source,
    )


def _setup(path: Path) -> tuple[StreamKey, IngestObservedCandle, RealtimeCandleHandler]:
    stream = StreamKey(InstrumentKey("BTCUSDT.P"), "1m")
    initialize_database(path)
    register_stream(path, stream, exchange_symbol="BTCUSDT", now_ms=1)
    ingestion = IngestObservedCandle(lambda: SqliteUnitOfWork(path))
    return stream, ingestion, RealtimeCandleHandler(ingestion, lambda: 70_000)


def test_rest_and_websocket_share_duplicate_and_correction_classification(
    tmp_path: Path,
) -> None:
    path = tmp_path / "market.sqlite3"
    stream, ingestion, handler = _setup(path)

    committed = ingestion.execute(
        _candle(stream, source=ObservationSource.BYBIT_REST),
        committed_at_ms=61_000,
    )
    duplicate = handler.handle(
        CandleObserved(stream, _candle(stream, source=ObservationSource.BYBIT_WEBSOCKET))
    )
    websocket_correction = handler.handle(
        CandleObserved(
            stream,
            _candle(
                stream,
                source=ObservationSource.BYBIT_WEBSOCKET,
                close="102",
                high="103",
            ),
        )
    )

    assert committed.classification.value == "committed"
    assert duplicate is not None
    assert duplicate.classification is RealtimeIngestionClassification.DUPLICATE
    assert websocket_correction is not None
    assert websocket_correction.classification is RealtimeIngestionClassification.CORRECTED

    with SqliteUnitOfWork(path) as unit_of_work:
        stored_before_rest_correction = unit_of_work.get_candle(stream, 0)
    assert stored_before_rest_correction is not None
    assert stored_before_rest_correction.ohlcv_text[3] == "101"

    rest_correction = ingestion.execute(
        _candle(
            stream,
            source=ObservationSource.BYBIT_REST,
            close="102",
            high="103",
        ),
        committed_at_ms=80_000,
    )
    assert rest_correction.classification.value == "corrected"
    with SqliteUnitOfWork(path) as unit_of_work:
        stored_after_rest_correction = unit_of_work.get_candle(stream, 0)
    assert stored_after_rest_correction is not None
    assert stored_after_rest_correction.ohlcv_text[3] == "102"


def test_invalid_confirmed_websocket_candle_is_rejected_and_quarantined(
    tmp_path: Path,
) -> None:
    path = tmp_path / "market.sqlite3"
    stream, _, handler = _setup(path)

    outcome = handler.handle(
        CandleObserved(
            stream,
            _candle(
                stream,
                source=ObservationSource.BYBIT_WEBSOCKET,
                close="105",
                high="102",
            ),
        )
    )

    assert outcome is not None
    assert outcome.classification is RealtimeIngestionClassification.REJECTED
    assert "invalid_ohlc" in outcome.issue_codes
    with SqliteUnitOfWork(path) as unit_of_work:
        assert unit_of_work.get_candle(stream, 0) is None
