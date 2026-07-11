from __future__ import annotations

import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from market_data_service.adapters.sqlite import (
    SqliteUnitOfWork,
    initialize_database,
    register_stream,
)
from market_data_service.application.ingest import IngestObservedCandle
from market_data_service.domain import (
    IngestionClassification,
    InstrumentKey,
    ObservationSource,
    ObservedCandle,
    StreamKey,
)


def _stream(ticker: str = "BTCUSDT.P") -> StreamKey:
    return StreamKey(InstrumentKey(ticker), "1m")


def _candle(
    stream: StreamKey,
    *,
    close: str = "101",
    source: ObservationSource = ObservationSource.BYBIT_REST,
) -> ObservedCandle:
    return ObservedCandle(
        stream=stream,
        open_time_ms=0,
        close_time_ms=59_999,
        open="100.0",
        high="102.000",
        low="99",
        close=close,
        volume="1.5000",
        confirmed=True,
        observed_at_ms=60_000,
        source=source,
    )


def _prepare(path: Path, *streams: StreamKey) -> None:
    initialize_database(path)
    for stream in streams:
        register_stream(
            path,
            stream,
            exchange_symbol=stream.instrument.ticker.removesuffix(".P"),
            now_ms=1,
        )


def test_insert_duplicate_and_restart_persistence(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    ingest = IngestObservedCandle(lambda: SqliteUnitOfWork(path))

    first = ingest.execute(_candle(stream), committed_at_ms=100)
    duplicate = ingest.execute(_candle(stream, close="101.000"), committed_at_ms=200)

    assert first.classification is IngestionClassification.COMMITTED
    assert duplicate.classification is IngestionClassification.DUPLICATE

    with SqliteUnitOfWork(path) as unit_of_work:
        candle = unit_of_work.get_candle(stream, 0)
        state = unit_of_work.get_stream_state(stream)
    assert candle is not None
    assert candle.ohlcv_text == ("100", "102", "99", "101", "1.5")
    assert state.latest_committed_open_time_ms == 0

    connection = sqlite3.connect(path)
    try:
        assert connection.execute("SELECT COUNT(*) FROM candles").fetchone()[0] == 1
    finally:
        connection.close()


def test_rest_correction_replaces_and_quarantines(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    ingest = IngestObservedCandle(lambda: SqliteUnitOfWork(path))
    ingest.execute(_candle(stream), committed_at_ms=100)

    result = ingest.execute(_candle(stream, close="101.5"), committed_at_ms=200)

    assert result.classification is IngestionClassification.CORRECTED
    with SqliteUnitOfWork(path) as unit_of_work:
        candle = unit_of_work.get_candle(stream, 0)
    assert candle is not None
    assert candle.ohlcv_text[3] == "101.5"

    connection = sqlite3.connect(path)
    try:
        row = connection.execute("SELECT reason_code FROM quarantine").fetchone()
        assert row == ("candle_correction_detected",)
    finally:
        connection.close()


def test_websocket_correction_is_quarantined_without_overwrite(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    ingest = IngestObservedCandle(lambda: SqliteUnitOfWork(path))
    ingest.execute(_candle(stream), committed_at_ms=100)

    result = ingest.execute(
        _candle(stream, close="101.5", source=ObservationSource.BYBIT_WEBSOCKET),
        committed_at_ms=200,
    )

    assert result.classification is IngestionClassification.CORRECTED
    with SqliteUnitOfWork(path) as unit_of_work:
        candle = unit_of_work.get_candle(stream, 0)
    assert candle is not None
    assert candle.ohlcv_text[3] == "101"


def test_invalid_and_unconfigured_observations_do_not_write(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    ingest = IngestObservedCandle(lambda: SqliteUnitOfWork(path))

    invalid = replace(_candle(stream), confirmed=False)
    unknown = _stream("ETHUSDT.P")

    invalid_result = ingest.execute(invalid, committed_at_ms=100)
    unknown_result = ingest.execute(_candle(unknown), committed_at_ms=100)

    assert invalid_result.classification is IngestionClassification.REJECTED_UNCONFIRMED
    assert unknown_result.classification is IngestionClassification.REJECTED_UNCONFIGURED

    connection = sqlite3.connect(path)
    try:
        row = connection.execute(
            "SELECT reason_code FROM quarantine ORDER BY id DESC LIMIT 1"
        ).fetchone()
        assert row == ("candle_validation_failed",)
    finally:
        connection.close()


def test_transaction_rolls_back_candle_and_state_together(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)

    with pytest.raises(RuntimeError), SqliteUnitOfWork(path) as unit_of_work:
        canonical = _candle(stream)
        from market_data_service.domain.candles import CanonicalCandle

        unit_of_work.insert_candle(
            CanonicalCandle.from_observation(canonical, committed_at_ms=100)
        )
        state = unit_of_work.get_stream_state(stream)
        unit_of_work.save_stream_state(
            replace(state, latest_committed_open_time_ms=0, updated_at_ms=100)
        )
        raise RuntimeError("force rollback")

    with SqliteUnitOfWork(path) as unit_of_work:
        assert unit_of_work.get_candle(stream, 0) is None
        assert unit_of_work.get_stream_state(stream).latest_committed_open_time_ms is None


def test_streams_are_isolated(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    btc = _stream("BTCUSDT.P")
    eth = _stream("ETHUSDT.P")
    _prepare(path, btc, eth)
    ingest = IngestObservedCandle(lambda: SqliteUnitOfWork(path))

    ingest.execute(_candle(btc), committed_at_ms=100)

    with SqliteUnitOfWork(path) as unit_of_work:
        assert unit_of_work.get_candle(btc, 0) is not None
        assert unit_of_work.get_candle(eth, 0) is None
        assert unit_of_work.get_stream_state(eth).latest_committed_open_time_ms is None
