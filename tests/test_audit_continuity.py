from __future__ import annotations

from pathlib import Path

import pytest

from market_data_service.adapters.sqlite import (
    SqliteUnitOfWork,
    initialize_database,
    register_stream,
)
from market_data_service.application.audit_continuity import (
    AuditStreamContinuity,
    AuditStreamContinuityRequest,
    UnknownStreamError,
)
from market_data_service.application.ingest import IngestObservedCandle
from market_data_service.domain import (
    Gap,
    InstrumentKey,
    ObservationSource,
    ObservedCandle,
    StreamKey,
)


def _stream(ticker: str = "BTCUSDT.P") -> StreamKey:
    return StreamKey(InstrumentKey(ticker), "1m")


def _prepare(path: Path, *streams: StreamKey) -> None:
    initialize_database(path)
    for stream in streams:
        register_stream(
            path,
            stream,
            exchange_symbol=stream.instrument.ticker.removesuffix(".P"),
            now_ms=1,
        )


def _insert(path: Path, stream: StreamKey, open_times_ms: tuple[int, ...]) -> None:
    ingest = IngestObservedCandle(lambda: SqliteUnitOfWork(path))
    for open_time_ms in open_times_ms:
        ingest.execute(
            ObservedCandle(
                stream=stream,
                open_time_ms=open_time_ms,
                close_time_ms=open_time_ms + 59_999,
                open="100",
                high="102",
                low="99",
                close="101",
                volume="1.5",
                confirmed=True,
                observed_at_ms=open_time_ms + 60_000,
                source=ObservationSource.BYBIT_REST,
            ),
            committed_at_ms=open_time_ms + 60_001,
        )


def _audit(path: Path, stream: StreamKey, start_ms: int, end_ms: int):
    return AuditStreamContinuity(lambda: SqliteUnitOfWork(path)).execute(
        AuditStreamContinuityRequest(
            stream=stream,
            start_time_ms=start_ms,
            end_time_ms=end_ms,
        )
    )


def test_complete_history_is_continuous(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    _insert(path, stream, (0, 60_000, 120_000, 180_000))

    report = _audit(path, stream, 0, 240_000)

    assert report.is_continuous is True
    assert report.candle_count == 4
    assert report.gaps == ()


def test_missing_candle_reports_gap(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    _insert(path, stream, (0, 60_000, 180_000))

    report = _audit(path, stream, 0, 240_000)

    assert report.is_continuous is False
    assert report.gaps == (Gap(120_000, 180_000),)


def test_gap_at_beginning_is_reported(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    _insert(path, stream, (120_000, 180_000, 240_000))

    report = _audit(path, stream, 0, 300_000)

    assert report.is_continuous is False
    assert report.gaps == (Gap(0, 120_000),)


def test_gap_at_end_is_reported(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    _insert(path, stream, (0, 60_000))

    report = _audit(path, stream, 0, 300_000)

    assert report.is_continuous is False
    assert report.gaps == (Gap(120_000, 300_000),)


def test_multiple_gaps_are_reported(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    _insert(path, stream, (0, 120_000, 300_000))

    report = _audit(path, stream, 0, 360_000)

    assert report.is_continuous is False
    assert report.gaps == (
        Gap(60_000, 120_000),
        Gap(180_000, 300_000),
    )


def test_audit_uses_only_requested_bounded_range(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    _insert(path, stream, (0, 60_000, 120_000, 180_000, 240_000))

    report = _audit(path, stream, 60_000, 240_000)

    assert report.is_continuous is True
    assert report.candle_count == 3
    assert report.gaps == ()


def test_audit_is_isolated_per_stream(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    btc = _stream("BTCUSDT.P")
    eth = _stream("ETHUSDT.P")
    _prepare(path, btc, eth)
    _insert(path, btc, (0, 60_000, 120_000))
    _insert(path, eth, (0, 120_000))

    btc_report = _audit(path, btc, 0, 180_000)
    eth_report = _audit(path, eth, 0, 180_000)

    assert btc_report.is_continuous is True
    assert btc_report.gaps == ()
    assert eth_report.is_continuous is False
    assert eth_report.gaps == (Gap(60_000, 120_000),)


def test_empty_range_is_not_continuous(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)

    report = _audit(path, stream, 0, 180_000)

    assert report.is_continuous is False
    assert report.candle_count == 0
    assert report.gaps == (Gap(0, 180_000),)


def test_unknown_stream_raises_typed_application_error(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    initialize_database(path)
    stream = _stream()

    with pytest.raises(UnknownStreamError, match=stream.canonical_id):
        _audit(path, stream, 0, 60_000)
