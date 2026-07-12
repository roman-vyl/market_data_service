from __future__ import annotations

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from market_data_service.adapters.bybit import BybitHttpError
from market_data_service.adapters.sqlite import (
    SqliteUnitOfWork,
    initialize_database,
    register_stream,
)
from market_data_service.application.backfill_stream import (
    BackfillStreamHistory,
    BackfillStreamRequest,
)
from market_data_service.application.import_window import ImportHistoricalWindow
from market_data_service.domain import (
    CanonicalCandle,
    InstrumentKey,
    ObservationSource,
    ObservedCandle,
    StreamKey,
    StreamLifecycleState,
    TimeWindow,
)
from market_data_service.ports.storage import CanonicalStorageUnitOfWork


@dataclass
class FakeClock:
    value: int = 1_000_000

    def now_ms(self) -> int:
        current = self.value
        self.value += 1
        return current


class FakeHistoricalSource:
    def __init__(
        self,
        *,
        fail_window_start_ms: int | None = None,
        failure: Exception | None = None,
    ) -> None:
        self.fail_window_start_ms = fail_window_start_ms
        self.failure = failure
        self.calls: list[tuple[StreamKey, TimeWindow]] = []

    def fetch_closed_candles(
        self,
        stream: StreamKey,
        window: TimeWindow,
        *,
        observed_at_ms: int,
    ) -> tuple[ObservedCandle, ...]:
        self.calls.append((stream, window))
        if window.start_ms == self.fail_window_start_ms:
            raise self.failure or RuntimeError("planned source failure")
        return tuple(
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
                observed_at_ms=observed_at_ms,
                source=ObservationSource.BYBIT_REST,
            )
            for open_time_ms in range(window.start_ms, window.end_ms, 60_000)
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


def _backfill(
    path: Path,
    source: FakeHistoricalSource,
    clock: FakeClock,
    *,
    max_candles_per_window: int = 2,
    unit_of_work_factory: Callable[[], CanonicalStorageUnitOfWork] | None = None,
) -> BackfillStreamHistory:
    if unit_of_work_factory is None:
        def unit_of_work_factory() -> CanonicalStorageUnitOfWork:
            return SqliteUnitOfWork(path)

    return BackfillStreamHistory(
        ImportHistoricalWindow(
            source,
            unit_of_work_factory,
            clock,
        ),
        unit_of_work_factory,
        clock,
        max_candles_per_window=max_candles_per_window,
    )


def _count_candles(path: Path, stream: StreamKey) -> int:
    connection = sqlite3.connect(path)
    try:
        return int(
            connection.execute(
                """
                SELECT COUNT(*)
                FROM candles c
                JOIN streams s ON s.id = c.stream_id
                JOIN instruments i ON i.id = s.instrument_id
                WHERE i.ticker = ? AND s.timeframe = ?
                """,
                (stream.instrument.ticker, stream.timeframe),
            ).fetchone()[0]
        )
    finally:
        connection.close()


def _state(path: Path, stream: StreamKey) -> tuple[str, int | None, int | None]:
    with SqliteUnitOfWork(path) as unit_of_work:
        snapshot = unit_of_work.get_stream_state(stream)
    return (
        snapshot.state.value,
        snapshot.earliest_available_open_time_ms,
        snapshot.latest_committed_open_time_ms,
    )


class FailingSecondInsertUnitOfWork:
    def __init__(self, path: Path) -> None:
        self._inner = SqliteUnitOfWork(path)
        self._insert_count = 0

    def __enter__(self) -> FailingSecondInsertUnitOfWork:
        self._inner.__enter__()
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self._inner.__exit__(exc_type, exc, traceback)

    def stream_exists(self, stream: StreamKey) -> bool:
        return self._inner.stream_exists(stream)

    def get_instrument_metadata(self, instrument):
        return self._inner.get_instrument_metadata(instrument)

    def save_instrument_metadata(self, metadata) -> None:
        self._inner.save_instrument_metadata(metadata)

    def get_candle(self, stream: StreamKey, open_time_ms: int) -> CanonicalCandle | None:
        return self._inner.get_candle(stream, open_time_ms)

    def insert_candle(self, candle: CanonicalCandle) -> None:
        self._insert_count += 1
        if self._insert_count == 2:
            raise RuntimeError("planned storage failure")
        self._inner.insert_candle(candle)

    def replace_candle(self, candle: CanonicalCandle) -> None:
        self._inner.replace_candle(candle)

    def get_stream_state(self, stream: StreamKey):
        return self._inner.get_stream_state(stream)

    def save_stream_state(self, snapshot) -> None:
        self._inner.save_stream_state(snapshot)

    def record_quarantine(self, **kwargs) -> None:
        self._inner.record_quarantine(**kwargs)

    def commit(self) -> None:
        self._inner.commit()

    def rollback(self) -> None:
        self._inner.rollback()


def test_backfills_small_range_and_moves_to_auditing(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)

    result = _backfill(path, FakeHistoricalSource(), FakeClock()).execute(
        BackfillStreamRequest(stream, 0, 180_000, max_windows=2)
    )

    assert result.completed_windows == 2
    assert result.reached_end is True
    assert [item.committed for item in result.window_results] == [2, 1]
    assert _count_candles(path, stream) == 3
    assert _state(path, stream) == (StreamLifecycleState.AUDITING.value, None, 120_000)
    with SqliteUnitOfWork(path) as unit_of_work:
        assert unit_of_work.get_stream_state(stream).last_audit_at_ms is None


def test_backfill_stops_at_max_windows_without_unbounded_bootstrap(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    source = FakeHistoricalSource()
    _prepare(path, stream)

    result = _backfill(path, source, FakeClock()).execute(
        BackfillStreamRequest(stream, 0, 300_000, max_windows=1)
    )

    assert result.completed_windows == 1
    assert result.reached_end is False
    assert result.next_start_time_ms == 120_000
    assert _count_candles(path, stream) == 2
    assert _state(path, stream)[0] == StreamLifecycleState.BOOTSTRAPPING.value


def test_bounded_backfill_does_not_invent_historical_lower_bound(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)

    _backfill(path, FakeHistoricalSource(), FakeClock()).execute(
        BackfillStreamRequest(stream, 600_000, 720_000, max_windows=1)
    )

    assert _state(path, stream)[1] is None


def test_restart_resumes_from_latest_committed_open_time(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    clock = FakeClock()

    _backfill(path, FakeHistoricalSource(), clock).execute(
        BackfillStreamRequest(stream, 0, 300_000, max_windows=1)
    )
    source = FakeHistoricalSource()
    resumed = _backfill(path, source, clock).execute(
        BackfillStreamRequest(stream, 0, 300_000, max_windows=10)
    )

    assert source.calls[0][1] == TimeWindow(120_000, 240_000)
    assert resumed.reached_end is True
    assert _count_candles(path, stream) == 5


def test_repeated_completed_backfill_does_not_insert_more_candles(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    clock = FakeClock()

    first = _backfill(path, FakeHistoricalSource(), clock).execute(
        BackfillStreamRequest(stream, 0, 120_000, max_windows=1)
    )
    repeated = _backfill(path, FakeHistoricalSource(), clock).execute(
        BackfillStreamRequest(stream, 0, 120_000, max_windows=1)
    )

    assert first.window_results[0].committed == 2
    assert repeated.completed_windows == 0
    assert repeated.reached_end is True
    assert _count_candles(path, stream) == 2


def test_btc_and_eth_backfill_progress_is_independent(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    btc = _stream("BTCUSDT.P")
    eth = _stream("ETHUSDT.P")
    clock = FakeClock()
    _prepare(path, btc, eth)

    _backfill(path, FakeHistoricalSource(), clock).execute(
        BackfillStreamRequest(btc, 0, 120_000, max_windows=1)
    )

    assert _count_candles(path, btc) == 2
    assert _count_candles(path, eth) == 0
    assert _state(path, btc)[2] == 60_000
    assert _state(path, eth)[2] is None


def test_failed_window_preserves_previously_committed_windows(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    source = FakeHistoricalSource(fail_window_start_ms=120_000)

    result = _backfill(path, source, FakeClock()).execute(
        BackfillStreamRequest(stream, 0, 300_000, max_windows=3)
    )

    assert result.error_code == "RuntimeError"
    assert result.attempted_windows == 2
    assert result.completed_windows == 1
    assert result.next_start_time_ms == 120_000
    assert _count_candles(path, stream) == 2
    assert _state(path, stream)[0] == StreamLifecycleState.FAILED.value


def test_window_storage_failure_rolls_back_entire_window(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)

    result = _backfill(
        path,
        FakeHistoricalSource(),
        FakeClock(),
        unit_of_work_factory=lambda: FailingSecondInsertUnitOfWork(path),
    ).execute(BackfillStreamRequest(stream, 0, 120_000, max_windows=1))

    assert result.error_code == "RuntimeError"
    assert _count_candles(path, stream) == 0
    assert _state(path, stream)[2] is None


def test_transient_bybit_failure_marks_stream_degraded(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    source = FakeHistoricalSource(
        fail_window_start_ms=0,
        failure=BybitHttpError("HTTP 429 rate limit"),
    )

    result = _backfill(path, source, FakeClock()).execute(
        BackfillStreamRequest(stream, 0, 120_000, max_windows=1)
    )

    assert result.error_code == "BybitHttpError"
    assert _state(path, stream)[0] == StreamLifecycleState.DEGRADED.value
