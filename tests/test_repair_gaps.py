from __future__ import annotations

import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pytest

from market_data_service.adapters.bybit import BybitHttpError
from market_data_service.adapters.sqlite import (
    SqliteUnitOfWork,
    initialize_database,
    register_stream,
)
from market_data_service.application.audit_continuity import AuditStreamContinuity
from market_data_service.application.import_window import ImportHistoricalWindow
from market_data_service.application.ingest import IngestObservedCandle
from market_data_service.application.repair_gaps import (
    RepairStatus,
    RepairStreamGaps,
    RepairStreamGapsRequest,
)
from market_data_service.application.use_cases import RepairStreamGaps as PublicRepairStreamGaps
from market_data_service.domain import (
    CanonicalCandle,
    Gap,
    InstrumentKey,
    InvalidStreamTransition,
    ObservationSource,
    ObservedCandle,
    StreamKey,
    StreamLifecycleState,
    TimeWindow,
    transition_stream_state,
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
        rows: dict[StreamKey, dict[int, ObservedCandle]],
        *,
        extra_rows: tuple[ObservedCandle, ...] = (),
        failure: Exception | None = None,
        fail_window_start_ms: int | None = None,
        before_return: Callable[[StreamKey, TimeWindow], None] | None = None,
    ) -> None:
        self.rows = rows
        self.extra_rows = extra_rows
        self.failure = failure
        self.fail_window_start_ms = fail_window_start_ms
        self.before_return = before_return
        self.calls: list[tuple[StreamKey, TimeWindow]] = []

    def fetch_closed_candles(
        self,
        stream: StreamKey,
        window: TimeWindow,
        *,
        observed_at_ms: int,
    ) -> tuple[ObservedCandle, ...]:
        self.calls.append((stream, window))
        if self.failure is not None and window.start_ms == self.fail_window_start_ms:
            raise self.failure
        if self.before_return is not None:
            self.before_return(stream, window)
        rows = tuple(
            row
            for open_time_ms, row in sorted(self.rows.get(stream, {}).items())
            if window.start_ms <= open_time_ms < window.end_ms
        )
        return rows + self.extra_rows


class WindowPlanningSpy:
    def __init__(self) -> None:
        self.yielded = 0

    def __call__(self, gap: Gap, *, step_ms: int, max_candles: int):
        cursor = gap.start_ms
        max_span_ms = step_ms * max_candles
        while cursor < gap.end_ms:
            self.yielded += 1
            end_ms = min(cursor + max_span_ms, gap.end_ms)
            yield TimeWindow(cursor, end_ms)
            cursor = end_ms


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

    def get_candle(self, stream: StreamKey, open_time_ms: int) -> CanonicalCandle | None:
        return self._inner.get_candle(stream, open_time_ms)

    def list_candles(
        self,
        stream: StreamKey,
        *,
        start_time_ms: int,
        end_time_ms: int,
    ) -> tuple[CanonicalCandle, ...]:
        return self._inner.list_candles(
            stream,
            start_time_ms=start_time_ms,
            end_time_ms=end_time_ms,
        )

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


def _stream(ticker: str = "BTCUSDT.P") -> StreamKey:
    return StreamKey(InstrumentKey(ticker), "1m")


def _candle(
    stream: StreamKey,
    open_time_ms: int,
    *,
    close: str = "101",
    source: ObservationSource = ObservationSource.BYBIT_REST,
) -> ObservedCandle:
    return ObservedCandle(
        stream=stream,
        open_time_ms=open_time_ms,
        close_time_ms=open_time_ms + 59_999,
        open="100",
        high="102",
        low="99",
        close=close,
        volume="1.5",
        confirmed=True,
        observed_at_ms=open_time_ms + 60_000,
        source=source,
    )


def _rows(stream: StreamKey, open_times_ms: tuple[int, ...]) -> dict[int, ObservedCandle]:
    return {open_time_ms: _candle(stream, open_time_ms) for open_time_ms in open_times_ms}


def _prepare(path: Path, *streams: StreamKey) -> None:
    initialize_database(path)
    for stream in streams:
        register_stream(
            path,
            stream,
            exchange_symbol=stream.instrument.ticker.removesuffix(".P"),
            now_ms=1,
        )
        _move_to_auditing(path, stream)


def _move_to_auditing(path: Path, stream: StreamKey) -> None:
    with SqliteUnitOfWork(path) as unit_of_work:
        snapshot = unit_of_work.get_stream_state(stream)
        snapshot = transition_stream_state(
            snapshot,
            StreamLifecycleState.BOOTSTRAPPING,
            changed_at_ms=2,
        )
        snapshot = transition_stream_state(
            snapshot,
            StreamLifecycleState.AUDITING,
            changed_at_ms=3,
        )
        unit_of_work.save_stream_state(snapshot)
        unit_of_work.commit()


def _move_to_repairing(path: Path, stream: StreamKey) -> None:
    with SqliteUnitOfWork(path) as unit_of_work:
        snapshot = unit_of_work.get_stream_state(stream)
        snapshot = transition_stream_state(
            snapshot,
            StreamLifecycleState.REPAIRING,
            changed_at_ms=4,
        )
        unit_of_work.save_stream_state(snapshot)
        unit_of_work.commit()


def _insert(path: Path, stream: StreamKey, open_times_ms: tuple[int, ...]) -> None:
    ingest = IngestObservedCandle(lambda: SqliteUnitOfWork(path))
    for open_time_ms in open_times_ms:
        ingest.execute(_candle(stream, open_time_ms), committed_at_ms=open_time_ms + 60_001)


def _repair(
    path: Path,
    source: FakeHistoricalSource,
    clock: FakeClock,
    *,
    unit_of_work_factory: Callable[[], CanonicalStorageUnitOfWork] | None = None,
    max_candles_per_window: int = 2,
) -> RepairStreamGaps:
    if unit_of_work_factory is None:
        def unit_of_work_factory() -> CanonicalStorageUnitOfWork:
            return SqliteUnitOfWork(path)

    auditor = AuditStreamContinuity(unit_of_work_factory)
    importer = ImportHistoricalWindow(source, unit_of_work_factory, clock)
    return RepairStreamGaps(
        auditor,
        importer,
        unit_of_work_factory,
        clock,
        max_candles_per_window=max_candles_per_window,
    )


def _execute(
    repair: RepairStreamGaps,
    stream: StreamKey,
    *,
    start: int = 0,
    end: int = 300_000,
    max_windows: int = 10,
):
    return repair.execute(RepairStreamGapsRequest(stream, start, end, max_windows))


def _open_times(path: Path, stream: StreamKey) -> tuple[int, ...]:
    with SqliteUnitOfWork(path) as unit_of_work:
        return tuple(
            candle.open_time_ms
            for candle in unit_of_work.list_candles(
                stream,
                start_time_ms=0,
                end_time_ms=1_000_000,
            )
        )


def _state(path: Path, stream: StreamKey) -> tuple[str, str | None]:
    with SqliteUnitOfWork(path) as unit_of_work:
        snapshot = unit_of_work.get_stream_state(stream)
    return snapshot.state.value, snapshot.last_error_code


def _quarantine_reasons(path: Path, stream: StreamKey) -> tuple[str, ...]:
    connection = sqlite3.connect(path)
    try:
        return tuple(
            row[0]
            for row in connection.execute(
                """
                SELECT q.reason_code
                FROM quarantine q
                JOIN streams s ON s.id = q.stream_id
                JOIN instruments i ON i.id = s.instrument_id
                WHERE i.ticker = ? AND s.timeframe = ?
                ORDER BY q.id
                """,
                (stream.instrument.ticker, stream.timeframe),
            )
        )
    finally:
        connection.close()


def test_no_gaps_does_not_fetch(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    _insert(path, stream, (0, 60_000, 120_000))
    source = FakeHistoricalSource({})

    result = _execute(_repair(path, source, FakeClock()), stream, end=180_000)

    assert result.status is RepairStatus.COMPLETE
    assert source.calls == []
    assert result.pre_repair_audit.gaps == ()
    assert _state(path, stream)[0] == StreamLifecycleState.AUDITING.value


def test_public_repair_use_case_import_is_production_workflow() -> None:
    assert PublicRepairStreamGaps is RepairStreamGaps


def test_one_internal_gap_is_repaired_and_post_audit_is_continuous(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    _insert(path, stream, (0, 120_000, 180_000))
    source = FakeHistoricalSource({stream: _rows(stream, (60_000,))})

    result = _execute(_repair(path, source, FakeClock()), stream, end=240_000)

    assert result.status is RepairStatus.COMPLETE
    assert result.pre_repair_audit.gaps == (Gap(60_000, 120_000),)
    assert result.post_repair_audit is not None
    assert result.post_repair_audit.is_continuous
    assert _open_times(path, stream) == (0, 60_000, 120_000, 180_000)
    assert _state(path, stream)[0] == StreamLifecycleState.AUDITING.value


def test_leading_and_trailing_gaps_are_repaired(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    _insert(path, stream, (120_000,))
    source = FakeHistoricalSource({stream: _rows(stream, (0, 60_000, 180_000, 240_000))})

    result = _execute(_repair(path, source, FakeClock()), stream, end=300_000)

    assert result.status is RepairStatus.COMPLETE
    assert [call[1] for call in source.calls] == [
        TimeWindow(0, 120_000),
        TimeWindow(180_000, 300_000),
    ]
    assert _open_times(path, stream) == (0, 60_000, 120_000, 180_000, 240_000)


def test_multiple_gaps_are_split_into_bounded_windows(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    _insert(path, stream, (0, 300_000))
    source = FakeHistoricalSource({stream: _rows(stream, (60_000, 120_000, 180_000, 240_000))})

    result = _execute(
        _repair(path, source, FakeClock(), max_candles_per_window=2),
        stream,
        end=360_000,
    )

    assert result.status is RepairStatus.COMPLETE
    assert [call[1] for call in source.calls] == [
        TimeWindow(60_000, 180_000),
        TimeWindow(180_000, 300_000),
    ]


def test_empty_source_response_leaves_repair_incomplete(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    _insert(path, stream, (0, 120_000))
    source = FakeHistoricalSource({stream: {}})

    result = _execute(_repair(path, source, FakeClock()), stream, end=180_000)

    assert result.status is RepairStatus.INCOMPLETE
    assert result.complete is False
    assert result.post_repair_audit is not None
    assert result.post_repair_audit.gaps == (Gap(60_000, 120_000),)
    assert "repair_incomplete_gap" in _quarantine_reasons(path, stream)


def test_partial_source_response_is_ingested_but_remains_incomplete(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    _insert(path, stream, (0, 180_000))
    source = FakeHistoricalSource({stream: _rows(stream, (60_000,))})

    result = _execute(
        _repair(path, source, FakeClock(), max_candles_per_window=3),
        stream,
        end=240_000,
    )

    assert result.status is RepairStatus.INCOMPLETE
    assert result.complete is False
    assert result.window_results[0].committed == 1
    assert _open_times(path, stream) == (0, 60_000, 180_000)
    assert result.post_repair_audit is not None
    assert result.post_repair_audit.gaps == (Gap(120_000, 180_000),)


def test_window_budget_exhaustion_is_bounded_and_incomplete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    _insert(path, stream, (0, 300_000))
    source = FakeHistoricalSource({stream: _rows(stream, (60_000, 120_000))})
    spy = WindowPlanningSpy()
    monkeypatch.setattr("market_data_service.application.repair_gaps.iter_fetch_windows", spy)

    result = _execute(
        _repair(path, source, FakeClock(), max_candles_per_window=2),
        stream,
        end=360_000,
        max_windows=1,
    )

    assert result.status is RepairStatus.INCOMPLETE
    assert result.complete is False
    assert spy.yielded == 1
    assert len(source.calls) == 1
    assert source.calls[0][1] == TimeWindow(60_000, 180_000)
    assert _open_times(path, stream) == (0, 60_000, 120_000, 300_000)
    assert result.post_repair_audit is not None
    assert result.post_repair_audit.gaps == (Gap(180_000, 300_000),)


def test_unexpected_rows_are_quarantined_and_not_inserted(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    eth = _stream("ETHUSDT.P")
    _prepare(path, stream, eth)
    _insert(path, stream, (0, 120_000))
    source = FakeHistoricalSource(
        {stream: _rows(stream, (60_000,))},
        extra_rows=(_candle(stream, 240_000), _candle(eth, 60_000)),
    )

    result = _execute(_repair(path, source, FakeClock()), stream, end=180_000)

    assert result.status is RepairStatus.COMPLETE
    assert result.window_results[0].unexpected == 2
    assert _open_times(path, stream) == (0, 60_000, 120_000)
    assert _open_times(path, eth) == ()
    assert _quarantine_reasons(path, stream).count("unexpected_historical_candle") == 2


def test_duplicates_and_corrections_use_canonical_classification(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    clock = FakeClock()
    _prepare(path, stream)
    _insert(path, stream, (0, 120_000))
    inserted_by_hook = False

    def race_insert(_stream: StreamKey, _window: TimeWindow) -> None:
        nonlocal inserted_by_hook
        if not inserted_by_hook:
            IngestObservedCandle(lambda: SqliteUnitOfWork(path)).execute(
                _candle(stream, 60_000, close="100", source=ObservationSource.BYBIT_WEBSOCKET),
                committed_at_ms=clock.now_ms(),
            )
            inserted_by_hook = True

    source = FakeHistoricalSource(
        {stream: _rows(stream, (60_000,))},
        before_return=race_insert,
    )

    result = _execute(_repair(path, source, clock), stream, end=180_000)

    assert result.status is RepairStatus.COMPLETE
    assert result.window_results[0].corrected == 1
    with SqliteUnitOfWork(path) as unit_of_work:
        assert unit_of_work.get_candle(stream, 60_000).ohlcv_text[3] == "101"
    assert "candle_correction_detected" in _quarantine_reasons(path, stream)


def test_idempotent_rerun_after_success_is_noop(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    _insert(path, stream, (0, 120_000))
    clock = FakeClock()

    first_source = FakeHistoricalSource({stream: _rows(stream, (60_000,))})
    first = _execute(_repair(path, first_source, clock), stream, end=180_000)
    second_source = FakeHistoricalSource({stream: _rows(stream, (60_000,))})
    second = _execute(_repair(path, second_source, clock), stream, end=180_000)

    assert first.status is RepairStatus.COMPLETE
    assert second.status is RepairStatus.COMPLETE
    assert second_source.calls == []
    assert _open_times(path, stream) == (0, 60_000, 120_000)


def test_multi_stream_and_bounded_range_isolation(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    btc = _stream("BTCUSDT.P")
    eth = _stream("ETHUSDT.P")
    _prepare(path, btc, eth)
    _insert(path, btc, (0, 180_000))
    _insert(path, eth, (0, 120_000))
    source = FakeHistoricalSource({btc: _rows(btc, (60_000, 120_000))})

    result = _execute(_repair(path, source, FakeClock()), btc, start=60_000, end=180_000)

    assert result.status is RepairStatus.COMPLETE
    assert _open_times(path, btc) == (0, 60_000, 120_000, 180_000)
    assert _open_times(path, eth) == (0, 120_000)


def test_window_transaction_rolls_back_on_storage_failure(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    source = FakeHistoricalSource({stream: _rows(stream, (0, 60_000))})

    result = _execute(
        _repair(
            path,
            source,
            FakeClock(),
            unit_of_work_factory=lambda: FailingSecondInsertUnitOfWork(path),
        ),
        stream,
        end=120_000,
    )

    assert result.status is RepairStatus.FAILED
    assert _open_times(path, stream) == ()
    assert _state(path, stream) == (StreamLifecycleState.FAILED.value, "RuntimeError")


def test_recoverable_source_failure_marks_stream_degraded(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    source = FakeHistoricalSource(
        {stream: {}},
        failure=BybitHttpError("HTTP 429"),
        fail_window_start_ms=0,
    )

    result = _execute(_repair(path, source, FakeClock()), stream, end=60_000)

    assert result.status is RepairStatus.FAILED
    assert _state(path, stream) == (StreamLifecycleState.DEGRADED.value, "BybitHttpError")


def test_restart_from_repairing_reruns_audit_before_repair(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    _prepare(path, stream)
    _move_to_repairing(path, stream)
    _insert(path, stream, (0, 120_000))
    source = FakeHistoricalSource({stream: _rows(stream, (60_000,))})

    result = _execute(_repair(path, source, FakeClock()), stream, end=180_000)

    assert result.status is RepairStatus.COMPLETE
    assert _state(path, stream)[0] == StreamLifecycleState.AUDITING.value
    assert _open_times(path, stream) == (0, 60_000, 120_000)


def test_illegal_repair_state_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    initialize_database(path)
    register_stream(path, stream, exchange_symbol="BTCUSDT", now_ms=1)

    with pytest.raises(InvalidStreamTransition):
        _execute(_repair(path, FakeHistoricalSource({}), FakeClock()), stream, end=60_000)
