from __future__ import annotations

import sqlite3
from dataclasses import dataclass, replace
from pathlib import Path

from market_data_service.adapters.bybit import BybitHttpError
from market_data_service.adapters.sqlite import (
    SqliteUnitOfWork,
    initialize_database,
    register_stream,
)
from market_data_service.application.audit_continuity import (
    AuditStreamContinuity,
    open_interest_open_times,
)
from market_data_service.application.import_open_interest import ImportOpenInterestWindow
from market_data_service.application.lower_bound import ResolveHistoricalLowerBound
from market_data_service.application.lower_bound_probe import OpenInterestLowerBoundProbe
from market_data_service.application.open_interest_track_state import OpenInterestTrackState
from market_data_service.application.repair_gaps import RepairStreamGaps
from market_data_service.domain import (
    CanonicalCandle,
    InstrumentKey,
    InstrumentMetadata,
    ObservationSource,
    StreamKey,
    StreamLifecycleState,
    TimeWindow,
)
from market_data_service.domain.open_interest import OPEN_INTEREST_TRACK, ObservedOpenInterest
from market_data_service.runtime.open_interest_reconciliation import OpenInterestTrackReconciler
from market_data_service.runtime.startup_types import StartupClassification, StartupStreamOutcome

STEP = 300_000
T0 = 1_600_000_200_000 - (1_600_000_200_000 % STEP)
STREAM = StreamKey(InstrumentKey("BTCUSDT.P"), "5m")


@dataclass
class FakeClock:
    value: int

    def now_ms(self) -> int:
        current = self.value
        self.value += 1
        return current


class FakeMetadata:
    def get_launch_time_ms(self, instrument: InstrumentKey) -> int:
        return T0

    def get_instrument_specification(self, instrument: InstrumentKey) -> object:
        raise AssertionError("not used")


class FakeOpenInterestSource:
    def __init__(self, points: dict[int, str]) -> None:
        self.points = points
        self.calls: list[TimeWindow] = []
        self.failure: Exception | None = None

    def fetch_open_interest(
        self,
        stream: StreamKey,
        window: TimeWindow,
        *,
        observed_at_ms: int,
    ) -> tuple[ObservedOpenInterest, ...]:
        self.calls.append(window)
        if self.failure is not None:
            raise self.failure
        return tuple(
            ObservedOpenInterest(stream, ts, value, observed_at_ms)
            for ts, value in sorted(self.points.items())
            if window.contains(ts)
        )


def _bar(index: int) -> int:
    return T0 + index * STEP


def _prepare(path: Path, *, candle_bars: range) -> None:
    initialize_database(path)
    register_stream(path, STREAM, exchange_symbol="BTCUSDT", now_ms=1)
    with SqliteUnitOfWork(path) as unit_of_work:
        unit_of_work.save_instrument_metadata(
            InstrumentMetadata(STREAM.instrument, "BTCUSDT", launch_time_ms=T0, fetched_at_ms=1)
        )
        for index in candle_bars:
            unit_of_work.insert_candle(
                CanonicalCandle(
                    stream=STREAM,
                    open_time_ms=_bar(index),
                    close_time_ms=_bar(index) + STEP - 1,
                    open="1",
                    high="2",
                    low="1",
                    close="2",
                    volume="3",
                    source=ObservationSource.BYBIT_REST,
                    committed_at_ms=1,
                )
            )
        snapshot = unit_of_work.get_stream_state(STREAM)
        unit_of_work.save_stream_state(
            replace(
                snapshot,
                state=StreamLifecycleState.READY,
                earliest_available_open_time_ms=_bar(candle_bars.start) if candle_bars else None,
            )
        )
        unit_of_work.commit()


def _reconciler(
    path: Path,
    source: FakeOpenInterestSource,
    *,
    now_bar: int,
    windows: int = 2,
) -> OpenInterestTrackReconciler:
    def factory() -> SqliteUnitOfWork:
        return SqliteUnitOfWork(path)

    clock = FakeClock(_bar(now_bar) + 1_000)
    state = OpenInterestTrackState(factory, clock)
    return OpenInterestTrackReconciler(
        lower_bound=ResolveHistoricalLowerBound(
            FakeMetadata(),  # type: ignore[arg-type]
            object(),  # type: ignore[arg-type]
            factory,
            clock,
            probe=OpenInterestLowerBoundProbe(source, max_points_per_probe=200),
            progress=state,
        ),
        repair=RepairStreamGaps(
            AuditStreamContinuity(factory, open_interest_open_times),
            ImportOpenInterestWindow(source, factory, clock),
            factory,
            clock,
            max_candles_per_window=200,
            state=state,
        ),
        state=state,
        now_ms=clock.now_ms,
        discovery_windows_per_pass=windows,
        repair_windows_per_pass=windows,
    )


def _run_cycle(
    reconciler: OpenInterestTrackReconciler, max_passes: int = 50
) -> StartupStreamOutcome:
    window = None
    for _ in range(max_passes):
        outcome = reconciler.execute_stream(STREAM, window)
        if outcome.classification is not StartupClassification.INCOMPLETE:
            return outcome
        window = outcome.window or window
    raise AssertionError("cycle did not finish")


def _snapshot(path: Path) -> tuple[object, ...]:
    with SqliteUnitOfWork(path) as unit_of_work:
        return (unit_of_work.get_stream_state(STREAM),)


def _rows(path: Path, sql: str) -> list[tuple[object, ...]]:
    connection = sqlite3.connect(path)
    try:
        return connection.execute(sql).fetchall()
    finally:
        connection.close()


def _progress(path: Path):  # type: ignore[no-untyped-def]
    with SqliteUnitOfWork(path) as unit_of_work:
        return unit_of_work.get_track_progress(STREAM, OPEN_INTEREST_TRACK)


def test_candles_before_open_interest_are_not_gaps_and_stay(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    _prepare(path, candle_bars=range(0, 600))
    candles_before = _rows(path, "SELECT * FROM candles ORDER BY open_time_ms")
    state_before = _snapshot(path)
    source = FakeOpenInterestSource({_bar(i): f"{1000 + i}.5" for i in range(450, 600)})

    outcome = _run_cycle(_reconciler(path, source, now_bar=600))

    assert outcome.classification is StartupClassification.CONNECTING
    progress = _progress(path)
    assert progress.earliest_available_open_time_ms == _bar(450)
    assert progress.discovery_next_open_time_ms is None
    assert progress.latest_committed_open_time_ms == _bar(599)
    assert progress.last_audit_gap_count == 0
    assert _rows(path, "SELECT COUNT(*) FROM open_interest") == [(150,)]
    assert _rows(path, "SELECT MIN(open_time_ms) FROM open_interest") == [(_bar(450),)]
    assert _rows(path, "SELECT * FROM candles ORDER BY open_time_ms") == candles_before
    assert _snapshot(path) == state_before
    assert _rows(path, "SELECT COUNT(*) FROM quarantine") == [(0,)]


def test_open_interest_before_candles_is_complete_on_its_own(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    _prepare(path, candle_bars=range(300, 600))
    candles_before = _rows(path, "SELECT * FROM candles")
    source = FakeOpenInterestSource({_bar(i): "7" for i in range(0, 600)})

    outcome = _run_cycle(_reconciler(path, source, now_bar=600))

    assert outcome.classification is StartupClassification.CONNECTING
    assert _progress(path).earliest_available_open_time_ms == _bar(0)
    assert _rows(path, "SELECT COUNT(*) FROM open_interest") == [(600,)]
    assert _rows(path, "SELECT * FROM candles") == candles_before


def test_restart_mid_history_repairs_only_remaining_gaps(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    _prepare(path, candle_bars=range(0, 0))
    source = FakeOpenInterestSource({_bar(i): str(i) for i in range(0, 1000)})
    first = _reconciler(path, source, now_bar=1000, windows=1)
    lower = first.execute_stream(STREAM)
    assert lower.classification is StartupClassification.INCOMPLETE
    partial = first.execute_stream(STREAM, lower.window)
    assert partial.classification is StartupClassification.INCOMPLETE
    stored_before_restart = _rows(path, "SELECT COUNT(*) FROM open_interest")[0][0]
    assert 0 < stored_before_restart < 1000

    source.calls.clear()
    outcome = _run_cycle(_reconciler(path, source, now_bar=1000, windows=1))

    assert outcome.classification is StartupClassification.CONNECTING
    assert _rows(path, "SELECT COUNT(*) FROM open_interest") == [(1000,)]
    fetched = sum(window.duration_ms // STEP for window in source.calls)
    assert fetched == 1000 - stored_before_restart


def test_internal_gap_is_found_and_repaired_in_the_next_cycle(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    _prepare(path, candle_bars=range(0, 0))
    source = FakeOpenInterestSource({_bar(i): str(i) for i in range(0, 400)})
    _run_cycle(_reconciler(path, source, now_bar=400))
    connection = sqlite3.connect(path)
    connection.execute(
        "DELETE FROM open_interest WHERE open_time_ms BETWEEN ? AND ?", (_bar(100), _bar(109))
    )
    connection.commit()
    connection.close()
    source.calls.clear()

    outcome = _run_cycle(_reconciler(path, source, now_bar=400))

    assert outcome.classification is StartupClassification.CONNECTING
    assert source.calls == [TimeWindow(_bar(100), _bar(110))]
    assert _rows(path, "SELECT COUNT(*) FROM open_interest") == [(400,)]


def test_window_import_classifies_and_quarantines(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    _prepare(path, candle_bars=range(0, 0))
    source = FakeOpenInterestSource({_bar(i): "5" for i in range(0, 4)})

    def factory() -> SqliteUnitOfWork:
        return SqliteUnitOfWork(path)

    importer = ImportOpenInterestWindow(source, factory, FakeClock(_bar(10)))
    first = importer.execute(STREAM, TimeWindow(_bar(0), _bar(4)))
    source.points[_bar(1)] = "6.50"
    source.points[_bar(2)] = "-1"
    second = importer.execute(STREAM, TimeWindow(_bar(0), _bar(4)))

    assert (first.committed, first.duplicates, first.corrected, first.rejected) == (4, 0, 0, 0)
    assert (second.committed, second.duplicates, second.corrected, second.rejected) == (0, 2, 1, 1)
    assert _rows(
        path, f"SELECT open_interest_value FROM open_interest WHERE open_time_ms = {_bar(1)}"
    ) == [("6.5",)]
    assert _rows(
        path, f"SELECT open_interest_value FROM open_interest WHERE open_time_ms = {_bar(2)}"
    ) == [("5",)]
    assert _rows(path, "SELECT reason_code FROM quarantine") == [("invalid_open_interest",)]


def test_invalid_point_keeps_the_cycle_incomplete(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    _prepare(path, candle_bars=range(0, 0))
    source = FakeOpenInterestSource({_bar(i): ("-1" if i == 5 else "5") for i in range(0, 10)})
    reconciler = _reconciler(path, source, now_bar=10)

    lower = reconciler.execute_stream(STREAM)
    outcome = reconciler.execute_stream(STREAM, lower.window)

    assert outcome.classification is StartupClassification.INCOMPLETE
    assert _progress(path).last_audit_gap_count == 1
    assert _rows(path, "SELECT COUNT(*) FROM open_interest") == [(9,)]


def test_source_failures_stay_inside_the_track(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    _prepare(path, candle_bars=range(0, 10))
    state_before = _snapshot(path)
    source = FakeOpenInterestSource({_bar(i): "5" for i in range(0, 10)})

    source.failure = BybitHttpError("timeout", status_code=None)
    recoverable = _reconciler(path, source, now_bar=10).execute_stream(STREAM)
    source.failure = ValueError("broken payload")
    fatal = _reconciler(path, source, now_bar=10).execute_stream(STREAM)

    assert recoverable.classification is StartupClassification.RECOVERABLE_FAILURE
    assert fatal.classification is StartupClassification.FATAL_FAILURE
    assert _progress(path).last_error_code == "ValueError"
    assert _snapshot(path) == state_before
