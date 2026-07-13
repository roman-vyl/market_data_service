from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path

from market_data_service.adapters.sqlite import (
    SqliteUnitOfWork,
    initialize_database,
    register_stream,
)
from market_data_service.application.audit_continuity import AuditStreamContinuity
from market_data_service.application.import_window import ImportHistoricalWindow
from market_data_service.application.ingest import IngestObservedCandle
from market_data_service.application.lower_bound import HistoricalLowerBoundResult
from market_data_service.application.repair_gaps import RepairStreamGaps
from market_data_service.domain import InstrumentKey, StreamKey, TimeWindow
from market_data_service.domain.candles import ObservationSource, ObservedCandle
from market_data_service.runtime.historical_worker import HistoricalReconciliationWorker
from market_data_service.runtime.lifecycle import RuntimeLifecycleRecorder
from market_data_service.runtime.reconciliation import HistoricalStreamReconciler
from market_data_service.runtime.startup import StartupCoordinator
from market_data_service.runtime.startup_types import (
    StartupClassification,
    StartupStreamOutcome,
)
from market_data_service.runtime.status import RuntimeStatusStore


@dataclass
class Clock:
    value: int = 360_000

    def now_ms(self) -> int:
        self.value += 1
        return self.value


class FixedLowerBound:
    def __init__(self, start_ms: int) -> None:
        self.start_ms = start_ms

    def execute(self, stream: StreamKey, *, max_windows: int) -> HistoricalLowerBoundResult:
        return HistoricalLowerBoundResult(
            stream=stream,
            launch_time_ms=self.start_ms,
            search_start_time_ms=self.start_ms,
            earliest_available_open_time_ms=self.start_ms,
            metadata_cached=True,
            lower_bound_cached=True,
            resolved=True,
            discovery_windows_used=0,
        )


class HistoricalSource:
    def __init__(self, rows: dict[StreamKey, dict[int, ObservedCandle]]) -> None:
        self.rows = rows
        self.calls: list[tuple[StreamKey, TimeWindow]] = []

    def fetch_closed_candles(
        self,
        stream: StreamKey,
        window: TimeWindow,
        *,
        observed_at_ms: int,
    ) -> tuple[ObservedCandle, ...]:
        self.calls.append((stream, window))
        return tuple(
            candle
            for open_time_ms, candle in sorted(self.rows[stream].items())
            if window.start_ms <= open_time_ms < window.end_ms
        )


def _candle(stream: StreamKey, open_time_ms: int) -> ObservedCandle:
    return ObservedCandle(
        stream=stream,
        open_time_ms=open_time_ms,
        close_time_ms=open_time_ms + 59_999,
        open="1",
        high="2",
        low="1",
        close="2",
        volume="3",
        confirmed=True,
        observed_at_ms=open_time_ms + 60_000,
        source=ObservationSource.BYBIT_REST,
    )


def _prepare(path: Path, stream: StreamKey) -> None:
    initialize_database(path)
    register_stream(
        path,
        stream,
        exchange_symbol=stream.instrument.ticker.removesuffix(".P"),
        now_ms=1,
    )


def _reconciler(
    path: Path,
    stream: StreamKey,
    source: HistoricalSource,
    clock: Clock,
    *,
    repair_windows_per_pass: int,
    max_candles_per_window: int,
) -> HistoricalStreamReconciler:
    def factory() -> SqliteUnitOfWork:
        return SqliteUnitOfWork(path)

    repair = RepairStreamGaps(
        AuditStreamContinuity(factory),
        ImportHistoricalWindow(source, factory, clock),
        factory,
        clock,
        max_candles_per_window=max_candles_per_window,
    )
    return HistoricalStreamReconciler(
        lower_bound=FixedLowerBound(0),  # type: ignore[arg-type]
        repair=repair,
        lifecycle=RuntimeLifecycleRecorder(factory, clock.now_ms),
        now_ms=clock.now_ms,
        discovery_windows_per_pass=1,
        repair_windows_per_pass=repair_windows_per_pass,
    )


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


def test_empty_database_converges_through_repeated_bounded_passes(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    stream = StreamKey(InstrumentKey("ETHUSDT.P"), "1m")
    _prepare(path, stream)
    rows = {open_time: _candle(stream, open_time) for open_time in range(0, 360_000, 60_000)}
    source = HistoricalSource({stream: rows})
    clock = Clock()
    coordinator = StartupCoordinator(
        _reconciler(
            path,
            stream,
            source,
            clock,
            repair_windows_per_pass=1,
            max_candles_per_window=2,
        )
    )

    first = coordinator.execute_stream(stream)
    second = coordinator.execute_stream(stream, first.window)
    third = coordinator.execute_stream(stream, second.window)

    assert first.classification is StartupClassification.INCOMPLETE
    assert second.classification is StartupClassification.INCOMPLETE
    assert third.classification is StartupClassification.CONNECTING
    assert _open_times(path, stream) == tuple(range(0, 360_000, 60_000))
    assert [call[1] for call in source.calls] == [
        TimeWindow(0, 120_000),
        TimeWindow(120_000, 240_000),
        TimeWindow(240_000, 360_000),
    ]


def test_internal_gap_is_repaired_even_with_later_candles_present(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    stream = StreamKey(InstrumentKey("BTCUSDT.P"), "1m")
    _prepare(path, stream)
    all_rows = {open_time: _candle(stream, open_time) for open_time in range(0, 300_000, 60_000)}
    ingest = IngestObservedCandle(lambda: SqliteUnitOfWork(path))
    for open_time in (0, 60_000, 180_000, 240_000):
        ingest.execute(all_rows[open_time], committed_at_ms=300_000)
    source = HistoricalSource({stream: all_rows})
    clock = Clock(value=300_000)

    outcome = StartupCoordinator(
        _reconciler(
            path,
            stream,
            source,
            clock,
            repair_windows_per_pass=1,
            max_candles_per_window=1000,
        )
    ).execute_stream(stream)

    assert outcome.classification is StartupClassification.CONNECTING
    assert _open_times(path, stream) == tuple(range(0, 300_000, 60_000))
    assert source.calls == [(stream, TimeWindow(120_000, 180_000))]


def test_restart_reconstructs_remaining_gap_from_sqlite(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    stream = StreamKey(InstrumentKey("ETHUSDT.P"), "1m")
    _prepare(path, stream)
    rows = {open_time: _candle(stream, open_time) for open_time in range(0, 240_000, 60_000)}
    source = HistoricalSource({stream: rows})

    first_clock = Clock(value=240_000)
    first = StartupCoordinator(
        _reconciler(
            path,
            stream,
            source,
            first_clock,
            repair_windows_per_pass=1,
            max_candles_per_window=2,
        )
    ).execute_stream(stream)
    assert first.classification is StartupClassification.INCOMPLETE

    restarted_source = HistoricalSource({stream: rows})
    restarted_clock = Clock(value=240_000)
    after_restart = StartupCoordinator(
        _reconciler(
            path,
            stream,
            restarted_source,
            restarted_clock,
            repair_windows_per_pass=1,
            max_candles_per_window=2,
        )
    ).execute_stream(stream)

    assert after_restart.classification is StartupClassification.CONNECTING
    assert restarted_source.calls == [(stream, TimeWindow(120_000, 240_000))]
    assert _open_times(path, stream) == (0, 60_000, 120_000, 180_000)


@dataclass
class SequencedCoordinator:
    outcomes: dict[StreamKey, list[StartupClassification]]
    calls: list[StreamKey]

    def execute_stream(self, stream: StreamKey, window=None):  # type: ignore[no-untyped-def]
        self.calls.append(stream)
        classification = self.outcomes[stream].pop(0)
        return StartupStreamOutcome(stream, classification, window=window)


def test_recoverable_backoff_does_not_block_another_stream() -> None:
    asyncio.run(_recoverable_backoff_scenario())


async def _recoverable_backoff_scenario() -> None:
    btc = StreamKey(InstrumentKey("BTCUSDT.P"), "5m")
    eth = StreamKey(InstrumentKey("ETHUSDT.P"), "5m")
    coordinator = SequencedCoordinator(
        outcomes={
            btc: [StartupClassification.RECOVERABLE_FAILURE, StartupClassification.CONNECTING],
            eth: [StartupClassification.CONNECTING],
        },
        calls=[],
    )
    stop = asyncio.Event()
    completed: list[StreamKey] = []

    async def on_complete(stream: StreamKey) -> None:
        completed.append(stream)
        if len(completed) == 2:
            stop.set()

    worker = HistoricalReconciliationWorker(
        coordinator=coordinator,  # type: ignore[arg-type]
        initial_outcomes=(
            StartupStreamOutcome(btc, StartupClassification.RECOVERABLE_FAILURE),
            StartupStreamOutcome(eth, StartupClassification.INCOMPLETE),
        ),
        status=RuntimeStatusStore((btc, eth)),
        operation_gate=asyncio.Lock(),
        on_complete=on_complete,
        base_backoff_seconds=0.001,
        max_backoff_seconds=0.001,
        idle_seconds=0.0001,
    )
    await asyncio.wait_for(worker.run(stop), timeout=1)

    assert coordinator.calls[0] == btc
    assert eth in coordinator.calls[1:2]
    assert completed[0] == eth
    assert set(completed) == {btc, eth}
