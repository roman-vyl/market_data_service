from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path

from market_data_service.adapters.sqlite import (
    SqliteUnitOfWork,
    initialize_database,
    register_stream,
)
from market_data_service.application.audit_continuity import (
    AuditStreamContinuity,
    AuditStreamContinuityRequest,
)
from market_data_service.application.import_window import ImportHistoricalWindow
from market_data_service.application.lower_bound import HistoricalLowerBoundResult
from market_data_service.application.repair_gaps import RepairStreamGaps
from market_data_service.domain import InstrumentKey, StreamKey, TimeWindow
from market_data_service.domain.candles import ObservationSource, ObservedCandle
from market_data_service.runtime.admission import RealtimeAdmissionGate
from market_data_service.runtime.historical_worker import HistoricalReconciliationWorker
from market_data_service.runtime.lifecycle import RuntimeLifecycleRecorder
from market_data_service.runtime.reconciliation import HistoricalStreamReconciler
from market_data_service.runtime.startup import StartupCoordinator
from market_data_service.runtime.startup_types import StartupClassification
from market_data_service.runtime.status import RuntimeStatusStore


@dataclass
class Clock:
    value: int = 360_000

    def now_ms(self) -> int:
        return self.value


class LowerBounds:
    def execute(self, stream: StreamKey, *, max_windows: int) -> HistoricalLowerBoundResult:
        return HistoricalLowerBoundResult(
            stream=stream,
            launch_time_ms=0,
            search_start_time_ms=0,
            earliest_available_open_time_ms=0,
            metadata_cached=True,
            lower_bound_cached=True,
            resolved=True,
            discovery_windows_used=0,
        )


class Source:
    def __init__(self, rows: dict[StreamKey, tuple[ObservedCandle, ...]]) -> None:
        self._rows = rows

    def fetch_closed_candles(self, stream, window, *, observed_at_ms):  # type: ignore[no-untyped-def]
        return tuple(
            candle
            for candle in self._rows[stream]
            if window.start_ms <= candle.open_time_ms < window.end_ms
        )


def _candle(stream: StreamKey, open_time_ms: int) -> ObservedCandle:
    return ObservedCandle(
        stream=stream,
        open_time_ms=open_time_ms,
        close_time_ms=open_time_ms + 59_999,
        open="100",
        high="102",
        low="99",
        close="101",
        volume="1",
        confirmed=True,
        observed_at_ms=open_time_ms + 60_000,
        source=ObservationSource.BYBIT_REST,
    )


def test_complete_stream_runs_while_empty_stream_converges_in_bounded_turns(
    tmp_path: Path,
) -> None:
    asyncio.run(_scenario(tmp_path))


async def _scenario(tmp_path: Path) -> None:
    database = tmp_path / "runtime.sqlite3"
    btc = StreamKey(InstrumentKey("BTCUSDT.P"), "1m")
    eth = StreamKey(InstrumentKey("ETHUSDT.P"), "1m")
    initialize_database(database)
    register_stream(database, btc, exchange_symbol="BTCUSDT", now_ms=1)
    register_stream(database, eth, exchange_symbol="ETHUSDT", now_ms=1)

    def uow_factory() -> SqliteUnitOfWork:
        return SqliteUnitOfWork(database)

    rows = {
        stream: tuple(_candle(stream, value) for value in range(0, 360_000, 60_000))
        for stream in (btc, eth)
    }
    source = Source(rows)
    clock = Clock()
    importer = ImportHistoricalWindow(source, uow_factory, clock)  # type: ignore[arg-type]
    repair = RepairStreamGaps(
        AuditStreamContinuity(uow_factory),
        importer,
        uow_factory,
        clock,
        max_candles_per_window=2,
    )
    lifecycle = RuntimeLifecycleRecorder(uow_factory, clock.now_ms)

    # BTC is already complete before runtime startup; ETH is empty.
    lifecycle.prepare_for_bootstrap(btc)
    lifecycle.mark_auditing(btc)
    for window_start in range(0, 360_000, 120_000):
        importer.execute(
            btc,
            TimeWindow(window_start, window_start + 120_000),
        )

    reconciler = HistoricalStreamReconciler(
        lower_bound=LowerBounds(),  # type: ignore[arg-type]
        repair=repair,
        lifecycle=lifecycle,
        now_ms=clock.now_ms,
        discovery_windows_per_pass=1,
        repair_windows_per_pass=1,
    )
    coordinator = StartupCoordinator(reconciler)
    outcomes = coordinator.execute((btc, eth))

    assert outcomes[0].classification is StartupClassification.CONNECTING
    assert outcomes[1].classification is StartupClassification.INCOMPLETE

    admission = RealtimeAdmissionGate((btc,))
    assert admission.allows(btc)
    assert not admission.allows(eth)

    stop = asyncio.Event()

    async def on_complete(stream: StreamKey) -> None:
        admission.admit(stream)
        stop.set()

    worker = HistoricalReconciliationWorker(
        coordinator=coordinator,
        initial_outcomes=outcomes,
        status=RuntimeStatusStore((btc, eth)),
        operation_gate=asyncio.Lock(),
        on_complete=on_complete,
        idle_seconds=0.001,
    )
    await asyncio.wait_for(worker.run(stop), timeout=2)

    assert admission.allows(eth)
    report = AuditStreamContinuity(uow_factory).execute(
        AuditStreamContinuityRequest(eth, 0, 360_000)
    )
    assert report.is_continuous
    assert report.gaps == ()
