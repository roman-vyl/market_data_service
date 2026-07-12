from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from market_data_service.adapters.sqlite import (
    SqliteUnitOfWork,
    initialize_database,
    register_stream,
)
from market_data_service.application.full_bootstrap import FullHistoryBootstrapResult
from market_data_service.application.lower_bound import HistoricalLowerBoundResult
from market_data_service.domain import ContinuityReport, InstrumentKey, StreamKey
from market_data_service.domain.stream_state import StreamLifecycleState
from market_data_service.runtime.lifecycle import RuntimeLifecycleRecorder
from market_data_service.runtime.startup import StartupCoordinator
from market_data_service.runtime.startup_types import StartupClassification


@dataclass
class Clock:
    value: int = 1_000_000

    def now_ms(self) -> int:
        self.value += 1
        return self.value


class FakeBootstrap:
    def __init__(self, stream: StreamKey) -> None:
        self.stream = stream

    def execute(self, request):  # type: ignore[no-untyped-def]
        lower = HistoricalLowerBoundResult(
            stream=self.stream,
            launch_time_ms=0,
            search_start_time_ms=0,
            earliest_available_open_time_ms=0,
            metadata_cached=True,
            lower_bound_cached=True,
            discovery_windows_used=0,
            resolved=True,
        )
        return FullHistoryBootstrapResult(
            stream=self.stream,
            status="backfilled",
            max_windows=request.max_windows,
            target_open_time_ms=60_000,
            lower_bound=lower,
            backfill=type("Backfill", (), {"reached_end": True})(),  # type: ignore[arg-type]
        )


class FakeAuditor:
    def execute(self, request):  # type: ignore[no-untyped-def]
        return ContinuityReport(
            stream=request.stream,
            checked_start_ms=request.start_time_ms,
            checked_end_ms=request.end_time_ms,
            candle_count=2,
            is_continuous=True,
            gaps=(),
        )


class FakeRepair:
    def execute(self, request):  # type: ignore[no-untyped-def]
        raise AssertionError("repair should not run for continuous history")


def test_startup_reconciles_persisted_ready_before_connecting(tmp_path: Path) -> None:
    database = tmp_path / "runtime.sqlite3"
    stream = StreamKey(InstrumentKey("BTCUSDT.P"), "1m")
    initialize_database(database)
    register_stream(database, stream, exchange_symbol="BTCUSDT", now_ms=1)
    clock = Clock()
    def factory() -> SqliteUnitOfWork:
        return SqliteUnitOfWork(database)
    lifecycle = RuntimeLifecycleRecorder(factory, clock.now_ms)
    lifecycle.prepare_for_bootstrap(stream)
    with factory() as uow:
        snapshot = uow.get_stream_state(stream)
        assert snapshot.state is StreamLifecycleState.BOOTSTRAPPING

    coordinator = StartupCoordinator(
        bootstrap_factory=lambda value: FakeBootstrap(value),  # type: ignore[arg-type]
        auditor=FakeAuditor(),  # type: ignore[arg-type]
        repair=FakeRepair(),  # type: ignore[arg-type]
        lifecycle=lifecycle,
        backfill_windows_per_stream=2,
        repair_windows_per_stream=2,
    )
    outcome = coordinator.execute((stream,))[0]

    assert outcome.classification is StartupClassification.CONNECTING
    assert lifecycle.snapshot(stream).state is StreamLifecycleState.CONNECTING
