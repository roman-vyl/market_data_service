from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from market_data_service.adapters.sqlite import (
    SqliteUnitOfWork,
    initialize_database,
    register_stream,
)
from market_data_service.application.lower_bound import HistoricalLowerBoundResult
from market_data_service.application.repair_types import (
    RepairStatus,
    RepairStreamGapsResult,
)
from market_data_service.domain import ContinuityReport, InstrumentKey, StreamKey, TimeWindow
from market_data_service.domain.stream_state import StreamLifecycleState
from market_data_service.runtime.lifecycle import RuntimeLifecycleRecorder
from market_data_service.runtime.reconciliation import HistoricalStreamReconciler
from market_data_service.runtime.startup import StartupCoordinator
from market_data_service.runtime.startup_types import StartupClassification


@dataclass
class Clock:
    value: int = 180_000

    def now_ms(self) -> int:
        self.value += 1
        return self.value


class FakeLowerBound:
    def __init__(self, stream: StreamKey) -> None:
        self.stream = stream

    def execute(self, stream: StreamKey, *, max_windows: int) -> HistoricalLowerBoundResult:
        assert stream == self.stream
        assert max_windows == 2
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


class FakeRepair:
    def __init__(self, stream: StreamKey) -> None:
        self.stream = stream
        self.requests = []

    def execute(self, request):  # type: ignore[no-untyped-def]
        self.requests.append(request)
        audit = ContinuityReport(
            stream=request.stream,
            checked_start_ms=request.start_time_ms,
            checked_end_ms=request.end_time_ms,
            candle_count=3,
            is_continuous=True,
            gaps=(),
        )
        return RepairStreamGapsResult(
            stream=request.stream,
            requested_window=TimeWindow(request.start_time_ms, request.end_time_ms),
            status=RepairStatus.COMPLETE,
            pre_repair_audit=audit,
            post_repair_audit=audit,
            attempted_windows=0,
            completed_windows=0,
            window_results=(),
        )


def test_startup_routes_full_fixed_window_through_existing_repair(tmp_path: Path) -> None:
    database = tmp_path / "runtime.sqlite3"
    stream = StreamKey(InstrumentKey("BTCUSDT.P"), "1m")
    initialize_database(database)
    register_stream(database, stream, exchange_symbol="BTCUSDT", now_ms=1)
    clock = Clock()

    def factory() -> SqliteUnitOfWork:
        return SqliteUnitOfWork(database)

    lifecycle = RuntimeLifecycleRecorder(factory, clock.now_ms)
    repair = FakeRepair(stream)
    reconciler = HistoricalStreamReconciler(
        lower_bound=FakeLowerBound(stream),  # type: ignore[arg-type]
        repair=repair,  # type: ignore[arg-type]
        lifecycle=lifecycle,
        now_ms=clock.now_ms,
        discovery_windows_per_pass=2,
        repair_windows_per_pass=2,
    )
    outcome = StartupCoordinator(reconciler).execute((stream,))[0]

    assert outcome.classification is StartupClassification.CONNECTING
    assert outcome.window is not None
    assert outcome.window.start_time_ms == 0
    assert outcome.window.end_time_ms == 180_000
    assert repair.requests[0].start_time_ms == 0
    assert repair.requests[0].end_time_ms == 180_000
    assert repair.requests[0].max_windows == 2
    assert lifecycle.snapshot(stream).state is StreamLifecycleState.CONNECTING
