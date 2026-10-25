from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

from market_data_service.domain import InstrumentKey, StreamKey
from market_data_service.runtime.historical_worker import HistoricalReconciliationWorker
from market_data_service.runtime.open_interest_runner import (
    LogOnlyBlockingReasons,
    OpenInterestTrackRunner,
)
from market_data_service.runtime.settings import RuntimeSettings
from market_data_service.runtime.startup_types import (
    LowerBoundProgressMarker,
    StartupClassification,
    StartupStreamOutcome,
)

STREAM = StreamKey(InstrumentKey("BTCUSDT.P"), "5m")


class CountingPasses:
    def __init__(self, classification: StartupClassification, *, seconds: float = 0.0) -> None:
        self.classification = classification
        self.seconds = seconds
        self.windows: list[object] = []

    def execute_stream(self, stream, window=None):  # type: ignore[no-untyped-def]
        self.windows.append(window)
        time.sleep(self.seconds)
        return StartupStreamOutcome(
            stream,
            self.classification,
            progress_marker=LowerBoundProgressMarker(len(self.windows)),
        )


def _worker(passes: CountingPasses, gate: asyncio.Lock, **kwargs) -> HistoricalReconciliationWorker:  # type: ignore[no-untyped-def]
    return HistoricalReconciliationWorker(
        coordinator=passes,
        initial_outcomes=(StartupStreamOutcome(STREAM, StartupClassification.INCOMPLETE),),
        status=LogOnlyBlockingReasons(),
        operation_gate=gate,
        idle_seconds=0.005,
        label="oi track",
        yield_to_gate=True,
        **kwargs,
    )


async def _noop(stream: StreamKey) -> None:
    return None


def test_open_interest_pass_does_not_start_while_the_gate_is_held() -> None:
    async def scenario() -> None:
        gate = asyncio.Lock()
        stop = asyncio.Event()
        passes = CountingPasses(StartupClassification.INCOMPLETE)
        await gate.acquire()
        task = asyncio.create_task(_worker(passes, gate, on_complete=_noop).run(stop))
        await asyncio.sleep(0.1)
        held_calls = len(passes.windows)
        gate.release()
        await asyncio.sleep(0.1)
        stop.set()
        await task
        assert held_calls == 0
        assert passes.windows

    asyncio.run(scenario())


def test_recovery_waits_at_most_one_open_interest_pass() -> None:
    async def scenario() -> None:
        gate = asyncio.Lock()
        stop = asyncio.Event()
        passes = CountingPasses(StartupClassification.INCOMPLETE, seconds=0.05)
        task = asyncio.create_task(_worker(passes, gate, on_complete=_noop).run(stop))
        await asyncio.sleep(0.12)
        before = len(passes.windows)
        async with gate:
            during = len(passes.windows)
            await asyncio.sleep(0.1)
            assert len(passes.windows) == during
        stop.set()
        await task
        assert during - before <= 1

    asyncio.run(scenario())


def test_completed_cycle_is_rescheduled_with_a_fresh_target() -> None:
    async def scenario() -> None:
        gate = asyncio.Lock()
        stop = asyncio.Event()
        passes = CountingPasses(StartupClassification.CONNECTING)
        holder: dict[str, HistoricalReconciliationWorker] = {}

        async def on_complete(stream: StreamKey) -> None:
            holder["worker"].schedule(stream, delay_seconds=0.05)

        worker = _worker(passes, gate, on_complete=on_complete)
        holder["worker"] = worker
        task = asyncio.create_task(worker.run(stop))
        await asyncio.sleep(0.02)
        assert len(passes.windows) == 1
        await asyncio.sleep(0.12)
        stop.set()
        await task
        assert len(passes.windows) >= 2
        assert all(window is None for window in passes.windows)

    asyncio.run(scenario())


class BrokenWiring:
    def __init__(self) -> None:
        self.builds = 0
        self.clock = SimpleNamespace(now_ms=lambda: 0)

    def open_interest_lower_bound(self):  # type: ignore[no-untyped-def]
        self.builds += 1
        raise RuntimeError("wiring failed")


def test_runner_contains_worker_failures_and_restarts() -> None:
    async def scenario() -> None:
        stop = asyncio.Event()
        wiring = BrokenWiring()
        runner = OpenInterestTrackRunner(
            streams=(STREAM,),
            wiring=wiring,  # type: ignore[arg-type]
            settings=RuntimeSettings(),
            operation_gate=asyncio.Lock(),
            restart_delay_seconds=0.01,
        )
        task = asyncio.create_task(runner.run(stop))
        await asyncio.sleep(0.1)
        stop.set()
        await task
        assert wiring.builds >= 2

    asyncio.run(scenario())


def test_runner_without_flagged_streams_does_nothing() -> None:
    async def scenario() -> None:
        wiring = BrokenWiring()
        runner = OpenInterestTrackRunner(
            streams=(),
            wiring=wiring,  # type: ignore[arg-type]
            settings=RuntimeSettings(),
            operation_gate=asyncio.Lock(),
        )
        await asyncio.wait_for(runner.run(asyncio.Event()), timeout=1)
        assert wiring.builds == 0

    asyncio.run(scenario())
