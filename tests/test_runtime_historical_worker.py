from __future__ import annotations

import asyncio
from dataclasses import dataclass

from market_data_service.domain import InstrumentKey, StreamKey
from market_data_service.runtime.historical_worker import HistoricalReconciliationWorker
from market_data_service.runtime.startup_types import (
    StartupClassification,
    StartupStreamOutcome,
)
from market_data_service.runtime.status import RuntimeStatusStore


@dataclass
class FakeCoordinator:
    calls: list[StreamKey]
    attempts: dict[StreamKey, int]

    def execute_stream(self, stream: StreamKey, window=None):  # type: ignore[no-untyped-def]
        self.calls.append(stream)
        count = self.attempts.get(stream, 0) + 1
        self.attempts[stream] = count
        classification = (
            StartupClassification.CONNECTING
            if count >= 2
            else StartupClassification.INCOMPLETE
        )
        return StartupStreamOutcome(stream, classification, window=window)


def test_worker_requeues_incomplete_streams_fairly() -> None:
    asyncio.run(_fairness_scenario())


async def _fairness_scenario() -> None:
    btc = StreamKey(InstrumentKey("BTCUSDT.P"), "5m")
    eth = StreamKey(InstrumentKey("ETHUSDT.P"), "5m")
    coordinator = FakeCoordinator([], {})
    status = RuntimeStatusStore((btc, eth))
    completed: list[StreamKey] = []
    stop = asyncio.Event()

    async def on_complete(stream: StreamKey) -> None:
        completed.append(stream)
        if len(completed) == 2:
            stop.set()

    worker = HistoricalReconciliationWorker(
        coordinator=coordinator,  # type: ignore[arg-type]
        initial_outcomes=(
            StartupStreamOutcome(btc, StartupClassification.INCOMPLETE),
            StartupStreamOutcome(eth, StartupClassification.INCOMPLETE),
        ),
        status=status,
        operation_gate=asyncio.Lock(),
        on_complete=on_complete,
        idle_seconds=0.001,
    )
    await asyncio.wait_for(worker.run(stop), timeout=1)

    assert coordinator.calls[:4] == [btc, eth, btc, eth]
    assert completed == [btc, eth]


def test_recoverable_failure_does_not_block_other_stream() -> None:
    asyncio.run(_failure_isolation_scenario())


async def _failure_isolation_scenario() -> None:
    btc = StreamKey(InstrumentKey("BTCUSDT.P"), "5m")
    eth = StreamKey(InstrumentKey("ETHUSDT.P"), "5m")
    calls: list[StreamKey] = []
    attempts: dict[StreamKey, int] = {}

    class Coordinator:
        def execute_stream(self, stream: StreamKey, window=None):  # type: ignore[no-untyped-def]
            calls.append(stream)
            attempts[stream] = attempts.get(stream, 0) + 1
            if stream == btc and attempts[stream] == 1:
                classification = StartupClassification.RECOVERABLE_FAILURE
            else:
                classification = StartupClassification.CONNECTING
            return StartupStreamOutcome(stream, classification, window=window)

    completed: list[StreamKey] = []
    stop = asyncio.Event()

    async def on_complete(stream: StreamKey) -> None:
        completed.append(stream)
        if len(completed) == 2:
            stop.set()

    worker = HistoricalReconciliationWorker(
        coordinator=Coordinator(),  # type: ignore[arg-type]
        initial_outcomes=(
            StartupStreamOutcome(btc, StartupClassification.RECOVERABLE_FAILURE),
            StartupStreamOutcome(eth, StartupClassification.INCOMPLETE),
        ),
        status=RuntimeStatusStore((btc, eth)),
        operation_gate=asyncio.Lock(),
        on_complete=on_complete,
        base_backoff_seconds=0.001,
        max_backoff_seconds=0.001,
        idle_seconds=0.001,
    )
    await asyncio.wait_for(worker.run(stop), timeout=1)

    assert calls[0] == btc
    assert eth in calls[1:3]
    assert completed[0] == eth
    assert set(completed) == {btc, eth}
