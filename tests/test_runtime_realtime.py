from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import replace
from pathlib import Path

from market_data_service.adapters.sqlite import (
    SqliteUnitOfWork,
    initialize_database,
    register_stream,
)
from market_data_service.adapters.sqlite.consumer_candle_reader import (
    SqliteConsumerCandleReader,
)
from market_data_service.application.consumer_read import CandleRangeRequest, GetCandleRange
from market_data_service.application.realtime.events import SubscriptionConfirmed
from market_data_service.application.realtime.recovery_types import (
    RealtimeRecoveryRequest,
    RealtimeRecoveryResult,
    RecoveryClassification,
)
from market_data_service.application.realtime.supervisor import RealtimeSupervisor
from market_data_service.config.markets import MarketSourceConfig, ValidatedMarketConfig
from market_data_service.domain.candles import CanonicalCandle, ObservationSource
from market_data_service.domain.identity import InstrumentKey, StreamKey
from market_data_service.domain.instruments import HistoryPolicy, InstrumentCoverage
from market_data_service.domain.stream_state import StreamLifecycleState
from market_data_service.runtime.admission import RealtimeAdmissionGate
from market_data_service.runtime.lifecycle import RuntimeLifecycleRecorder
from market_data_service.runtime.realtime import RuntimeRealtimeCoordinator
from market_data_service.runtime.status import RuntimeStatusStore


class Clock:
    def __init__(self) -> None:
        self.value = 1_000

    def now_ms(self) -> int:
        self.value += 1
        return self.value


class IdleConnector:
    async def run(self, stop_event: asyncio.Event) -> None:
        await stop_event.wait()


class FakeRecovery:
    def __init__(
        self,
        results: dict[StreamKey, tuple[RecoveryClassification, ...]],
    ) -> None:
        self._results = {
            stream: deque(classifications)
            for stream, classifications in results.items()
        }
        self.calls: list[StreamKey] = []

    async def execute(self, request: RealtimeRecoveryRequest) -> RealtimeRecoveryResult:
        self.calls.append(request.signal.stream)
        classification = self._results[request.signal.stream].popleft()
        return RealtimeRecoveryResult(
            stream=request.signal.stream,
            classification=classification,
            recovery_window=None,
            restored_through_open_time_ms=0
            if classification is RecoveryClassification.RESTORED
            else None,
            error_code="temporary_source_failure"
            if classification is RecoveryClassification.RECOVERABLE_FAILURE
            else None,
        )


def _factory(path: Path):
    def factory() -> SqliteUnitOfWork:
        return SqliteUnitOfWork(path)

    return factory


def _stream(ticker: str) -> StreamKey:
    return StreamKey(InstrumentKey(ticker), "1m")


def _register_connecting(path: Path, stream: StreamKey) -> None:
    register_stream(
        path,
        stream,
        exchange_symbol=stream.instrument.ticker.removesuffix(".P"),
        now_ms=1,
    )
    with SqliteUnitOfWork(path) as unit_of_work:
        state = unit_of_work.get_stream_state(stream)
        unit_of_work.save_stream_state(
            replace(
                state,
                state=StreamLifecycleState.CONNECTING,
                earliest_available_open_time_ms=0,
                latest_committed_open_time_ms=0,
            )
        )
        unit_of_work.commit()


def _insert_candle(path: Path, stream: StreamKey) -> None:
    with SqliteUnitOfWork(path) as unit_of_work:
        unit_of_work.insert_candle(
            CanonicalCandle(
                stream=stream,
                open_time_ms=0,
                close_time_ms=59_999,
                open="1",
                high="2",
                low="1",
                close="1.5",
                volume="3",
                source=ObservationSource.BYBIT_REST,
                committed_at_ms=10,
            )
        )
        unit_of_work.commit()


def _runtime(
    path: Path,
    streams: tuple[StreamKey, ...],
    recovery: FakeRecovery,
    *,
    backoff_seconds: float = 0.001,
) -> tuple[RuntimeRealtimeCoordinator, RuntimeStatusStore]:
    clock = Clock()
    topics = {
        f"kline.1.{stream.instrument.ticker.removesuffix('.P')}": stream
        for stream in streams
    }
    status = RuntimeStatusStore(streams)
    runtime = RuntimeRealtimeCoordinator(
        streams=streams,
        connector=IdleConnector(),  # type: ignore[arg-type]
        supervisor=RealtimeSupervisor(streams, topics, clock.now_ms),
        recovery=recovery,  # type: ignore[arg-type]
        lifecycle=RuntimeLifecycleRecorder(_factory(path), clock.now_ms),
        status=status,
        admission=RealtimeAdmissionGate(streams),
        operation_gate=asyncio.Lock(),
        now_ms=clock.now_ms,
        max_backfill_windows=1,
        max_repair_windows=1,
        stale_check_seconds=0.01,
        recovery_base_backoff_seconds=backoff_seconds,
        recovery_max_backoff_seconds=backoff_seconds,
        recovery_idle_seconds=0.001,
    )
    return runtime, status


async def _wait_for_state(
    path: Path,
    stream: StreamKey,
    state: StreamLifecycleState,
) -> None:
    for _ in range(200):
        with SqliteUnitOfWork(path) as unit_of_work:
            current = unit_of_work.get_stream_state(stream).state
        if current is state:
            return
        await asyncio.sleep(0.001)
    raise AssertionError(f"{stream.canonical_id} did not reach {state.value}")


def test_realtime_recovery_requeues_incomplete_without_blocking_other_streams(
    tmp_path: Path,
) -> None:
    asyncio.run(_requeue_incomplete_scenario(tmp_path))


async def _requeue_incomplete_scenario(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    initialize_database(path)
    btc = _stream("BTCUSDT.P")
    eth = _stream("ETHUSDT.P")
    _register_connecting(path, btc)
    _register_connecting(path, eth)
    recovery = FakeRecovery(
        {
            btc: (
                RecoveryClassification.INCOMPLETE,
                RecoveryClassification.RESTORED,
            ),
            eth: (RecoveryClassification.RESTORED,),
        }
    )
    runtime, _ = _runtime(path, (btc, eth), recovery)
    stop = asyncio.Event()
    runner = asyncio.create_task(runtime.run(stop))
    await runtime.on_event(
        SubscriptionConfirmed(
            ("kline.1.BTCUSDT", "kline.1.ETHUSDT"),
            observed_at_ms=10,
        )
    )

    await _wait_for_state(path, btc, StreamLifecycleState.READY)
    await _wait_for_state(path, eth, StreamLifecycleState.READY)
    stop.set()
    await runner

    assert recovery.calls == [btc, eth, btc]


def test_realtime_recovery_retries_recoverable_failure_after_backoff(
    tmp_path: Path,
) -> None:
    asyncio.run(_recoverable_backoff_scenario(tmp_path))


async def _recoverable_backoff_scenario(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    initialize_database(path)
    btc = _stream("BTCUSDT.P")
    eth = _stream("ETHUSDT.P")
    _register_connecting(path, btc)
    _register_connecting(path, eth)
    recovery = FakeRecovery(
        {
            btc: (
                RecoveryClassification.RECOVERABLE_FAILURE,
                RecoveryClassification.RESTORED,
            ),
            eth: (RecoveryClassification.RESTORED,),
        }
    )
    runtime, _ = _runtime(path, (btc, eth), recovery)
    stop = asyncio.Event()
    runner = asyncio.create_task(runtime.run(stop))
    await runtime.on_event(
        SubscriptionConfirmed(
            ("kline.1.BTCUSDT", "kline.1.ETHUSDT"),
            observed_at_ms=10,
        )
    )

    await _wait_for_state(path, btc, StreamLifecycleState.READY)
    await _wait_for_state(path, eth, StreamLifecycleState.READY)
    stop.set()
    await runner

    assert recovery.calls[0] == btc
    assert eth in recovery.calls[1:3]
    assert recovery.calls.count(btc) == 2


def test_restored_recovery_marks_ready_and_consumer_can_read_before_live_candle(
    tmp_path: Path,
) -> None:
    asyncio.run(_data_ready_consumer_read_scenario(tmp_path))


async def _data_ready_consumer_read_scenario(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    initialize_database(path)
    stream = _stream("BTCUSDT.P")
    _register_connecting(path, stream)
    _insert_candle(path, stream)
    recovery = FakeRecovery({stream: (RecoveryClassification.RESTORED,)})
    runtime, status = _runtime(path, (stream,), recovery)
    stop = asyncio.Event()
    runner = asyncio.create_task(runtime.run(stop))
    await runtime.on_event(
        SubscriptionConfirmed(("kline.1.BTCUSDT",), observed_at_ms=10)
    )

    await _wait_for_state(path, stream, StreamLifecycleState.READY)
    document = status.readiness_document()
    stream_status = document["streams"][0]
    assert stream_status["ready"] is True
    assert stream_status["data_ready"] is True
    assert stream_status["realtime_live"] is False

    config = ValidatedMarketConfig(
        1,
        MarketSourceConfig("bybit", "linear"),
        (
            InstrumentCoverage(
                stream.instrument,
                "BTCUSDT",
                True,
                ("1m",),
                HistoryPolicy.FULL_AVAILABLE,
            ),
        ),
    )
    result = GetCandleRange(config, SqliteConsumerCandleReader(path)).execute(
        CandleRangeRequest("BTCUSDT.P", "1m", 0, 60_000)
    )
    assert [candle.open_time_ms for candle in result.candles] == [0]

    stop.set()
    await runner


def test_fatal_recovery_is_not_retried(tmp_path: Path) -> None:
    asyncio.run(_fatal_no_retry_scenario(tmp_path))


async def _fatal_no_retry_scenario(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    initialize_database(path)
    stream = _stream("BTCUSDT.P")
    _register_connecting(path, stream)
    recovery = FakeRecovery({stream: (RecoveryClassification.FATAL_FAILURE,)})
    runtime, _ = _runtime(path, (stream,), recovery)
    stop = asyncio.Event()
    runner = asyncio.create_task(runtime.run(stop))
    await runtime.on_event(
        SubscriptionConfirmed(("kline.1.BTCUSDT",), observed_at_ms=10)
    )

    await _wait_for_state(path, stream, StreamLifecycleState.FAILED)
    stop.set()
    await runner

    assert recovery.calls == [stream]
