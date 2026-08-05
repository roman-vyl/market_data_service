from __future__ import annotations

import asyncio
import logging
import threading
from collections import deque
from dataclasses import replace
from pathlib import Path

import pytest

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
from market_data_service.application.realtime.outcomes import (
    RealtimeIngestionClassification,
    RealtimeIngestionOutcome,
)
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
from market_data_service.ports.committed_bar_notifier import CommittedBarNotification
from market_data_service.runtime.admission import RealtimeAdmissionGate
from market_data_service.runtime.committed_bar_notification import (
    CommittedBarNotificationWorker,
)
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
            stream: deque(classifications) for stream, classifications in results.items()
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


class ExplodingRecovery:
    async def execute(self, request: RealtimeRecoveryRequest) -> RealtimeRecoveryResult:
        raise RuntimeError(f"recovery exploded for {request.signal.stream.canonical_id}")


class GatedNotifier:
    """Port-level fake: send() blocks on a threading.Event until released."""

    def __init__(self) -> None:
        self._started = threading.Event()
        self._release = threading.Event()
        self.calls: list[CommittedBarNotification] = []

    def send(self, notification: CommittedBarNotification) -> None:
        self.calls.append(notification)
        self._started.set()
        assert self._release.wait(timeout=5), "send() was never released"

    def wait_started(self) -> None:
        assert self._started.wait(timeout=5), "send() never started"

    def release(self) -> None:
        self._release.set()


class RecordingNotifierWorker:
    def __init__(self) -> None:
        self.notifications: list[CommittedBarNotification] = []

    async def enqueue(self, notification: CommittedBarNotification) -> None:
        self.notifications.append(notification)


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
    notifier_worker: CommittedBarNotificationWorker | None = None,
) -> tuple[RuntimeRealtimeCoordinator, RuntimeStatusStore]:
    clock = Clock()
    topics = {
        f"kline.1.{stream.instrument.ticker.removesuffix('.P')}": stream for stream in streams
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
        max_recovery_windows=1,
        stale_check_seconds=0.01,
        recovery_base_backoff_seconds=backoff_seconds,
        recovery_max_backoff_seconds=backoff_seconds,
        recovery_idle_seconds=0.001,
        notifier_worker=notifier_worker,
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
    await runtime.on_event(SubscriptionConfirmed(("kline.1.BTCUSDT",), observed_at_ms=10))

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
    await runtime.on_event(SubscriptionConfirmed(("kline.1.BTCUSDT",), observed_at_ms=10))

    await _wait_for_state(path, stream, StreamLifecycleState.FAILED)
    stop.set()
    await runner

    assert recovery.calls == [stream]


def test_recovery_worker_failure_terminates_realtime_coordinator(tmp_path: Path) -> None:
    asyncio.run(_recovery_worker_failure_scenario(tmp_path))


async def _recovery_worker_failure_scenario(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    initialize_database(path)
    stream = _stream("BTCUSDT.P")
    _register_connecting(path, stream)
    runtime, _ = _runtime(path, (stream,), ExplodingRecovery())  # type: ignore[arg-type]
    stop = asyncio.Event()
    runner = asyncio.create_task(runtime.run(stop))
    await runtime.on_event(SubscriptionConfirmed(("kline.1.BTCUSDT",), observed_at_ms=10))

    with pytest.raises(ExceptionGroup) as exc_info:
        await asyncio.wait_for(runner, timeout=1)
    assert any(
        "recovery exploded for BTCUSDT.P:1m" in str(error) for error in exc_info.value.exceptions
    )


def test_failing_sibling_task_does_not_orphan_an_in_flight_notification(
    tmp_path: Path,
) -> None:
    """A sibling TaskGroup task raising must not abandon an in-flight send.

    When ExplodingRecovery raises, asyncio.TaskGroup cancels every sibling
    task, including the real committed-bar notifier worker. That worker's
    own cancellation-safe _send() must still wait for its in-flight HTTP
    thread before the group finishes unwinding, exactly as in the
    single-worker cancellation test, now exercised through the coordinator.
    """
    asyncio.run(_failing_sibling_does_not_orphan_send_scenario(tmp_path))


async def _failing_sibling_does_not_orphan_send_scenario(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    initialize_database(path)
    stream = _stream("BTCUSDT.P")
    _register_connecting(path, stream)
    notifier = GatedNotifier()
    notifier_worker = CommittedBarNotificationWorker(notifier, capacity=4)
    runtime, _ = _runtime(
        path,
        (stream,),
        ExplodingRecovery(),  # type: ignore[arg-type]
        notifier_worker=notifier_worker,
    )
    stop = asyncio.Event()
    runner = asyncio.create_task(runtime.run(stop))

    await runtime.on_outcome(
        RealtimeIngestionOutcome(stream, 0, RealtimeIngestionClassification.COMMITTED)
    )
    await asyncio.to_thread(notifier.wait_started)

    # Trigger the sibling failure while the notifier's send is still in
    # flight; the TaskGroup will try to cancel the notifier worker's task.
    await runtime.on_event(SubscriptionConfirmed(("kline.1.BTCUSDT",), observed_at_ms=10))

    # The group cannot finish unwinding until the gated send completes.
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.shield(runner), timeout=0.2)
    assert not runner.done()

    notifier.release()
    with pytest.raises(ExceptionGroup) as exc_info:
        await asyncio.wait_for(runner, timeout=1)
    assert any(
        "recovery exploded for BTCUSDT.P:1m" in str(error) for error in exc_info.value.exceptions
    )
    assert len(notifier.calls) == 1


def test_stop_does_not_wait_for_or_execute_delayed_realtime_retry(
    tmp_path: Path,
) -> None:
    asyncio.run(_stop_during_realtime_backoff_scenario(tmp_path))


async def _stop_during_realtime_backoff_scenario(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    initialize_database(path)
    stream = _stream("BTCUSDT.P")
    _register_connecting(path, stream)
    recovery = FakeRecovery(
        {
            stream: (
                RecoveryClassification.RECOVERABLE_FAILURE,
                RecoveryClassification.RESTORED,
            )
        }
    )
    runtime, _ = _runtime(path, (stream,), recovery, backoff_seconds=60)
    stop = asyncio.Event()
    runner = asyncio.create_task(runtime.run(stop))
    await runtime.on_event(SubscriptionConfirmed(("kline.1.BTCUSDT",), observed_at_ms=10))
    for _ in range(200):
        if len(recovery.calls) == 1:
            break
        await asyncio.sleep(0.001)
    assert recovery.calls == [stream]

    stop.set()
    await asyncio.wait_for(runner, timeout=0.5)
    assert recovery.calls == [stream]


def test_unchanged_incomplete_recovery_backs_off_without_tight_loop(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger="market_data_service.runtime.realtime")
    asyncio.run(_stop_during_incomplete_backoff_scenario(tmp_path))
    assert "no_progress_attempts=1" in caplog.text
    assert "delay_seconds=60" in caplog.text


async def _stop_during_incomplete_backoff_scenario(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    initialize_database(path)
    stream = _stream("BTCUSDT.P")
    _register_connecting(path, stream)
    recovery = FakeRecovery(
        {
            stream: (
                RecoveryClassification.INCOMPLETE,
                RecoveryClassification.RESTORED,
            )
        }
    )
    runtime, status = _runtime(path, (stream,), recovery, backoff_seconds=60)
    stop = asyncio.Event()
    runner = asyncio.create_task(runtime.run(stop))
    await runtime.on_event(SubscriptionConfirmed(("kline.1.BTCUSDT",), observed_at_ms=10))
    for _ in range(200):
        if recovery.calls:
            break
        await asyncio.sleep(0.001)
    await asyncio.sleep(0.01)

    assert recovery.calls == [stream]
    assert status.readiness_document()["streams"][0]["reason"] == (
        "realtime_recovery_no_progress_backoff"
    )
    stop.set()
    await asyncio.wait_for(runner, timeout=0.5)
    assert recovery.calls == [stream]


def test_late_admission_synchronizes_supervisor_before_first_outcome(tmp_path: Path) -> None:
    asyncio.run(_late_admission_scenario(tmp_path))


async def _late_admission_scenario(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    initialize_database(path)
    stream = _stream("ETHUSDT.P")
    _register_connecting(path, stream)
    with SqliteUnitOfWork(path) as unit_of_work:
        state = unit_of_work.get_stream_state(stream)
        unit_of_work.save_stream_state(
            replace(state, latest_committed_open_time_ms=240_000)
        )
        unit_of_work.commit()
    clock = Clock()
    topics = {"kline.1.ETHUSDT": stream}
    supervisor = RealtimeSupervisor(
        (stream,),
        topics,
        clock.now_ms,
        initial_latest_open_time_ms={stream: 0},
    )
    gate = RealtimeAdmissionGate()
    runtime = RuntimeRealtimeCoordinator(
        streams=(stream,),
        connector=IdleConnector(),  # type: ignore[arg-type]
        supervisor=supervisor,
        recovery=FakeRecovery({stream: (RecoveryClassification.RESTORED,)}),  # type: ignore[arg-type]
        lifecycle=RuntimeLifecycleRecorder(_factory(path), clock.now_ms),
        status=RuntimeStatusStore((stream,)),
        admission=gate,
        operation_gate=asyncio.Lock(),
        now_ms=clock.now_ms,
        max_recovery_windows=1,
    )

    await runtime.admit(stream)
    signals = await runtime.on_outcome(
        RealtimeIngestionOutcome(
            stream,
            300_000,
            RealtimeIngestionClassification.COMMITTED,
        )
    )

    assert signals is None
    assert gate.allows(stream)
    assert supervisor.facts(stream).last_successful_open_time_ms == 300_000


def test_late_admission_without_durable_anchor_keeps_gate_closed(tmp_path: Path) -> None:
    asyncio.run(_missing_admission_anchor_scenario(tmp_path))


async def _missing_admission_anchor_scenario(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    initialize_database(path)
    stream = _stream("BTCUSDT.P")
    register_stream(path, stream, exchange_symbol="BTCUSDT", now_ms=1)
    clock = Clock()
    gate = RealtimeAdmissionGate()
    runtime = RuntimeRealtimeCoordinator(
        streams=(stream,),
        connector=IdleConnector(),  # type: ignore[arg-type]
        supervisor=RealtimeSupervisor(
            (stream,),
            {"kline.1.BTCUSDT": stream},
            clock.now_ms,
        ),
        recovery=FakeRecovery({stream: (RecoveryClassification.RESTORED,)}),  # type: ignore[arg-type]
        lifecycle=RuntimeLifecycleRecorder(_factory(path), clock.now_ms),
        status=RuntimeStatusStore((stream,)),
        admission=gate,
        operation_gate=asyncio.Lock(),
        now_ms=clock.now_ms,
        max_recovery_windows=1,
    )

    await runtime.admit(stream)

    assert not gate.allows(stream)


def _coordinator_with_notifier(
    tmp_path: Path,
    stream: StreamKey,
    notifier_worker: RecordingNotifierWorker,
    *,
    admitted: bool = True,
) -> RuntimeRealtimeCoordinator:
    path = tmp_path / "market.sqlite3"
    initialize_database(path)
    register_stream(
        path,
        stream,
        exchange_symbol=stream.instrument.ticker.removesuffix(".P"),
        now_ms=1,
    )
    clock = Clock()
    return RuntimeRealtimeCoordinator(
        streams=(stream,),
        connector=IdleConnector(),  # type: ignore[arg-type]
        supervisor=RealtimeSupervisor(
            (stream,),
            {"kline.1.BTCUSDT": stream},
            clock.now_ms,
        ),
        recovery=FakeRecovery({stream: (RecoveryClassification.RESTORED,)}),  # type: ignore[arg-type]
        lifecycle=RuntimeLifecycleRecorder(_factory(path), clock.now_ms),
        status=RuntimeStatusStore((stream,)),
        admission=RealtimeAdmissionGate((stream,) if admitted else ()),
        operation_gate=asyncio.Lock(),
        now_ms=clock.now_ms,
        max_recovery_windows=1,
        notifier_worker=notifier_worker,  # type: ignore[arg-type]
    )


def test_committed_outcome_on_admitted_stream_enqueues_one_notification(
    tmp_path: Path,
) -> None:
    asyncio.run(_committed_enqueues_scenario(tmp_path))


async def _committed_enqueues_scenario(tmp_path: Path) -> None:
    stream = _stream("BTCUSDT.P")
    notifier_worker = RecordingNotifierWorker()
    runtime = _coordinator_with_notifier(tmp_path, stream, notifier_worker)

    await runtime.on_outcome(
        RealtimeIngestionOutcome(stream, 300_000, RealtimeIngestionClassification.COMMITTED)
    )

    assert notifier_worker.notifications == [
        CommittedBarNotification(instrument="BTCUSDT.P", timeframe="1m", open_time_ms=300_000)
    ]


def test_non_committed_classifications_enqueue_nothing(tmp_path: Path) -> None:
    asyncio.run(_non_committed_scenario(tmp_path))


async def _non_committed_scenario(tmp_path: Path) -> None:
    stream = _stream("BTCUSDT.P")
    notifier_worker = RecordingNotifierWorker()
    runtime = _coordinator_with_notifier(tmp_path, stream, notifier_worker)

    for classification in (
        RealtimeIngestionClassification.DUPLICATE,
        RealtimeIngestionClassification.CORRECTED,
        RealtimeIngestionClassification.REJECTED,
        RealtimeIngestionClassification.FAILED,
    ):
        await runtime.on_outcome(RealtimeIngestionOutcome(stream, 0, classification))

    assert notifier_worker.notifications == []


def test_committed_outcome_on_non_admitted_stream_enqueues_nothing(tmp_path: Path) -> None:
    asyncio.run(_not_admitted_scenario(tmp_path))


async def _not_admitted_scenario(tmp_path: Path) -> None:
    stream = _stream("BTCUSDT.P")
    notifier_worker = RecordingNotifierWorker()
    runtime = _coordinator_with_notifier(tmp_path, stream, notifier_worker, admitted=False)

    await runtime.on_outcome(
        RealtimeIngestionOutcome(stream, 0, RealtimeIngestionClassification.COMMITTED)
    )

    assert notifier_worker.notifications == []


def test_on_outcome_without_notifier_worker_does_not_raise(tmp_path: Path) -> None:
    asyncio.run(_no_notifier_worker_scenario(tmp_path))


async def _no_notifier_worker_scenario(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    initialize_database(path)
    stream = _stream("BTCUSDT.P")
    register_stream(path, stream, exchange_symbol="BTCUSDT", now_ms=1)
    clock = Clock()
    runtime = RuntimeRealtimeCoordinator(
        streams=(stream,),
        connector=IdleConnector(),  # type: ignore[arg-type]
        supervisor=RealtimeSupervisor((stream,), {"kline.1.BTCUSDT": stream}, clock.now_ms),
        recovery=FakeRecovery({stream: (RecoveryClassification.RESTORED,)}),  # type: ignore[arg-type]
        lifecycle=RuntimeLifecycleRecorder(_factory(path), clock.now_ms),
        status=RuntimeStatusStore((stream,)),
        admission=RealtimeAdmissionGate((stream,)),
        operation_gate=asyncio.Lock(),
        now_ms=clock.now_ms,
        max_recovery_windows=1,
    )

    await runtime.on_outcome(
        RealtimeIngestionOutcome(stream, 0, RealtimeIngestionClassification.COMMITTED)
    )
