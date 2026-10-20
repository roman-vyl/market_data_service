from __future__ import annotations

import asyncio
import itertools
from collections import deque

from market_data_service.application.realtime.events import RecoveryReason, RecoveryRequired
from market_data_service.application.realtime.recovery_types import (
    RealtimeRecoveryRequest,
    RealtimeRecoveryResult,
    RecoveryClassification,
)
from market_data_service.application.realtime.supervisor import RealtimeSupervisor
from market_data_service.domain.identity import InstrumentKey, StreamKey
from market_data_service.domain.windows import TimeWindow
from market_data_service.runtime.realtime_recovery_worker import RealtimeRecoveryWorker
from market_data_service.runtime.status import RuntimeStatusStore

FIRST = 0.06
NEXT = 0.18
BASE = 0.005
MAX = 0.01
ADVANCED = TimeWindow(60_000, 120_000)


class TimedRecovery:
    def __init__(self, results: list[RecoveryClassification | str]) -> None:
        self._results = deque(results)
        self.times: list[float] = []

    async def execute(self, request: RealtimeRecoveryRequest) -> RealtimeRecoveryResult:
        self.times.append(asyncio.get_running_loop().time())
        item = self._results.popleft()
        advanced = item == "advanced"
        classification = RecoveryClassification.INCOMPLETE if advanced else item
        assert isinstance(classification, RecoveryClassification)
        return RealtimeRecoveryResult(
            stream=request.signal.stream,
            classification=classification,
            recovery_window=request.recovery_window,
            next_recovery_window=ADVANCED if advanced else None,
            restored_through_open_time_ms=(
                0 if classification is RecoveryClassification.RESTORED else None
            ),
            error_code=(
                "temporary_source_failure"
                if classification is RecoveryClassification.RECOVERABLE_FAILURE
                else None
            ),
        )


def _run(reason: RecoveryReason, results: list[RecoveryClassification | str]) -> list[float]:
    async def scenario() -> list[float]:
        stream = StreamKey(InstrumentKey("BTCUSDT.P"), "5m")
        recovery = TimedRecovery(results)
        worker = RealtimeRecoveryWorker(
            recovery=recovery,  # type: ignore[arg-type]
            supervisor=RealtimeSupervisor((stream,), {}, lambda: 1_000),
            status=RuntimeStatusStore((stream,)),
            operation_gate=asyncio.Lock(),
            sync_lifecycle=lambda: None,
            max_windows=1,
            base_backoff_seconds=BASE,
            max_backoff_seconds=MAX,
            stale_first_retry_seconds=FIRST,
            stale_next_retry_seconds=NEXT,
            idle_seconds=0.001,
        )
        stop = asyncio.Event()
        runner = asyncio.create_task(worker.run(stop))
        await worker.enqueue(RecoveryRequired(stream, reason, 1_000, 0))
        for _ in range(2000):
            if not recovery._results:
                break
            await asyncio.sleep(0.005)
        await asyncio.sleep(0.02)
        stop.set()
        await runner
        return recovery.times

    return asyncio.run(scenario())


def _gaps(times: list[float]) -> list[float]:
    return [later - earlier for earlier, later in itertools.pairwise(times)]


F = RecoveryClassification.RECOVERABLE_FAILURE
I = RecoveryClassification.INCOMPLETE  # noqa: E741
R = RecoveryClassification.RESTORED


def test_stale_failures_follow_first_then_next_retry_delay() -> None:
    gaps = _gaps(_run(RecoveryReason.STALE, [F, F, F, R]))

    assert len(gaps) == 3
    assert gaps[0] >= FIRST
    assert gaps[1] >= NEXT
    assert gaps[2] >= NEXT


def test_stale_incomplete_without_advance_is_rate_limited() -> None:
    gaps = _gaps(_run(RecoveryReason.STALE, [I, I, I, R]))

    assert gaps[0] >= FIRST
    assert gaps[1] >= NEXT
    assert gaps[2] >= NEXT


def test_stale_real_window_advance_continues_immediately_and_resets_counter() -> None:
    gaps = _gaps(_run(RecoveryReason.STALE, [F, "advanced", F, R]))

    assert gaps[0] >= FIRST
    assert gaps[1] < FIRST
    assert gaps[2] >= FIRST
    assert gaps[2] < NEXT


def test_stale_restored_with_nothing_attempted_is_not_retried() -> None:
    assert len(_run(RecoveryReason.STALE, [R])) == 1


def test_reconnect_and_sequence_gap_keep_existing_backoff() -> None:
    for reason in (RecoveryReason.DISCONNECT, RecoveryReason.SEQUENCE_DISCONTINUITY):
        gaps = _gaps(_run(reason, [F, F, I, R]))

        assert len(gaps) == 3
        assert all(gap < FIRST for gap in gaps), (reason, gaps)


def test_reconnect_signal_is_not_delayed_by_waiting_stale_retry() -> None:
    async def scenario() -> tuple[list[float], float]:
        stream = StreamKey(InstrumentKey("BTCUSDT.P"), "5m")
        recovery = TimedRecovery([F, R, R])
        worker = RealtimeRecoveryWorker(
            recovery=recovery,  # type: ignore[arg-type]
            supervisor=RealtimeSupervisor((stream,), {}, lambda: 1_000),
            status=RuntimeStatusStore((stream,)),
            operation_gate=asyncio.Lock(),
            sync_lifecycle=lambda: None,
            max_windows=1,
            base_backoff_seconds=BASE,
            max_backoff_seconds=MAX,
            stale_first_retry_seconds=30.0,
            stale_next_retry_seconds=60.0,
            idle_seconds=0.001,
        )
        stop = asyncio.Event()
        runner = asyncio.create_task(worker.run(stop))
        await worker.enqueue(RecoveryRequired(stream, RecoveryReason.STALE, 1_000, 0))
        await asyncio.sleep(0.05)  # STALE attempt failed, retry is 30 s away
        asked_at = asyncio.get_running_loop().time()
        await worker.enqueue(RecoveryRequired(stream, RecoveryReason.DISCONNECT, 1_000, None))
        await asyncio.sleep(0.3)
        stop.set()
        await runner
        return recovery.times, asked_at

    times, asked_at = asyncio.run(scenario())

    assert len(times) >= 2
    assert times[1] - asked_at < 1.0
