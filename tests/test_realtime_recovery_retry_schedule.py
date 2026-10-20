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
        self.requests: list[RealtimeRecoveryRequest] = []

    async def execute(self, request: RealtimeRecoveryRequest) -> RealtimeRecoveryResult:
        self.times.append(asyncio.get_running_loop().time())
        self.requests.append(request)
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


class GatedRecovery(TimedRecovery):
    """The first call blocks until the test releases it."""

    def __init__(self, results: list[RecoveryClassification | str]) -> None:
        super().__init__(results)
        self.started = asyncio.Event()
        self.release = asyncio.Event()
        self._block_first = True

    async def execute(self, request: RealtimeRecoveryRequest) -> RealtimeRecoveryResult:
        if self._block_first:
            self._block_first = False
            self.requests.append(request)
            self.started.set()
            await self.release.wait()
            self.requests.pop()
        return await super().execute(request)


STREAM = StreamKey(InstrumentKey("BTCUSDT.P"), "5m")


def _worker(recovery: object, *, base: float = BASE, cap: float = MAX) -> RealtimeRecoveryWorker:
    return RealtimeRecoveryWorker(
        recovery=recovery,  # type: ignore[arg-type]
        supervisor=RealtimeSupervisor((STREAM,), {}, lambda: 1_000),
        status=RuntimeStatusStore((STREAM,)),
        operation_gate=asyncio.Lock(),
        sync_lifecycle=lambda: None,
        max_windows=1,
        base_backoff_seconds=base,
        max_backoff_seconds=cap,
        stale_first_retry_seconds=30.0,
        stale_next_retry_seconds=60.0,
        idle_seconds=0.001,
    )


def _signal(reason: RecoveryReason, hint: int | None = None) -> RecoveryRequired:
    return RecoveryRequired(STREAM, reason, 1_000, hint)


async def _until(condition: object, timeout: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not condition():  # type: ignore[operator]
        assert loop.time() < deadline, "condition not reached"
        await asyncio.sleep(0.005)


def test_urgent_signal_escalates_waiting_background_retry_and_resets_state() -> None:
    async def scenario() -> None:
        recovery = TimedRecovery(["advanced", F, R])
        worker = _worker(recovery)
        stop = asyncio.Event()
        runner = asyncio.create_task(worker.run(stop))
        await worker.enqueue(_signal(RecoveryReason.STALE, 0))
        await _until(lambda: len(recovery.times) == 2)
        await asyncio.sleep(0.02)
        intent = worker._intents[STREAM]
        assert not intent.urgent
        assert intent.recovery_window == ADVANCED
        assert intent.attempts_without_progress == 1
        assert intent.due_at > asyncio.get_running_loop().time() + 10

        await worker.enqueue(_signal(RecoveryReason.DISCONNECT))
        # No await since the merge: the worker cannot have run yet.
        assert worker._intents[STREAM] is intent
        assert intent.urgent
        assert intent.reason is RecoveryReason.DISCONNECT
        assert intent.revision == 1
        assert intent.recovery_window is None
        assert intent.progress_marker is None
        assert intent.attempts_without_progress == 0
        assert intent.due_at == 0.0

        await _until(lambda: len(recovery.times) == 3)
        await _until(lambda: not worker._intents)
        stop.set()
        await runner

    asyncio.run(scenario())


def test_duplicate_urgent_signal_changes_nothing_and_keeps_backoff() -> None:
    async def scenario() -> None:
        recovery = TimedRecovery([F, R])
        worker = _worker(recovery, base=5.0, cap=10.0)
        stop = asyncio.Event()
        runner = asyncio.create_task(worker.run(stop))
        await worker.enqueue(_signal(RecoveryReason.DISCONNECT))
        await _until(lambda: len(recovery.times) == 1)
        await asyncio.sleep(0.02)
        intent = worker._intents[STREAM]
        before = (intent.revision, intent.due_at, intent.attempts_without_progress)
        assert before[1] > asyncio.get_running_loop().time() + 3

        for reason in (
            RecoveryReason.DISCONNECT,
            RecoveryReason.STARTUP_RECONCILIATION,
            RecoveryReason.SEQUENCE_DISCONTINUITY,
        ):
            await worker.enqueue(_signal(reason))
            assert (intent.revision, intent.due_at, intent.attempts_without_progress) == before
        await asyncio.sleep(0.05)
        assert len(recovery.times) == 1  # a flood of duplicates does not speed up retries
        stop.set()
        await runner

    asyncio.run(scenario())


def test_earliest_only_moves_earlier_and_only_then_wakes_the_intent() -> None:
    async def scenario() -> None:
        worker = _worker(TimedRecovery([]))
        await worker.enqueue(_signal(RecoveryReason.DISCONNECT))
        intent = worker._intents[STREAM]
        assert (intent.earliest_open_ms, intent.revision) == (None, 0)

        await worker.enqueue(_signal(RecoveryReason.SEQUENCE_DISCONTINUITY, 200))
        assert (intent.earliest_open_ms, intent.revision) == (200, 1)
        await worker.enqueue(_signal(RecoveryReason.SEQUENCE_DISCONTINUITY, 300))
        assert (intent.earliest_open_ms, intent.revision) == (200, 1)
        intent.due_at = 99.0
        await worker.enqueue(_signal(RecoveryReason.SEQUENCE_DISCONTINUITY, 100))
        assert (intent.earliest_open_ms, intent.revision, intent.due_at) == (100, 2, 0.0)

    asyncio.run(scenario())


def test_event_during_running_attempt_forces_another_pass_with_the_hint() -> None:
    async def scenario() -> None:
        recovery = GatedRecovery([R, R])
        worker = _worker(recovery)
        stop = asyncio.Event()
        runner = asyncio.create_task(worker.run(stop))
        await worker.enqueue(_signal(RecoveryReason.STALE, 600))
        await asyncio.wait_for(recovery.started.wait(), timeout=2)
        intent = worker._intents[STREAM]

        await worker.enqueue(_signal(RecoveryReason.SEQUENCE_DISCONTINUITY, 123))
        assert worker._intents[STREAM] is intent  # merged, nothing replaced or started

        recovery.release.set()
        await _until(lambda: len(recovery.requests) == 2)
        await _until(lambda: not worker._intents)
        stop.set()
        await runner

        # The first RESTORED did not finish the recovery: a second pass ran for the new hint.
        second = recovery.requests[1]
        assert second.signal.reason is RecoveryReason.SEQUENCE_DISCONTINUITY
        assert second.signal.suspected_start_time_ms == 123
        assert second.recovery_window is None

    asyncio.run(scenario())


def test_unchanged_intent_keeps_incomplete_continuation_cursor() -> None:
    async def scenario() -> None:
        recovery = GatedRecovery(["advanced", R])
        worker = _worker(recovery)
        stop = asyncio.Event()
        runner = asyncio.create_task(worker.run(stop))
        await worker.enqueue(_signal(RecoveryReason.DISCONNECT))
        await asyncio.wait_for(recovery.started.wait(), timeout=2)
        recovery.release.set()
        await _until(lambda: not worker._intents)
        stop.set()
        await runner

        assert [r.recovery_window for r in recovery.requests] == [None, ADVANCED]

    asyncio.run(scenario())


def test_changed_intent_drops_the_old_cursor() -> None:
    async def scenario() -> None:
        recovery = GatedRecovery(["advanced", R])
        worker = _worker(recovery)
        stop = asyncio.Event()
        runner = asyncio.create_task(worker.run(stop))
        await worker.enqueue(_signal(RecoveryReason.DISCONNECT))
        await asyncio.wait_for(recovery.started.wait(), timeout=2)
        await worker.enqueue(_signal(RecoveryReason.SEQUENCE_DISCONTINUITY, 50))
        recovery.release.set()
        await _until(lambda: not worker._intents)
        stop.set()
        await runner

        assert [r.recovery_window for r in recovery.requests] == [None, None]
        assert recovery.requests[1].signal.suspected_start_time_ms == 50

    asyncio.run(scenario())
