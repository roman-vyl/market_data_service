"""Recovery episode flag: one notification for the latest bar a restored episode completed."""

from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

from tests.test_runtime_realtime import (
    RecordingNotifierWorker,
    _insert_candle,
    _register_connecting,
    _runtime,
    _stream,
)

from market_data_service.adapters.sqlite import initialize_database
from market_data_service.application.realtime.events import (
    RecoveryReason,
    RecoveryRequired,
    SubscriptionConfirmed,
)
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
from market_data_service.domain.identity import InstrumentKey, StreamKey
from market_data_service.domain.windows import TimeWindow
from market_data_service.ports.committed_bar_notifier import CommittedBarNotification
from market_data_service.runtime.realtime_recovery_worker import RealtimeRecoveryWorker
from market_data_service.runtime.status import RuntimeStatusStore

STREAM = StreamKey(InstrumentKey("BTCUSDT.P"), "5m")
R, INC, F, X = (
    RecoveryClassification.RESTORED,
    RecoveryClassification.INCOMPLETE,
    RecoveryClassification.RECOVERABLE_FAILURE,
    RecoveryClassification.FATAL_FAILURE,
)
ADVANCED = TimeWindow(300_000, 600_000)


class Scripted:
    """Recovery results as (classification, committed, restored_through)."""

    def __init__(self, script: list[tuple[RecoveryClassification, int, int | None]]) -> None:
        self._script = list(script)
        self.gate: asyncio.Event | None = None
        self.sync_count_at_execute: list[int] = []
        self.sync_count = lambda: 0

    async def execute(self, request: RealtimeRecoveryRequest) -> RealtimeRecoveryResult:
        if self.gate is not None:
            await self.gate.wait()
        self.sync_count_at_execute.append(self.sync_count())
        classification, committed, through = self._script.pop(0)
        return RealtimeRecoveryResult(
            stream=request.signal.stream,
            classification=classification,
            recovery_window=None,
            repair=SimpleNamespace(  # type: ignore[arg-type]
                attempted_windows=1,
                completed_windows=1,
                progress_marker=None,
                window_results=(
                    SimpleNamespace(
                        committed=committed, duplicates=0, corrected=0, rejected=0, unexpected=0
                    ),
                ),
            ),
            next_recovery_window=ADVANCED if classification is INC else None,
            restored_through_open_time_ms=through if classification is R else None,
            error_code="temporary_source_failure" if classification is F else None,
        )


class Harness:
    def __init__(self, script: list[tuple[RecoveryClassification, int, int | None]]) -> None:
        self.recovery = Scripted(script)
        self.syncs = 0
        self.notified: list[tuple[int, int]] = []
        self.recovery.sync_count = lambda: self.syncs
        self.worker = RealtimeRecoveryWorker(
            recovery=self.recovery,  # type: ignore[arg-type]
            supervisor=RealtimeSupervisor((STREAM,), {}, lambda: 1_000),
            status=RuntimeStatusStore((STREAM,)),
            operation_gate=asyncio.Lock(),
            sync_lifecycle=self._sync,
            max_windows=1,
            base_backoff_seconds=0.001,
            max_backoff_seconds=0.001,
            stale_first_retry_seconds=0.001,
            stale_next_retry_seconds=0.001,
            idle_seconds=0.001,
            on_restored=self._restored,
        )

    def _sync(self) -> None:
        self.syncs += 1

    async def _restored(self, stream: StreamKey, open_time_ms: int) -> None:
        assert stream == STREAM
        self.notified.append((open_time_ms, self.syncs))

    async def run(
        self,
        reason: RecoveryReason = RecoveryReason.DISCONNECT,
        *,
        before_release: object = None,
    ) -> None:
        stop = asyncio.Event()
        runner = asyncio.create_task(self.worker.run(stop))
        self.recovery.gate = asyncio.Event()
        await self.worker.enqueue(RecoveryRequired(STREAM, reason, 1_000, 0))
        if before_release is not None:
            before_release()  # type: ignore[operator]
        self.recovery.gate.set()
        for _ in range(400):
            if not self.recovery._script:
                break
            await asyncio.sleep(0.005)
        await asyncio.sleep(0.03)
        stop.set()
        await runner


def _run(script: list[tuple[RecoveryClassification, int, int | None]], **kwargs: object) -> Harness:
    harness = Harness(script)
    asyncio.run(harness.run(**kwargs))  # type: ignore[arg-type]
    return harness


def test_commit_in_an_earlier_attempt_notifies_after_a_restored_zero_attempt() -> None:
    harness = _run([(INC, 1, None), (R, 0, 600_000)])

    assert [open_ms for open_ms, _ in harness.notified] == [600_000]


def test_lost_bar_restored_over_rest_notifies_once_for_the_latest_bar() -> None:
    harness = _run([(R, 10, 3_000_000)])

    assert [open_ms for open_ms, _ in harness.notified] == [3_000_000]


def test_notification_comes_after_lifecycle_sync_of_the_restored_attempt() -> None:
    harness = _run([(R, 2, 600_000)])

    ((_, syncs_at_notification),) = harness.notified
    assert syncs_at_notification > harness.recovery.sync_count_at_execute[-1]


def test_restored_episode_without_any_commit_stays_silent() -> None:
    assert _run([(R, 0, 600_000)]).notified == []


def test_websocket_commit_marks_the_active_episode() -> None:
    harness = Harness([(R, 0, 600_000)])

    async def scenario() -> None:
        await harness.run(before_release=lambda: harness.worker.mark_live_commit(STREAM))

    asyncio.run(scenario())

    assert [open_ms for open_ms, _ in harness.notified] == [600_000]


def test_websocket_commit_without_an_active_episode_marks_nothing_and_does_not_leak() -> None:
    harness = Harness([(R, 0, 600_000)])
    harness.worker.mark_live_commit(STREAM)  # no episode yet

    asyncio.run(harness.run())

    assert harness.notified == []


def test_finished_episode_is_not_marked_and_the_next_one_starts_clean() -> None:
    harness = Harness([(R, 1, 600_000), (R, 0, 900_000)])

    async def scenario() -> None:
        await harness.run()
        harness.worker.mark_live_commit(STREAM)  # episode already over
        harness.recovery.gate = None
        stop = asyncio.Event()
        runner = asyncio.create_task(harness.worker.run(stop))
        await harness.worker.enqueue(RecoveryRequired(STREAM, RecoveryReason.DISCONNECT, 1_000, 0))
        await asyncio.sleep(0.05)
        stop.set()
        await runner

    asyncio.run(scenario())

    assert [open_ms for open_ms, _ in harness.notified] == [600_000]


def test_flag_survives_incomplete_and_recoverable_failure_until_restored() -> None:
    harness = _run([(INC, 1, None), (F, 0, None), (R, 0, 900_000)])

    assert [open_ms for open_ms, _ in harness.notified] == [900_000]


def test_fatal_failure_ends_the_episode_without_notification() -> None:
    assert _run([(INC, 3, None), (X, 0, None)]).notified == []


class NotifierWithRun(RecordingNotifierWorker):
    async def run(self, stop_event: asyncio.Event) -> None:
        await stop_event.wait()


def test_sequence_gap_bar_creates_and_marks_its_episode(tmp_path: Path) -> None:
    asyncio.run(_gap_scenario(tmp_path))


async def _gap_scenario(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite3"
    initialize_database(path)
    stream = _stream("BTCUSDT.P")
    _register_connecting(path, stream)
    _insert_candle(path, stream)
    recovery = Scripted([(R, 0, 0), (R, 0, 120_000)])
    notifier = NotifierWithRun()
    runtime, _ = _runtime(
        path,
        (stream,),
        recovery,
        notifier_worker=notifier,  # type: ignore[arg-type]
    )
    stop = asyncio.Event()
    runner = asyncio.create_task(runtime.run(stop))
    await runtime.on_outcome(
        RealtimeIngestionOutcome(stream, 0, RealtimeIngestionClassification.COMMITTED)
    )
    await runtime.on_event(SubscriptionConfirmed(("kline.1.BTCUSDT",), observed_at_ms=10))
    await asyncio.sleep(0.1)  # startup episode: restored, nothing committed, no live bar
    assert notifier.notifications == [
        CommittedBarNotification(instrument="BTCUSDT.P", timeframe="1m", open_time_ms=0)
    ]

    # A WebSocket bar that skips 60_000 reveals a sequence gap and opens a new episode.
    notifier.notifications.clear()
    await runtime.on_outcome(
        RealtimeIngestionOutcome(stream, 120_000, RealtimeIngestionClassification.COMMITTED)
    )
    await asyncio.sleep(0.1)
    stop.set()
    await runner

    assert [n.open_time_ms for n in notifier.notifications] == [120_000, 120_000]


def test_restored_episode_without_notifier_worker_is_a_no_op(tmp_path: Path) -> None:
    async def scenario() -> None:
        path = tmp_path / "market.sqlite3"
        initialize_database(path)
        stream = _stream("BTCUSDT.P")
        _register_connecting(path, stream)
        recovery = Scripted([(R, 5, 60_000)])
        runtime, _status = _runtime(path, (stream,), recovery)  # type: ignore[arg-type]
        stop = asyncio.Event()
        runner = asyncio.create_task(runtime.run(stop))
        await runtime.on_event(SubscriptionConfirmed(("kline.1.BTCUSDT",), observed_at_ms=10))
        await asyncio.sleep(0.1)
        stop.set()
        await runner

        assert recovery._script == []

    asyncio.run(scenario())
