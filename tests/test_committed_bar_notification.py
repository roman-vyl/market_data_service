from __future__ import annotations

import asyncio
import logging
import threading

import pytest

from market_data_service.ports.committed_bar_notifier import CommittedBarNotification
from market_data_service.runtime.committed_bar_notification import (
    CommittedBarNotificationWorker,
)


class ScriptedNotifier:
    """Deterministic fake notifier: per-call started signal, optional gate/error.

    Each call index has a `started` event set synchronously when `send()`
    begins. If that index is in `gated_indices`, the call blocks on a
    per-index `release` event until the test releases it. If an exception is
    scripted for that index, it is raised after any gating. Tests use
    `wait_started(index)`/`release(index)` instead of sleep-based polling to
    observe and control exactly when each call starts and completes.
    """

    def __init__(
        self,
        count: int,
        *,
        gated_indices: frozenset[int] = frozenset(),
        errors: dict[int, Exception] | None = None,
    ) -> None:
        self.calls: list[CommittedBarNotification] = []
        self._started = [threading.Event() for _ in range(count)]
        self._release = [threading.Event() for _ in range(count)]
        self._gated_indices = gated_indices
        self._errors = errors or {}
        self._index = 0
        self._lock = threading.Lock()

    def send(self, notification: CommittedBarNotification) -> None:
        with self._lock:
            index = self._index
            self._index += 1
        self.calls.append(notification)
        self._started[index].set()
        if index in self._gated_indices:
            assert self._release[index].wait(timeout=5), f"call {index} was never released"
        if index in self._errors:
            raise self._errors[index]

    def wait_started(self, index: int) -> None:
        assert self._started[index].wait(timeout=5), f"call {index} did not start in time"

    def release(self, index: int) -> None:
        self._release[index].set()


def _notification(instrument: str = "BTCUSDT.P", open_time_ms: int = 0) -> CommittedBarNotification:
    return CommittedBarNotification(
        instrument=instrument, timeframe="1m", open_time_ms=open_time_ms
    )


def test_worker_delivers_one_notification() -> None:
    asyncio.run(_deliver_one_scenario())


async def _deliver_one_scenario() -> None:
    notifier = ScriptedNotifier(1, gated_indices=frozenset({0}))
    worker = CommittedBarNotificationWorker(notifier, capacity=4)
    stop = asyncio.Event()
    runner = asyncio.create_task(worker.run(stop))
    await worker.enqueue(_notification())
    await asyncio.to_thread(notifier.wait_started, 0)
    notifier.release(0)
    stop.set()
    await runner
    assert notifier.calls == [_notification()]


def test_worker_delivers_in_fifo_order() -> None:
    asyncio.run(_fifo_order_scenario())


async def _fifo_order_scenario() -> None:
    notifier = ScriptedNotifier(3, gated_indices=frozenset({0, 1, 2}))
    worker = CommittedBarNotificationWorker(notifier, capacity=8)
    stop = asyncio.Event()
    runner = asyncio.create_task(worker.run(stop))
    for open_time_ms in (0, 60_000, 120_000):
        await worker.enqueue(_notification(open_time_ms=open_time_ms))

    for index in range(3):
        await asyncio.to_thread(notifier.wait_started, index)
        notifier.release(index)

    stop.set()
    await runner
    assert [call.open_time_ms for call in notifier.calls] == [0, 60_000, 120_000]


def test_worker_drops_notification_when_queue_is_full(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.ERROR, logger="market_data_service.runtime.committed_bar_notification")
    asyncio.run(_overflow_scenario())
    assert "instrument=BTCUSDT.P" in caplog.text
    assert "capacity=1" in caplog.text


async def _overflow_scenario() -> None:
    notifier = ScriptedNotifier(2, gated_indices=frozenset({0}))
    worker = CommittedBarNotificationWorker(notifier, capacity=1)
    stop = asyncio.Event()
    runner = asyncio.create_task(worker.run(stop))
    await worker.enqueue(_notification(open_time_ms=0))
    await asyncio.to_thread(notifier.wait_started, 0)
    # Queue is empty (item already dequeued) but the send is in flight; fill capacity=1.
    await worker.enqueue(_notification(open_time_ms=60_000))
    # This one must overflow: capacity is 1 and one item is already queued.
    await worker.enqueue(_notification(open_time_ms=120_000))
    notifier.release(0)
    await asyncio.to_thread(notifier.wait_started, 1)
    stop.set()
    await runner
    assert [call.open_time_ms for call in notifier.calls] == [0, 60_000]


def test_worker_logs_and_continues_after_send_failure() -> None:
    asyncio.run(_send_failure_scenario())


async def _send_failure_scenario() -> None:
    notifier = ScriptedNotifier(2, errors={0: RuntimeError("boom")})
    worker = CommittedBarNotificationWorker(notifier, capacity=4)
    stop = asyncio.Event()
    runner = asyncio.create_task(worker.run(stop))
    await worker.enqueue(_notification(open_time_ms=0))
    await worker.enqueue(_notification(open_time_ms=60_000))
    await asyncio.to_thread(notifier.wait_started, 1)
    stop.set()
    await runner
    assert [call.open_time_ms for call in notifier.calls] == [0, 60_000]


def test_at_most_one_send_in_flight_at_a_time() -> None:
    asyncio.run(_sequential_delivery_scenario())


async def _sequential_delivery_scenario() -> None:
    notifier = ScriptedNotifier(2, gated_indices=frozenset({0, 1}))
    worker = CommittedBarNotificationWorker(notifier, capacity=4)
    stop = asyncio.Event()
    runner = asyncio.create_task(worker.run(stop))
    await worker.enqueue(_notification(open_time_ms=0))
    await worker.enqueue(_notification(open_time_ms=60_000))

    await asyncio.to_thread(notifier.wait_started, 0)
    assert len(notifier.calls) == 1, "second notification must not start while first is in flight"

    notifier.release(0)
    await asyncio.to_thread(notifier.wait_started, 1)
    assert [call.open_time_ms for call in notifier.calls] == [0, 60_000]

    notifier.release(1)
    stop.set()
    await runner


def test_event_loop_is_not_blocked_while_send_is_offloaded() -> None:
    asyncio.run(_non_blocking_loop_scenario())


async def _non_blocking_loop_scenario() -> None:
    notifier = ScriptedNotifier(1, gated_indices=frozenset({0}))
    worker = CommittedBarNotificationWorker(notifier, capacity=4)
    stop = asyncio.Event()
    runner = asyncio.create_task(worker.run(stop))
    await worker.enqueue(_notification())

    probe_ticks = 0

    async def probe() -> None:
        nonlocal probe_ticks
        for _ in range(20):
            await asyncio.sleep(0)
            probe_ticks += 1

    await asyncio.to_thread(notifier.wait_started, 0)
    await asyncio.wait_for(probe(), timeout=1)
    assert probe_ticks == 20, "event loop must keep scheduling other tasks during a send"

    notifier.release(0)
    stop.set()
    await runner
    assert notifier.calls == [_notification()]


def test_shutdown_while_idle_returns_promptly_without_draining_queue() -> None:
    asyncio.run(_idle_shutdown_scenario())


async def _idle_shutdown_scenario() -> None:
    notifier = ScriptedNotifier(0)
    worker = CommittedBarNotificationWorker(notifier, capacity=4)
    stop = asyncio.Event()
    runner = asyncio.create_task(worker.run(stop))
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(runner, timeout=0.5)
    assert notifier.calls == []


def test_shutdown_waits_for_in_flight_send_and_sends_no_further_item() -> None:
    asyncio.run(_in_flight_shutdown_scenario())


async def _in_flight_shutdown_scenario() -> None:
    notifier = ScriptedNotifier(2, gated_indices=frozenset({0}))
    worker = CommittedBarNotificationWorker(notifier, capacity=4)
    stop = asyncio.Event()
    runner = asyncio.create_task(worker.run(stop))
    await worker.enqueue(_notification(open_time_ms=0))
    await worker.enqueue(_notification(open_time_ms=60_000))
    await asyncio.to_thread(notifier.wait_started, 0)

    stop.set()
    await asyncio.sleep(0.05)
    assert not runner.done(), "run() must wait for the in-flight send before exiting"

    notifier.release(0)
    await asyncio.wait_for(runner, timeout=1)
    assert [call.open_time_ms for call in notifier.calls] == [0]


def test_stop_event_set_between_dequeue_and_send_discards_the_item() -> None:
    """The dequeue-vs-stop race: stop_event wins before the send starts."""
    asyncio.run(_dequeue_vs_stop_race_scenario())


async def _dequeue_vs_stop_race_scenario() -> None:
    notifier = ScriptedNotifier(0)
    worker = CommittedBarNotificationWorker(notifier, capacity=4)
    stop = asyncio.Event()
    runner = asyncio.create_task(worker.run(stop))
    # Give the worker one scheduling opportunity to reach its queue.get()
    # suspension point before we act; enqueue() and stop.set() below contain
    # no awaits of their own, so nothing yields control back to the worker
    # between them.
    await asyncio.sleep(0)

    await worker.enqueue(_notification())
    stop.set()

    await asyncio.wait_for(runner, timeout=1)
    assert notifier.calls == []


def test_cancellation_waits_for_in_flight_send_then_reraises() -> None:
    asyncio.run(_cancellation_safe_send_scenario())


async def _cancellation_safe_send_scenario() -> None:
    notifier = ScriptedNotifier(2, gated_indices=frozenset({0}))
    worker = CommittedBarNotificationWorker(notifier, capacity=4)
    stop = asyncio.Event()
    runner = asyncio.create_task(worker.run(stop))
    await worker.enqueue(_notification(open_time_ms=0))
    await worker.enqueue(_notification(open_time_ms=60_000))
    await asyncio.to_thread(notifier.wait_started, 0)

    runner.cancel()
    await asyncio.sleep(0)  # let the scheduled cancellation reach the shield() await
    assert not runner.done(), "cancellation must wait for the in-flight send, not abandon it"

    notifier.release(0)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(runner), timeout=1)

    assert [call.open_time_ms for call in notifier.calls] == [0]


def test_repeated_cancellation_still_waits_for_the_in_flight_send() -> None:
    """A second cancel() while already waiting must not orphan the HTTP thread.

    The first CancelledError is caught by _send()'s shielded wait; if the
    subsequent wait for send_task's actual completion were itself an
    unshielded `await send_task`, a second cancel() arriving during that
    wait would cancel send_task directly, leaving the underlying
    asyncio.to_thread(...) OS thread running unobserved. This exercises
    exactly that timing: two cancel() calls before the gate is released.
    """
    asyncio.run(_repeated_cancellation_scenario())


async def _repeated_cancellation_scenario() -> None:
    notifier = ScriptedNotifier(2, gated_indices=frozenset({0}))
    worker = CommittedBarNotificationWorker(notifier, capacity=4)
    stop = asyncio.Event()
    runner = asyncio.create_task(worker.run(stop))
    await worker.enqueue(_notification(open_time_ms=0))
    await worker.enqueue(_notification(open_time_ms=60_000))
    await asyncio.to_thread(notifier.wait_started, 0)

    runner.cancel()
    with pytest.raises(TimeoutError):
        # Bounded wait, not a fixed yield count: give the first cancellation's
        # propagation through the shielded await as much real scheduler time
        # as it needs, and prove the worker is still genuinely blocked on the
        # in-flight send rather than merely "not yet scheduled".
        await asyncio.wait_for(asyncio.shield(runner), timeout=0.2)

    runner.cancel()
    with pytest.raises(TimeoutError):
        # Same bounded proof for the second cancel(): a second cancel() must
        # not abandon the in-flight send by cancelling send_task itself.
        await asyncio.wait_for(asyncio.shield(runner), timeout=0.2)

    notifier.release(0)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(runner), timeout=1)

    assert [call.open_time_ms for call in notifier.calls] == [0]


def test_worker_calls_task_done_exactly_once_per_dequeued_item() -> None:
    asyncio.run(_task_done_bookkeeping_scenario())


async def _task_done_bookkeeping_scenario() -> None:
    notifier = ScriptedNotifier(2, errors={1: RuntimeError("boom")})
    worker = CommittedBarNotificationWorker(notifier, capacity=4)
    stop = asyncio.Event()
    runner = asyncio.create_task(worker.run(stop))
    await worker.enqueue(_notification(open_time_ms=0))
    await worker.enqueue(_notification(open_time_ms=60_000))

    # worker._queue.join() resolves only once task_done() has been called
    # exactly once for every put() — a double-call would make it resolve
    # too early relative to actual completion, and a missed call would make
    # it hang. Bounding by wait_for is a safety net, not the proof itself.
    await asyncio.wait_for(worker._queue.join(), timeout=1)

    stop.set()
    await runner
    assert [call.open_time_ms for call in notifier.calls] == [0, 60_000]
