from __future__ import annotations

import asyncio
import logging
import threading

import pytest

from market_data_service.ports.committed_bar_notifier import CommittedBarNotification
from market_data_service.runtime.committed_bar_notification import (
    CommittedBarNotificationWorker,
)


class RecordingNotifier:
    def __init__(self) -> None:
        self.calls: list[CommittedBarNotification] = []

    def send(self, notification: CommittedBarNotification) -> None:
        self.calls.append(notification)


class FailingNotifier:
    def __init__(self, error: Exception) -> None:
        self._error = error
        self.calls: list[CommittedBarNotification] = []

    def send(self, notification: CommittedBarNotification) -> None:
        self.calls.append(notification)
        raise self._error


class GatedNotifier:
    """Blocks send() on a thread-safe gate until release() is called."""

    def __init__(self) -> None:
        self._can_start = threading.Event()
        self._release = threading.Event()
        self.calls: list[CommittedBarNotification] = []

    def send(self, notification: CommittedBarNotification) -> None:
        self.calls.append(notification)
        self._can_start.set()
        self._release.wait(timeout=5)

    def wait_started(self) -> None:
        self._can_start.wait(timeout=5)

    def release(self) -> None:
        self._release.set()


def _notification(
    instrument: str = "BTCUSDT.P", open_time_ms: int = 0
) -> CommittedBarNotification:
    return CommittedBarNotification(
        instrument=instrument, timeframe="1m", open_time_ms=open_time_ms
    )


def test_worker_delivers_one_notification() -> None:
    asyncio.run(_deliver_one_scenario())


async def _deliver_one_scenario() -> None:
    notifier = RecordingNotifier()
    worker = CommittedBarNotificationWorker(notifier, capacity=4)
    stop = asyncio.Event()
    runner = asyncio.create_task(worker.run(stop))
    await worker.enqueue(_notification())
    for _ in range(200):
        if notifier.calls:
            break
        await asyncio.sleep(0.001)
    stop.set()
    await runner
    assert notifier.calls == [_notification()]


def test_worker_delivers_in_fifo_order() -> None:
    asyncio.run(_fifo_order_scenario())


async def _fifo_order_scenario() -> None:
    notifier = RecordingNotifier()
    worker = CommittedBarNotificationWorker(notifier, capacity=8)
    stop = asyncio.Event()
    runner = asyncio.create_task(worker.run(stop))
    for open_time_ms in (0, 60_000, 120_000):
        await worker.enqueue(_notification(open_time_ms=open_time_ms))
    for _ in range(200):
        if len(notifier.calls) == 3:
            break
        await asyncio.sleep(0.001)
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
    gated = GatedNotifier()
    worker = CommittedBarNotificationWorker(gated, capacity=1)
    stop = asyncio.Event()
    runner = asyncio.create_task(worker.run(stop))
    await worker.enqueue(_notification(open_time_ms=0))
    await asyncio.to_thread(gated.wait_started)
    # Queue is empty (item already dequeued) but the send is in flight; fill capacity=1.
    await worker.enqueue(_notification(open_time_ms=60_000))
    # This one must overflow: capacity is 1 and one item is already queued.
    await worker.enqueue(_notification(open_time_ms=120_000))
    gated.release()
    for _ in range(200):
        if len(gated.calls) == 2:
            break
        await asyncio.sleep(0.001)
    stop.set()
    await runner
    assert [call.open_time_ms for call in gated.calls] == [0, 60_000]


def test_worker_logs_and_continues_after_send_failure() -> None:
    asyncio.run(_send_failure_scenario())


async def _send_failure_scenario() -> None:
    notifier = FailingNotifier(RuntimeError("boom"))
    worker = CommittedBarNotificationWorker(notifier, capacity=4)
    stop = asyncio.Event()
    runner = asyncio.create_task(worker.run(stop))
    await worker.enqueue(_notification(open_time_ms=0))
    await worker.enqueue(_notification(open_time_ms=60_000))
    for _ in range(200):
        if len(notifier.calls) == 2:
            break
        await asyncio.sleep(0.001)
    stop.set()
    await runner
    assert [call.open_time_ms for call in notifier.calls] == [0, 60_000]


def test_at_most_one_send_in_flight_at_a_time() -> None:
    asyncio.run(_sequential_delivery_scenario())


async def _sequential_delivery_scenario() -> None:
    gated = GatedNotifier()
    worker = CommittedBarNotificationWorker(gated, capacity=4)
    stop = asyncio.Event()
    runner = asyncio.create_task(worker.run(stop))
    await worker.enqueue(_notification(open_time_ms=0))
    await worker.enqueue(_notification(open_time_ms=60_000))
    await asyncio.to_thread(gated.wait_started)
    await asyncio.sleep(0.02)
    assert len(gated.calls) == 1, "second notification must not start while first is in flight"
    gated.release()
    for _ in range(200):
        if len(gated.calls) == 2:
            break
        await asyncio.sleep(0.001)
    stop.set()
    await runner
    assert [call.open_time_ms for call in gated.calls] == [0, 60_000]


def test_event_loop_is_not_blocked_while_send_is_offloaded() -> None:
    asyncio.run(_non_blocking_loop_scenario())


async def _non_blocking_loop_scenario() -> None:
    gated = GatedNotifier()
    worker = CommittedBarNotificationWorker(gated, capacity=4)
    stop = asyncio.Event()
    runner = asyncio.create_task(worker.run(stop))
    await worker.enqueue(_notification())

    probe_ticks = 0

    async def probe() -> None:
        nonlocal probe_ticks
        for _ in range(20):
            await asyncio.sleep(0)
            probe_ticks += 1

    await asyncio.to_thread(gated.wait_started)
    await asyncio.wait_for(probe(), timeout=1)
    assert probe_ticks == 20, "event loop must keep scheduling other tasks during a send"

    gated.release()
    for _ in range(200):
        if gated.calls:
            break
        await asyncio.sleep(0.001)
    stop.set()
    await runner


def test_shutdown_while_idle_returns_promptly_without_draining_queue() -> None:
    asyncio.run(_idle_shutdown_scenario())


async def _idle_shutdown_scenario() -> None:
    notifier = RecordingNotifier()
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
    gated = GatedNotifier()
    worker = CommittedBarNotificationWorker(gated, capacity=4)
    stop = asyncio.Event()
    runner = asyncio.create_task(worker.run(stop))
    await worker.enqueue(_notification(open_time_ms=0))
    await worker.enqueue(_notification(open_time_ms=60_000))
    await asyncio.to_thread(gated.wait_started)

    stop.set()
    await asyncio.sleep(0.05)
    assert not runner.done(), "run() must wait for the in-flight send before exiting"

    gated.release()
    await asyncio.wait_for(runner, timeout=1)
    assert [call.open_time_ms for call in gated.calls] == [0]
