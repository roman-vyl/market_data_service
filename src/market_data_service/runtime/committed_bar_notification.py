"""Bounded, single-consumer delivery of committed-bar notifications."""

from __future__ import annotations

import asyncio
import logging

from market_data_service.ports.committed_bar_notifier import (
    CommittedBarNotification,
    CommittedBarNotifier,
)


class CommittedBarNotificationWorker:
    """Deliver committed-bar notifications one at a time, off the event loop."""

    def __init__(
        self,
        notifier: CommittedBarNotifier,
        capacity: int,
        *,
        logger: logging.Logger | None = None,
    ) -> None:
        self._notifier = notifier
        self._capacity = capacity
        self._queue: asyncio.Queue[CommittedBarNotification] = asyncio.Queue(maxsize=capacity)
        self._logger = logger or logging.getLogger(
            "market_data_service.runtime.committed_bar_notification"
        )

    async def enqueue(self, notification: CommittedBarNotification) -> None:
        try:
            self._queue.put_nowait(notification)
        except asyncio.QueueFull:
            self._logger.error(
                "committed-bar notification queue full; dropping notification "
                "instrument=%s timeframe=%s open_time_ms=%s capacity=%s",
                notification.instrument,
                notification.timeframe,
                notification.open_time_ms,
                self._capacity,
            )

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                notification = await asyncio.wait_for(self._queue.get(), timeout=0.2)
            except TimeoutError:
                continue
            # No await between this check and starting the send below: stop_event
            # is set on this same loop, so a synchronous re-check here is exactly
            # equivalent to checking "right before the send begins".
            if stop_event.is_set():
                self._queue.task_done()
                return
            await self._send(notification)

    async def _send(self, notification: CommittedBarNotification) -> None:
        send_task: asyncio.Task[None] = asyncio.ensure_future(
            asyncio.to_thread(self._notifier.send, notification)
        )
        try:
            cancelled = await self._wait_for_send(send_task)
            try:
                send_task.result()
            except Exception as exc:
                self._log_delivery_failure(notification, exc)
            if cancelled:
                # Re-raise only after send_task has actually finished, so
                # the caller (run()) still stops, but never before the
                # in-flight HTTP thread's outcome has been observed.
                raise asyncio.CancelledError
        finally:
            self._queue.task_done()

    @staticmethod
    async def _wait_for_send(send_task: asyncio.Task[None]) -> bool:
        """Wait for send_task, re-shielding across any number of cancellations.

        Every await here goes through `asyncio.shield(send_task)`, never a
        bare `await send_task` — so no matter how many times the calling
        coroutine is cancelled while this is running (one cancel(), two,
        or more, e.g. from an application-wide shutdown following a sibling
        TaskGroup failure), send_task itself is never cancelled and the
        underlying `asyncio.to_thread(...)` call always runs to completion.
        Returns True if at least one cancellation was observed while
        waiting, so the caller can re-raise it after send_task is done.
        """
        cancelled = False
        while not send_task.done():
            try:
                await asyncio.shield(send_task)
            except asyncio.CancelledError:
                cancelled = True
            except Exception:
                pass  # retrieved and logged by the caller via .result()
        return cancelled

    def _log_delivery_failure(
        self,
        notification: CommittedBarNotification,
        error: Exception,
    ) -> None:
        self._logger.warning(
            "committed-bar notification delivery failed "
            "instrument=%s timeframe=%s open_time_ms=%s error=%s",
            notification.instrument,
            notification.timeframe,
            notification.open_time_ms,
            error,
        )
