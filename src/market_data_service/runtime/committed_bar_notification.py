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
            await self._send(notification)

    async def _send(self, notification: CommittedBarNotification) -> None:
        try:
            await asyncio.to_thread(self._notifier.send, notification)
        except Exception as exc:
            self._logger.warning(
                "committed-bar notification delivery failed "
                "instrument=%s timeframe=%s open_time_ms=%s error=%s",
                notification.instrument,
                notification.timeframe,
                notification.open_time_ms,
                exc,
            )
