"""Event-loop lag monitor: logs when the loop was blocked longer than expected."""

from __future__ import annotations

import asyncio
import logging

_TICK_SECONDS = 0.5


class EventLoopLagMonitor:
    """Sleeps in short ticks and warns when a tick overshoots by `warn_seconds`.

    A blocked loop cannot answer WebSocket keepalive pings, so lag is the
    observable precursor of a `keepalive ping timeout` disconnect.
    """

    def __init__(self, logger: logging.Logger, *, warn_seconds: float) -> None:
        self._logger = logger
        self._warn_seconds = warn_seconds

    async def run(self, stop_event: asyncio.Event) -> None:
        loop = asyncio.get_running_loop()
        while not stop_event.is_set():
            started = loop.time()
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=_TICK_SECONDS)
                return
            except TimeoutError:
                pass
            lag = loop.time() - started - _TICK_SECONDS
            if lag >= self._warn_seconds:
                self._logger.warning("event loop lag %.2fs (loop was blocked)", lag)
