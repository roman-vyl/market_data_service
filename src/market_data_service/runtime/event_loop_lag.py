"""Event-loop lag monitor: logs when the loop was blocked longer than expected."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from time import perf_counter_ns

from market_data_service.runtime.loop_diagnostics import active_loop_diagnostics, diagnostic_tasks
from market_data_service.runtime.settings import RuntimeSettings

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
        diagnostics = active_loop_diagnostics.get()
        while not stop_event.is_set():
            started = loop.time()
            deadline_ns = (
                perf_counter_ns() + int(_TICK_SECONDS * 1_000_000_000)
                if diagnostics is not None
                else 0
            )
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=_TICK_SECONDS)
                return
            except TimeoutError:
                pass
            lag = loop.time() - started - _TICK_SECONDS
            if diagnostics is not None:
                diagnostics.tick(deadline_ns, perf_counter_ns(), warn=lag >= self._warn_seconds)
            elif lag >= self._warn_seconds:
                self._logger.warning("event loop lag %.2fs (loop was blocked)", lag)


@asynccontextmanager
async def runtime_tasks(
    stop_event: asyncio.Event, logger: logging.Logger, settings: RuntimeSettings
) -> AsyncIterator[asyncio.TaskGroup]:
    async with diagnostic_tasks(
        stop_event, logger, diagnostics_enabled=settings.loop_diagnostics_enabled
    ) as group:
        yield group
        group.create_task(
            EventLoopLagMonitor(logger, warn_seconds=settings.event_loop_lag_warn_seconds).run(
                stop_event
            )
        )
