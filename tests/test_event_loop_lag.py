from __future__ import annotations

import asyncio
import logging
import time

import pytest

from market_data_service.runtime.event_loop_lag import EventLoopLagMonitor


def test_monitor_logs_when_loop_is_blocked(caplog: pytest.LogCaptureFixture) -> None:
    async def scenario() -> None:
        stop = asyncio.Event()
        monitor = EventLoopLagMonitor(logging.getLogger("lag-test"), warn_seconds=0.2)
        task = asyncio.create_task(monitor.run(stop))
        await asyncio.sleep(0.1)
        time.sleep(0.9)
        await asyncio.sleep(0.7)
        stop.set()
        await task

    with caplog.at_level(logging.WARNING, logger="lag-test"):
        asyncio.run(scenario())
    assert any("event loop lag" in r.message for r in caplog.records)


def test_monitor_quiet_when_loop_is_free(caplog: pytest.LogCaptureFixture) -> None:
    async def scenario() -> None:
        stop = asyncio.Event()
        monitor = EventLoopLagMonitor(logging.getLogger("lag-test"), warn_seconds=5.0)
        task = asyncio.create_task(monitor.run(stop))
        await asyncio.sleep(0.8)
        stop.set()
        await task

    with caplog.at_level(logging.WARNING, logger="lag-test"):
        asyncio.run(scenario())
    assert not caplog.records
