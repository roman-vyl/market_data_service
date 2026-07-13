from __future__ import annotations

import asyncio

import pytest

from market_data_service.domain import InstrumentKey, StreamKey
from market_data_service.runtime.service import RuntimeService
from market_data_service.runtime.settings import RuntimeSettings
from market_data_service.runtime.startup_types import (
    StartupClassification,
    StartupStreamOutcome,
)
from market_data_service.runtime.status import RuntimeStatusStore


class FakeHttpServer:
    def __init__(self) -> None:
        self.started = False
        self.closed = False

    def start(self) -> None:
        self.started = True

    def close(self) -> None:
        self.closed = True


class FakeStartupCoordinator:
    def execute_stream(self, stream, window=None):  # type: ignore[no-untyped-def]
        raise AssertionError("no historical work expected")


class FailingRealtime:
    async def admit(self, stream):  # type: ignore[no-untyped-def]
        raise AssertionError("no admission expected")

    async def run(self, stop_event: asyncio.Event) -> None:
        raise RuntimeError("realtime worker failed")


class FailingRealtimeService(RuntimeService):
    def _startup(self):  # type: ignore[no-untyped-def]
        return FakeStartupCoordinator(), ()

    def _build_realtime(self, admitted_streams, operation_gate):  # type: ignore[no-untyped-def]
        return FailingRealtime()


class FailingHistoricalCoordinator:
    def execute_stream(self, stream, window=None):  # type: ignore[no-untyped-def]
        raise RuntimeError("historical worker failed")


class StoppableRealtime:
    def __init__(self) -> None:
        self.task: asyncio.Task[None] | None = None
        self.stopped = False

    async def admit(self, stream):  # type: ignore[no-untyped-def]
        raise AssertionError("no admission expected")

    async def run(self, stop_event: asyncio.Event) -> None:
        self.task = asyncio.current_task()
        try:
            await stop_event.wait()
        finally:
            self.stopped = True


class FailingHistoricalService(RuntimeService):
    def __init__(self, *, stream: StreamKey, **kwargs) -> None:  # type: ignore[no-untyped-def]
        super().__init__(**kwargs)
        self._stream = stream
        self.realtime = StoppableRealtime()

    def _startup(self):  # type: ignore[no-untyped-def]
        return (
            FailingHistoricalCoordinator(),
            (StartupStreamOutcome(self._stream, StartupClassification.INCOMPLETE),),
        )

    def _build_realtime(self, admitted_streams, operation_gate):  # type: ignore[no-untyped-def]
        return self.realtime


def test_runtime_service_marks_fatal_when_realtime_worker_fails() -> None:
    asyncio.run(_fatal_runtime_scenario())


async def _fatal_runtime_scenario() -> None:
    stream = StreamKey(InstrumentKey("BTCUSDT.P"), "1m")
    status = RuntimeStatusStore((stream,))
    http = FakeHttpServer()
    service = FailingRealtimeService(
        settings=RuntimeSettings(),
        config=object(),  # type: ignore[arg-type]
        wiring=object(),  # type: ignore[arg-type]
        status=status,
        http_server=http,  # type: ignore[arg-type]
    )

    with pytest.raises(RuntimeError, match="realtime worker failed"):
        await service.run(asyncio.Event())

    assert http.started
    assert http.closed
    assert status.health_document() == {
        "status": "unhealthy",
        "fatal_error": "RuntimeError: realtime worker failed",
    }


def test_runtime_service_stops_realtime_when_historical_worker_fails() -> None:
    asyncio.run(_fatal_historical_worker_scenario())


async def _fatal_historical_worker_scenario() -> None:
    stream = StreamKey(InstrumentKey("BTCUSDT.P"), "1m")
    status = RuntimeStatusStore((stream,))
    http = FakeHttpServer()
    stop_event = asyncio.Event()
    service = FailingHistoricalService(
        stream=stream,
        settings=RuntimeSettings(),
        config=object(),  # type: ignore[arg-type]
        wiring=object(),  # type: ignore[arg-type]
        status=status,
        http_server=http,  # type: ignore[arg-type]
    )

    with pytest.raises(RuntimeError, match="historical worker failed"):
        await service.run(stop_event)

    assert stop_event.is_set()
    assert http.started
    assert http.closed
    assert service.realtime.stopped
    assert service.realtime.task is not None
    assert service.realtime.task.done()
    assert status.health_document() == {
        "status": "unhealthy",
        "fatal_error": "RuntimeError: historical worker failed",
    }
