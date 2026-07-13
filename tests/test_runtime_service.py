from __future__ import annotations

import asyncio

import pytest

from market_data_service.domain import InstrumentKey, StreamKey
from market_data_service.runtime.service import RuntimeService
from market_data_service.runtime.settings import RuntimeSettings
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
