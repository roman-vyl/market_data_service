from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from market_data_service.application.realtime.connector import RealtimeConnector, ReconnectPolicy
from market_data_service.application.realtime.events import RealtimeEvent, Stopped, TransportFailed
from market_data_service.application.realtime.handler import RealtimeCandleHandler
from market_data_service.application.realtime.outcomes import RealtimeIngestionOutcome


class FailingTransport:
    def __init__(self) -> None:
        self.attempts = 0

    @asynccontextmanager
    async def connect(self, url: str) -> AsyncIterator[object]:
        self.attempts += 1
        raise ConnectionError(f"connect failed: {url}")
        yield object()


class Adapter:
    def subscription_payload(self, request_id: str) -> str:
        return "{}"

    def unsubscription_payload(self, request_id: str) -> str:
        return "{}"

    def parse(self, raw: str, *, observed_at_ms: int) -> tuple[RealtimeEvent, ...]:
        return ()


class Ingestion:
    def execute(self, candle, *, committed_at_ms: int):  # type: ignore[no-untyped-def]
        raise AssertionError("not used")


def test_connector_exhausts_bounded_reconnect_and_stops() -> None:
    async def scenario() -> tuple[FailingTransport, list[RealtimeEvent]]:
        transport = FailingTransport()
        events: list[RealtimeEvent] = []

        async def on_event(event: RealtimeEvent) -> None:
            events.append(event)

        async def on_outcome(outcome: RealtimeIngestionOutcome) -> None:
            raise AssertionError(outcome)

        connector = RealtimeConnector(
            url="ws://unavailable",
            transport=transport,  # type: ignore[arg-type]
            adapter=Adapter(),
            candle_handler=RealtimeCandleHandler(Ingestion(), lambda: 1),
            now_ms=lambda: 1,
            on_event=on_event,
            on_outcome=on_outcome,
            reconnect_policy=ReconnectPolicy(max_attempts=2, delay_seconds=0),
        )
        await connector.run(asyncio.Event())
        return transport, events

    transport, events = asyncio.run(scenario())

    assert transport.attempts == 2
    assert sum(isinstance(event, TransportFailed) for event in events) == 2
    assert isinstance(events[-1], Stopped)


def test_connector_reports_reconnect_exhaustion() -> None:
    from market_data_service.application.realtime.events import ReconnectExhausted

    async def scenario() -> list[RealtimeEvent]:
        events: list[RealtimeEvent] = []

        async def on_event(event: RealtimeEvent) -> None:
            events.append(event)

        async def on_outcome(outcome: RealtimeIngestionOutcome) -> None:
            raise AssertionError(outcome)

        connector = RealtimeConnector(
            url="ws://unavailable",
            transport=FailingTransport(),  # type: ignore[arg-type]
            adapter=Adapter(),
            candle_handler=RealtimeCandleHandler(Ingestion(), lambda: 1),
            now_ms=lambda: 1,
            on_event=on_event,
            on_outcome=on_outcome,
            reconnect_policy=ReconnectPolicy(max_attempts=2, delay_seconds=0),
        )
        await connector.run(asyncio.Event())
        return events

    events = asyncio.run(scenario())

    assert sum(isinstance(event, ReconnectExhausted) for event in events) == 1
    assert isinstance(events[-1], Stopped)
