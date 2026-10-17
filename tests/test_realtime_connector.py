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


class DroppingSession:
    def __init__(self, transport: DroppingTransport) -> None:
        self._transport = transport

    async def send_text(self, message: str) -> None:
        return None

    async def receive_text(self) -> str:
        from market_data_service.ports.realtime import WebSocketDisconnected

        if self._transport.sessions >= self._transport.stop_after:
            self._transport.stop_event.set()
            await asyncio.sleep(3600)
        raise WebSocketDisconnected("dropped", code=1011, reason="keepalive ping timeout")

    async def close(self) -> None:
        return None


class DroppingTransport:
    """Every session connects, then drops; the last one is stopped cleanly."""

    def __init__(self, stop_event: asyncio.Event, stop_after: int) -> None:
        self.stop_event = stop_event
        self.stop_after = stop_after
        self.sessions = 0

    @asynccontextmanager
    async def connect(self, url: str) -> AsyncIterator[DroppingSession]:
        self.sessions += 1
        yield DroppingSession(self)


def _dropping_run(healthy_session_seconds: float) -> tuple[DroppingTransport, list[RealtimeEvent]]:
    async def scenario() -> tuple[DroppingTransport, list[RealtimeEvent]]:
        stop_event = asyncio.Event()
        transport = DroppingTransport(stop_event, stop_after=6)
        events: list[RealtimeEvent] = []

        async def on_event(event: RealtimeEvent) -> None:
            events.append(event)

        async def on_outcome(outcome: RealtimeIngestionOutcome) -> None:
            raise AssertionError(outcome)

        connector = RealtimeConnector(
            url="ws://dropping",
            transport=transport,  # type: ignore[arg-type]
            adapter=Adapter(),
            candle_handler=RealtimeCandleHandler(Ingestion(), lambda: 1),
            now_ms=lambda: 1,
            on_event=on_event,
            on_outcome=on_outcome,
            reconnect_policy=ReconnectPolicy(
                max_attempts=3, delay_seconds=0, healthy_session_seconds=healthy_session_seconds
            ),
        )
        await connector.run(stop_event)
        return transport, events

    return asyncio.run(scenario())


def test_healthy_sessions_reset_the_reconnect_budget() -> None:
    from market_data_service.application.realtime.events import ReconnectExhausted

    transport, events = _dropping_run(healthy_session_seconds=0)

    assert transport.sessions == 6  # more drops than max_attempts, still reconnecting
    assert not any(isinstance(event, ReconnectExhausted) for event in events)
    assert isinstance(events[-1], Stopped)


def test_sessions_that_drop_right_away_still_exhaust_the_budget() -> None:
    from market_data_service.application.realtime.events import ReconnectExhausted

    transport, events = _dropping_run(healthy_session_seconds=3600)

    assert transport.sessions == 3
    assert sum(isinstance(event, ReconnectExhausted) for event in events) == 1
    assert isinstance(events[-1], Stopped)
