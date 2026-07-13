"""Cancellable bounded WebSocket connection lifecycle."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol

from market_data_service.application.realtime.events import (
    CandleObserved,
    Connected,
    Disconnected,
    RealtimeEvent,
    ReconnectExhausted,
    Stopped,
    TransportFailed,
)
from market_data_service.application.realtime.outcomes import RealtimeIngestionOutcome
from market_data_service.ports.realtime import WebSocketDisconnected, WebSocketTransport


class RealtimeCandleEventHandler(Protocol):
    def handle(self, event: CandleObserved) -> RealtimeIngestionOutcome | None: ...


class RealtimeProtocolAdapter(Protocol):
    def subscription_payload(self, request_id: str) -> str: ...
    def unsubscription_payload(self, request_id: str) -> str: ...
    def parse(self, raw: str, *, observed_at_ms: int) -> tuple[RealtimeEvent, ...]: ...


@dataclass(frozen=True, slots=True)
class ReconnectPolicy:
    max_attempts: int = 3
    delay_seconds: float = 1.0

    def __post_init__(self) -> None:
        if self.max_attempts <= 0:
            raise ValueError("max_attempts must be positive")
        if self.delay_seconds < 0:
            raise ValueError("delay_seconds must be non-negative")


class RealtimeConnector:
    def __init__(
        self,
        *,
        url: str,
        transport: WebSocketTransport,
        adapter: RealtimeProtocolAdapter,
        candle_handler: RealtimeCandleEventHandler,
        now_ms: Callable[[], int],
        on_event: Callable[[RealtimeEvent], Awaitable[None]],
        on_outcome: Callable[[RealtimeIngestionOutcome], Awaitable[None]],
        reconnect_policy: ReconnectPolicy | None = None,
    ) -> None:
        self._url = url
        self._transport = transport
        self._adapter = adapter
        self._candle_handler = candle_handler
        self._now_ms = now_ms
        self._on_event = on_event
        self._on_outcome = on_outcome
        self._reconnect_policy = reconnect_policy or ReconnectPolicy()

    async def run(self, stop_event: asyncio.Event) -> None:
        exhausted = True
        for attempt in range(1, self._reconnect_policy.max_attempts + 1):
            if stop_event.is_set():
                exhausted = False
                break
            try:
                await self._run_session(stop_event)
                exhausted = False
                break
            except asyncio.CancelledError:
                raise
            except WebSocketDisconnected as exc:
                await self._on_event(
                    Disconnected(exc.code, exc.reason, self._now_ms())
                )
                await self._on_event(
                    TransportFailed(type(exc).__name__, str(exc), self._now_ms())
                )
            except Exception as exc:
                await self._on_event(
                    TransportFailed(type(exc).__name__, str(exc), self._now_ms())
                )
            if attempt < self._reconnect_policy.max_attempts:
                await self._wait_or_stop(stop_event)
                if stop_event.is_set():
                    exhausted = False
                    break
        if exhausted:
            await self._on_event(
                ReconnectExhausted(
                    attempts=self._reconnect_policy.max_attempts,
                    observed_at_ms=self._now_ms(),
                )
            )
        await self._on_event(Stopped(self._now_ms()))

    async def _run_session(self, stop_event: asyncio.Event) -> None:
        async with self._transport.connect(self._url) as session:
            await self._on_event(Connected(self._now_ms()))
            await session.send_text(self._adapter.subscription_payload("subscribe-1"))
            while not stop_event.is_set():
                receive = asyncio.create_task(session.receive_text())
                stop = asyncio.create_task(stop_event.wait())
                done, pending = await asyncio.wait(
                    {receive, stop}, return_when=asyncio.FIRST_COMPLETED
                )
                for task in pending:
                    task.cancel()
                if pending:
                    await asyncio.gather(*pending, return_exceptions=True)
                if stop in done and stop.result():
                    await session.send_text(self._adapter.unsubscription_payload("unsubscribe-1"))
                    await session.close()
                    return
                raw = receive.result()
                for event in self._adapter.parse(raw, observed_at_ms=self._now_ms()):
                    await self._on_event(event)
                    if isinstance(event, CandleObserved):
                        outcome = self._candle_handler.handle(event)
                        if outcome is not None:
                            await self._on_outcome(outcome)

    async def _wait_or_stop(self, stop_event: asyncio.Event) -> None:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=self._reconnect_policy.delay_seconds)
        except TimeoutError:
            return
