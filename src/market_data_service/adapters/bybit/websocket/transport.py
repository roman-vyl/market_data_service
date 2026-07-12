"""WebSocket transport backed by the websockets asyncio client."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed

from market_data_service.ports.realtime import WebSocketDisconnected


@dataclass(slots=True)
class WebsocketsSession:
    connection: ClientConnection

    async def send_text(self, message: str) -> None:
        await self.connection.send(message)

    async def receive_text(self) -> str:
        try:
            message = await self.connection.recv()
        except ConnectionClosed as exc:
            code = exc.rcvd.code if exc.rcvd is not None else None
            reason = exc.rcvd.reason if exc.rcvd is not None else None
            raise WebSocketDisconnected(str(exc), code=code, reason=reason) from exc
        if not isinstance(message, str):
            raise TypeError("binary WebSocket messages are unsupported")
        return message

    async def close(self) -> None:
        await self.connection.close()


@dataclass(frozen=True, slots=True)
class WebsocketsTransport:
    ping_interval_seconds: float = 20.0
    ping_timeout_seconds: float = 20.0
    close_timeout_seconds: float = 10.0

    @asynccontextmanager
    async def connect(self, url: str) -> AsyncIterator[WebsocketsSession]:
        async with connect(
            url,
            ping_interval=self.ping_interval_seconds,
            ping_timeout=self.ping_timeout_seconds,
            close_timeout=self.close_timeout_seconds,
        ) as connection:
            yield WebsocketsSession(connection)
