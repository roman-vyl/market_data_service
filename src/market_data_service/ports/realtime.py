"""Ports for realtime WebSocket transport."""

from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import Protocol


class WebSocketDisconnected(ConnectionError):
    """Transport connection closed before the caller requested stop."""

    def __init__(self, detail: str, *, code: int | None = None, reason: str | None = None) -> None:
        super().__init__(detail)
        self.code = code
        self.reason = reason


class WebSocketSession(Protocol):
    async def send_text(self, message: str) -> None: ...
    async def receive_text(self) -> str: ...
    async def close(self) -> None: ...


class WebSocketTransport(Protocol):
    def connect(self, url: str) -> AbstractAsyncContextManager[WebSocketSession]: ...
