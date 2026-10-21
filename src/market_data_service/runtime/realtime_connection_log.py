"""Operator-facing log lines for WebSocket connection events."""

from __future__ import annotations

import logging

from market_data_service.application.realtime.events import (
    Connected,
    Disconnected,
    RealtimeEvent,
    ReconnectExhausted,
    TransportFailed,
)

_LOGGER = logging.getLogger("market_data_service.runtime.realtime")


def log_connection_event(event: RealtimeEvent) -> None:
    if isinstance(event, Connected):
        _LOGGER.info("websocket connected")
    elif isinstance(event, Disconnected):
        _LOGGER.warning("websocket disconnected code=%s reason=%s", event.code, event.reason)
    elif isinstance(event, TransportFailed):
        _LOGGER.warning("websocket transport failed code=%s detail=%s", event.code, event.detail)
    elif isinstance(event, ReconnectExhausted):
        _LOGGER.error("websocket reconnect exhausted attempts=%s", event.attempts)
