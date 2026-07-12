"""Bybit WebSocket protocol encoding and parsing."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

from market_data_service.adapters.bybit.websocket.topics import BybitTopicMap
from market_data_service.application.realtime.events import (
    CandleObserved,
    HeartbeatObserved,
    RealtimeEvent,
    SubscriptionConfirmed,
    TransportFailed,
)
from market_data_service.domain.candles import ObservationSource, ObservedCandle
from market_data_service.domain.identity import StreamKey


class BybitWebSocketPayloadError(ValueError):
    """Bybit sent an invalid or unsupported realtime payload."""


def subscription_message(topics: tuple[str, ...], *, request_id: str) -> str:
    return json.dumps({"req_id": request_id, "op": "subscribe", "args": list(topics)})


def unsubscription_message(topics: tuple[str, ...], *, request_id: str) -> str:
    return json.dumps({"req_id": request_id, "op": "unsubscribe", "args": list(topics)})


def ping_message() -> str:
    return json.dumps({"op": "ping"})


def parse_message(
    raw: str,
    *,
    topic_map: BybitTopicMap,
    observed_at_ms: int,
) -> tuple[RealtimeEvent, ...]:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise BybitWebSocketPayloadError("websocket message is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise BybitWebSocketPayloadError("websocket payload must be an object")

    op = payload.get("op")
    if op == "pong" or payload.get("ret_msg") == "pong":
        return (HeartbeatObserved(observed_at_ms),)
    if op in {"subscribe", "unsubscribe"}:
        if payload.get("success") is not True:
            return (
                TransportFailed(
                    code="subscription_failed",
                    detail=str(payload.get("ret_msg", "unknown subscription failure")),
                    observed_at_ms=observed_at_ms,
                ),
            )
        data = payload.get("data")
        args = data.get("successTopics") if isinstance(data, dict) else None
        topics = (
            tuple(args)
            if isinstance(args, list) and all(isinstance(value, str) for value in args)
            else topic_map.topics
        )
        return (SubscriptionConfirmed(topics=topics, observed_at_ms=observed_at_ms),)

    topic = payload.get("topic")
    if not isinstance(topic, str):
        raise BybitWebSocketPayloadError("websocket payload has no topic")
    stream = topic_map.resolve(topic)
    rows = payload.get("data")
    if not isinstance(rows, list) or not rows:
        raise BybitWebSocketPayloadError("kline payload data must be a non-empty array")
    events: list[RealtimeEvent] = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise BybitWebSocketPayloadError("kline row must be an object")
        events.append(
            CandleObserved(
                stream=stream,
                candle=_parse_candle(row, stream, topic, observed_at_ms),
            )
        )
    return tuple(events)


def _parse_candle(
    row: Mapping[str, Any], stream: StreamKey, topic: str, observed_at_ms: int
) -> ObservedCandle:
    try:
        expected_interval = topic.split(".")[1]
        if str(row["interval"]) != expected_interval:
            raise BybitWebSocketPayloadError(
                f"kline interval mismatch: topic={expected_interval} row={row['interval']}"
            )
        confirmed = row["confirm"]
        if not isinstance(confirmed, bool):
            raise BybitWebSocketPayloadError("kline confirm must be a boolean")
        return ObservedCandle(
            stream=stream,
            open_time_ms=int(row["start"]),
            close_time_ms=int(row["end"]),
            open=str(row["open"]),
            high=str(row["high"]),
            low=str(row["low"]),
            close=str(row["close"]),
            volume=str(row["volume"]),
            confirmed=confirmed,
            observed_at_ms=observed_at_ms,
            source=ObservationSource.BYBIT_WEBSOCKET,
        )
    except BybitWebSocketPayloadError:
        raise
    except (KeyError, TypeError, ValueError) as exc:
        raise BybitWebSocketPayloadError(f"invalid kline row: {exc}") from exc
