"""Small Bybit-specific realtime adapter facade."""

from __future__ import annotations

from dataclasses import dataclass

from market_data_service.adapters.bybit.websocket.protocol import (
    parse_message,
    subscription_message,
    unsubscription_message,
)
from market_data_service.adapters.bybit.websocket.topics import BybitTopicMap
from market_data_service.application.realtime.events import RealtimeEvent


@dataclass(frozen=True, slots=True)
class BybitWebSocketAdapter:
    topic_map: BybitTopicMap

    def subscription_payload(self, request_id: str) -> str:
        return subscription_message(self.topic_map.topics, request_id=request_id)

    def unsubscription_payload(self, request_id: str) -> str:
        return unsubscription_message(self.topic_map.topics, request_id=request_id)

    def parse(self, raw: str, *, observed_at_ms: int) -> tuple[RealtimeEvent, ...]:
        return parse_message(raw, topic_map=self.topic_map, observed_at_ms=observed_at_ms)
