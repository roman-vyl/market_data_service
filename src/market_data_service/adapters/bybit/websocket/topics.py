"""Deterministic Bybit topic routing."""

from __future__ import annotations

from dataclasses import dataclass

from market_data_service.config import ValidatedMarketConfig
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.timeframes import get_timeframe


class RealtimeTopicMapError(ValueError):
    """Configured topics are unknown, duplicate, or ambiguous."""


@dataclass(frozen=True, slots=True)
class BybitTopicMap:
    topic_to_stream: dict[str, StreamKey]

    @classmethod
    def from_config(cls, config: ValidatedMarketConfig) -> BybitTopicMap:
        mappings: dict[str, StreamKey] = {}
        symbol_by_ticker = config.exchange_symbols
        for stream in config.enabled_streams:
            symbol = symbol_by_ticker[stream.instrument.ticker]
            interval = get_timeframe(stream.timeframe).bybit_interval
            topic = f"kline.{interval}.{symbol}"
            if topic in mappings:
                raise RealtimeTopicMapError(f"duplicate or ambiguous topic mapping: {topic}")
            mappings[topic] = stream
        if not mappings:
            raise RealtimeTopicMapError("no enabled realtime topics configured")
        return cls(topic_to_stream=mappings)

    @property
    def topics(self) -> tuple[str, ...]:
        return tuple(self.topic_to_stream)

    def resolve(self, topic: str) -> StreamKey:
        try:
            return self.topic_to_stream[topic]
        except KeyError as exc:
            raise RealtimeTopicMapError(f"unknown realtime topic: {topic}") from exc
