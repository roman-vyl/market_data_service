from __future__ import annotations

import json
from pathlib import Path

import pytest

from market_data_service.adapters.bybit.websocket.protocol import (
    BybitWebSocketPayloadError,
    parse_message,
    subscription_message,
)
from market_data_service.adapters.bybit.websocket.topics import BybitTopicMap
from market_data_service.application.realtime.events import CandleObserved
from market_data_service.config import load_market_config


def _config(tmp_path: Path):
    path = tmp_path / "markets.toml"
    path.write_text(
        '''
schema_version = 1
[source]
venue = "bybit"
category = "linear"
[[instruments]]
ticker = "BTCUSDT.P"
exchange_symbol = "BTCUSDT"
enabled = true
canonical_timeframes = ["1m", "5m", "1h"]
history_policy = "full_available"
[[instruments]]
ticker = "ETHUSDT.P"
exchange_symbol = "ETHUSDT"
enabled = true
canonical_timeframes = ["1m", "5m", "1h"]
history_policy = "full_available"
''',
        encoding="utf-8",
    )
    return load_market_config(path)


def test_topic_map_expands_every_symbol_and_timeframe(tmp_path: Path) -> None:
    topic_map = BybitTopicMap.from_config(_config(tmp_path))

    assert topic_map.topics == (
        "kline.1.BTCUSDT",
        "kline.5.BTCUSDT",
        "kline.60.BTCUSDT",
        "kline.1.ETHUSDT",
        "kline.5.ETHUSDT",
        "kline.60.ETHUSDT",
    )
    assert topic_map.resolve("kline.60.ETHUSDT").canonical_id == "ETHUSDT.P:1h"


def test_subscription_message_is_deterministic(tmp_path: Path) -> None:
    topic_map = BybitTopicMap.from_config(_config(tmp_path))
    payload = json.loads(subscription_message(topic_map.topics, request_id="req-1"))

    assert payload == {"req_id": "req-1", "op": "subscribe", "args": list(topic_map.topics)}


def test_protocol_parses_confirmed_and_unconfirmed_candles(tmp_path: Path) -> None:
    topic_map = BybitTopicMap.from_config(_config(tmp_path))
    raw = json.dumps(
        {
            "topic": "kline.5.BTCUSDT",
            "data": [
                {
                    "start": 300_000,
                    "end": 599_999,
                    "interval": "5",
                    "open": "100",
                    "high": "102",
                    "low": "99",
                    "close": "101",
                    "volume": "12",
                    "confirm": True,
                }
            ],
        }
    )

    events = parse_message(raw, topic_map=topic_map, observed_at_ms=700_000)

    assert len(events) == 1
    event = events[0]
    assert isinstance(event, CandleObserved)
    assert event.stream.canonical_id == "BTCUSDT.P:5m"
    assert event.candle.confirmed is True
    assert event.candle.ohlcv_text == ("100", "102", "99", "101", "12")


def test_protocol_rejects_unknown_topic(tmp_path: Path) -> None:
    topic_map = BybitTopicMap.from_config(_config(tmp_path))
    with pytest.raises(ValueError, match="unknown realtime topic"):
        parse_message(
            json.dumps({"topic": "kline.1.XRPUSDT", "data": [{}]}),
            topic_map=topic_map,
            observed_at_ms=1,
        )


def test_protocol_rejects_malformed_json(tmp_path: Path) -> None:
    with pytest.raises(BybitWebSocketPayloadError, match="valid JSON"):
        parse_message("{", topic_map=BybitTopicMap.from_config(_config(tmp_path)), observed_at_ms=1)


def test_subscription_ack_without_topic_list_confirms_configured_topics(tmp_path: Path) -> None:
    from market_data_service.application.realtime.events import SubscriptionConfirmed

    topic_map = BybitTopicMap.from_config(_config(tmp_path))
    events = parse_message(
        json.dumps({"success": True, "ret_msg": "", "op": "subscribe", "req_id": "req-1"}),
        topic_map=topic_map,
        observed_at_ms=10,
    )

    assert events == (SubscriptionConfirmed(topic_map.topics, 10),)
