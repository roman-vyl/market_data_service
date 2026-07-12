from __future__ import annotations

import asyncio
import json
from pathlib import Path

from websockets.asyncio.server import ServerConnection, serve

from market_data_service.adapters.bybit.websocket import (
    BybitTopicMap,
    BybitWebSocketAdapter,
    WebsocketsTransport,
)
from market_data_service.adapters.sqlite import (
    SqliteUnitOfWork,
    initialize_database,
    register_stream,
)
from market_data_service.application.ingest import IngestObservedCandle
from market_data_service.application.realtime.connector import RealtimeConnector, ReconnectPolicy
from market_data_service.application.realtime.events import RealtimeEvent
from market_data_service.application.realtime.handler import RealtimeCandleHandler
from market_data_service.application.realtime.outcomes import RealtimeIngestionOutcome
from market_data_service.config import load_market_config
from market_data_service.domain.timeframes import get_timeframe


def _write_config(tmp_path: Path) -> Path:
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
    return path


def test_fake_websocket_ingests_all_confirmed_streams(tmp_path: Path) -> None:
    asyncio.run(_scenario(tmp_path))


async def _scenario(tmp_path: Path) -> None:
    config = load_market_config(_write_config(tmp_path))
    topic_map = BybitTopicMap.from_config(config)
    database = tmp_path / "market.sqlite3"
    initialize_database(database)
    coverage_by_ticker = {item.instrument.ticker: item for item in config.enabled_instruments}
    for stream in config.enabled_streams:
        register_stream(
            database,
            stream,
            exchange_symbol=coverage_by_ticker[stream.instrument.ticker].exchange_symbol,
            now_ms=1,
        )

    server_topics: list[str] = []

    async def server_handler(connection: ServerConnection) -> None:
        subscribe = json.loads(await connection.recv())
        server_topics.extend(subscribe["args"])
        await connection.send(
            json.dumps(
                {
                    "op": "subscribe",
                    "success": True,
                    "data": {"successTopics": subscribe["args"]},
                }
            )
        )
        for topic in subscribe["args"]:
            interval = topic.split(".")[1]
            stream = topic_map.resolve(topic)
            step = get_timeframe(stream.timeframe).duration_ms
            row = {
                "start": 0,
                "end": step - 1,
                "interval": interval,
                "open": "100",
                "high": "102",
                "low": "99",
                "close": "101",
                "volume": "10",
            }
            await connection.send(json.dumps({"topic": topic, "data": [{**row, "confirm": False}]}))
            await connection.send(json.dumps({"topic": topic, "data": [{**row, "confirm": True}]}))
        await connection.recv()  # unsubscribe

    events: list[RealtimeEvent] = []
    outcomes: list[RealtimeIngestionOutcome] = []
    stop_event = asyncio.Event()

    async def on_event(event: RealtimeEvent) -> None:
        events.append(event)

    async def on_outcome(outcome: RealtimeIngestionOutcome) -> None:
        outcomes.append(outcome)
        if len(outcomes) == len(config.enabled_streams):
            stop_event.set()

    ingestion = IngestObservedCandle(lambda: SqliteUnitOfWork(database))
    handler = RealtimeCandleHandler(ingestion, lambda: 10_000)

    async with serve(server_handler, "127.0.0.1", 0) as websocket_server:
        port = websocket_server.sockets[0].getsockname()[1]
        connector = RealtimeConnector(
            url=f"ws://127.0.0.1:{port}",
            transport=WebsocketsTransport(),
            adapter=BybitWebSocketAdapter(topic_map),
            candle_handler=handler,
            now_ms=lambda: 9_000,
            on_event=on_event,
            on_outcome=on_outcome,
            reconnect_policy=ReconnectPolicy(max_attempts=1, delay_seconds=0),
        )
        await asyncio.wait_for(connector.run(stop_event), timeout=5)

    assert tuple(server_topics) == topic_map.topics
    assert len(outcomes) == 6
    assert {outcome.stream.canonical_id for outcome in outcomes} == {
        "BTCUSDT.P:1m",
        "BTCUSDT.P:5m",
        "BTCUSDT.P:1h",
        "ETHUSDT.P:1m",
        "ETHUSDT.P:5m",
        "ETHUSDT.P:1h",
    }
    with SqliteUnitOfWork(database) as unit_of_work:
        stored = [unit_of_work.get_candle(stream, 0) for stream in config.enabled_streams]
    assert all(candle is not None for candle in stored)
