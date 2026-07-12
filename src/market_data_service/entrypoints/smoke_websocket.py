"""Bounded real Bybit public WebSocket ingestion smoke."""

from __future__ import annotations

import argparse
import asyncio
import tempfile
import time
from pathlib import Path

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
from market_data_service.application.realtime.connector import (
    RealtimeConnector,
    ReconnectPolicy,
)
from market_data_service.application.realtime.events import (
    RealtimeEvent,
    SubscriptionConfirmed,
)
from market_data_service.application.realtime.handler import RealtimeCandleHandler
from market_data_service.application.realtime.outcomes import (
    RealtimeIngestionClassification,
    RealtimeIngestionOutcome,
)
from market_data_service.config import load_market_config

_DEFAULT_URL = "wss://stream.bybit.com/v5/public/linear"


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.database is not None:
        args.database.parent.mkdir(parents=True, exist_ok=True)
        return asyncio.run(_run(args.database, args))
    with tempfile.TemporaryDirectory(prefix="market-data-smoke-websocket-") as directory:
        return asyncio.run(_run(Path(directory) / "smoke.sqlite3", args))


async def _run(database: Path, args: argparse.Namespace) -> int:
    config = load_market_config(args.config)
    topic_map = BybitTopicMap.from_config(config)
    initialize_database(database)
    coverage_by_ticker = {
        item.instrument.ticker: item for item in config.enabled_instruments
    }
    for stream in config.enabled_streams:
        register_stream(
            database,
            stream,
            exchange_symbol=coverage_by_ticker[stream.instrument.ticker].exchange_symbol,
            now_ms=_now_ms(),
        )

    events: list[RealtimeEvent] = []
    outcomes: list[RealtimeIngestionOutcome] = []
    stop_event = asyncio.Event()
    outcome_ready = asyncio.Event()

    async def on_event(event: RealtimeEvent) -> None:
        events.append(event)

    async def on_outcome(outcome: RealtimeIngestionOutcome) -> None:
        outcomes.append(outcome)
        outcome_ready.set()
        stop_event.set()

    ingestion = IngestObservedCandle(lambda: SqliteUnitOfWork(database))
    connector = RealtimeConnector(
        url=args.url,
        transport=WebsocketsTransport(),
        adapter=BybitWebSocketAdapter(topic_map),
        candle_handler=RealtimeCandleHandler(ingestion, _now_ms),
        now_ms=_now_ms,
        on_event=on_event,
        on_outcome=on_outcome,
        reconnect_policy=ReconnectPolicy(
            max_attempts=args.max_attempts,
            delay_seconds=args.reconnect_delay_seconds,
        ),
    )
    connector_task = asyncio.create_task(connector.run(stop_event))
    timed_out = False
    try:
        await asyncio.wait_for(outcome_ready.wait(), timeout=args.timeout_seconds)
    except TimeoutError:
        timed_out = True
        stop_event.set()
    await connector_task

    confirmed_topics = {
        topic
        for event in events
        if isinstance(event, SubscriptionConfirmed)
        for topic in event.topics
    }
    acceptable = {
        RealtimeIngestionClassification.COMMITTED,
        RealtimeIngestionClassification.DUPLICATE,
        RealtimeIngestionClassification.CORRECTED,
    }
    passed = bool(outcomes) and outcomes[0].classification in acceptable
    print(f"database={database}")
    print(f"configured_topics={len(topic_map.topics)}")
    print(f"confirmed_topics={len(confirmed_topics)}")
    print(f"timed_out={str(timed_out).lower()}")
    if outcomes:
        outcome = outcomes[0]
        print(
            f"outcome stream={outcome.stream.canonical_id} "
            f"open_time_ms={outcome.open_time_ms} "
            f"classification={outcome.classification.value}"
        )
    print(f"smoke_result={'PASS' if passed else 'FAIL'}")
    return 0 if passed else 1


def _now_ms() -> int:
    return int(time.time() * 1000)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path)
    parser.add_argument("--config", type=Path, default=Path("config/markets.toml"))
    parser.add_argument("--url", default=_DEFAULT_URL)
    parser.add_argument("--timeout-seconds", type=float, default=90.0)
    parser.add_argument("--max-attempts", type=int, default=2)
    parser.add_argument("--reconnect-delay-seconds", type=float, default=1.0)
    return parser
