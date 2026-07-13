"""Run the long-lived market-data service runtime."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from contextlib import suppress
from dataclasses import replace
from pathlib import Path

from market_data_service.adapters.http import RuntimeHttpServer
from market_data_service.adapters.http.consumer_read import ConsumerReadHttpHandler
from market_data_service.adapters.sqlite import initialize_database, register_stream
from market_data_service.application.market_metadata import VerifyConfiguredInstrumentMetadata
from market_data_service.config import load_market_config
from market_data_service.runtime.service import RuntimeService
from market_data_service.runtime.settings import RuntimeSettings
from market_data_service.runtime.status import RuntimeStatusStore
from market_data_service.runtime.wiring import RuntimeWiring


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    settings = _apply_cli(RuntimeSettings.from_environment(), args)
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    return asyncio.run(_run(settings))


async def _run(settings: RuntimeSettings) -> int:
    config = load_market_config(settings.markets_config_path)
    settings.database_path.parent.mkdir(parents=True, exist_ok=True)
    initialize_database(settings.database_path)
    wiring = RuntimeWiring.build(
        settings.database_path,
        config,
        rest_base_url=settings.rest_base_url,
    )
    verifier = VerifyConfiguredInstrumentMetadata(
        wiring.rest_source,
        category=config.source.category,
    )
    coverage_by_ticker = {
        coverage.instrument.ticker: coverage
        for coverage in config.enabled_instruments
    }
    for coverage in config.enabled_instruments:
        verifier.execute(coverage)
    for stream in config.enabled_streams:
        coverage = coverage_by_ticker[stream.instrument.ticker]
        register_stream(
            settings.database_path,
            stream,
            exchange_symbol=coverage.exchange_symbol,
            now_ms=wiring.clock.now_ms(),
        )

    status = RuntimeStatusStore(config.enabled_streams)
    http_server = RuntimeHttpServer(
        settings.http_host,
        settings.http_port,
        status,
        ConsumerReadHttpHandler(wiring.consumer_read()),
    )
    service = RuntimeService(
        settings=settings,
        config=config,
        wiring=wiring,
        status=status,
        http_server=http_server,
    )
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for name in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(name, stop_event.set)
    await service.run(stop_event)
    return 0


def _apply_cli(settings: RuntimeSettings, args: argparse.Namespace) -> RuntimeSettings:
    values = {
        "database_path": args.database,
        "markets_config_path": args.config,
        "http_host": args.host,
        "http_port": args.port,
        "rest_base_url": args.rest_url,
        "websocket_url": args.websocket_url,
        "startup_backfill_windows_per_stream": args.startup_backfill_windows,
        "startup_repair_windows_per_stream": args.startup_repair_windows,
        "log_level": args.log_level,
    }
    return replace(
        settings,
        **{key: value for key, value in values.items() if value is not None},
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--rest-url")
    parser.add_argument("--websocket-url")
    parser.add_argument("--startup-backfill-windows", type=int)
    parser.add_argument("--startup-repair-windows", type=int)
    parser.add_argument("--log-level")
    return parser
