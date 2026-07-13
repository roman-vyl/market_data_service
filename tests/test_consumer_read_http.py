from __future__ import annotations

import json
from decimal import Decimal
from urllib.error import HTTPError
from urllib.request import urlopen

from market_data_service.adapters.http import RuntimeHttpServer
from market_data_service.adapters.http.consumer_read import ConsumerReadHttpHandler
from market_data_service.application.consumer_read import GetCandleRange
from market_data_service.config.markets import MarketSourceConfig, ValidatedMarketConfig
from market_data_service.domain.candles import CanonicalCandle, ObservationSource
from market_data_service.domain.identity import InstrumentKey, StreamKey
from market_data_service.domain.instruments import HistoryPolicy, InstrumentCoverage
from market_data_service.domain.stream_state import (
    StreamLifecycleState,
    StreamStateSnapshot,
)
from market_data_service.ports.consumer_read import ConsumerReadSnapshot
from market_data_service.runtime.status import RuntimeStatusStore

STREAM = StreamKey(InstrumentKey("BTCUSDT.P"), "5m")
CONFIG = ValidatedMarketConfig(
    1,
    MarketSourceConfig("bybit", "linear"),
    (
        InstrumentCoverage(
            STREAM.instrument,
            "BTCUSDT",
            True,
            ("5m",),
            HistoryPolicy.FULL_AVAILABLE,
        ),
    ),
)


class Reader:
    def read_snapshot(self, stream: StreamKey, *, start_time_ms: int, end_time_ms: int):
        state = StreamStateSnapshot(
            stream,
            StreamLifecycleState.READY,
            earliest_available_open_time_ms=0,
            latest_committed_open_time_ms=0,
        )
        candles = (
            CanonicalCandle(
                stream,
                0,
                299_999,
                Decimal("1.2300"),
                Decimal("2.0"),
                Decimal("1.0"),
                Decimal("1.500"),
                Decimal("10.500"),
                ObservationSource.BYBIT_REST,
                1,
            ),
        )
        return ConsumerReadSnapshot(state, candles)


def get(url: str):
    try:
        response = urlopen(url, timeout=2)
    except HTTPError as exc:
        return exc.code, json.loads(exc.read())
    return response.status, json.loads(response.read())


def test_http_contract_decimal_text_and_errors() -> None:
    status = RuntimeStatusStore((STREAM,))
    server = RuntimeHttpServer(
        "127.0.0.1",
        0,
        status,
        ConsumerReadHttpHandler(GetCandleRange(CONFIG, Reader())),
    )
    server.start()
    host, port = server.address
    try:
        code, payload = get(
            f"http://{host}:{port}/v1/candles?"
            "ticker=BTCUSDT.P&timeframe=5m&from_ms=0&to_ms=300000"
        )
        assert code == 200
        assert payload["candles"][0]["open"] == "1.23"
        assert payload["candles"][0]["volume"] == "10.5"
        assert isinstance(payload["candles"][0]["open"], str)
        code, payload = get(f"http://{host}:{port}/v1/candles?ticker=BTCUSDT.P")
        assert code == 422
        assert payload["error"] == "invalid_range"
    finally:
        server.close()


def test_openapi_document_is_served() -> None:
    status = RuntimeStatusStore((STREAM,))
    server = RuntimeHttpServer("127.0.0.1", 0, status)
    server.start()
    host, port = server.address
    try:
        code, payload = get(f"http://{host}:{port}/openapi.json")
        assert code == 200
        assert "/v1/candles" in payload["paths"]
    finally:
        server.close()
