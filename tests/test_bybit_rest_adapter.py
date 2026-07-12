from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from market_data_service.adapters.bybit import (
    BybitApiError,
    BybitHttpError,
    BybitRestCandleSource,
)
from market_data_service.adapters.sqlite import (
    SqliteUnitOfWork,
    initialize_database,
    register_stream,
)
from market_data_service.application.import_window import ImportHistoricalWindow
from market_data_service.domain import InstrumentKey, StreamKey, TimeWindow


class FakeTransport:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload
        self.calls: list[tuple[str, dict[str, str | int], float]] = []

    def get_json(
        self,
        url: str,
        params: dict[str, str | int],
        timeout_seconds: float,
    ) -> dict[str, Any]:
        self.calls.append((url, params, timeout_seconds))
        return self.payload


class SequencedTransport:
    def __init__(self, outcomes: list[dict[str, Any] | Exception]) -> None:
        self._outcomes = list(outcomes)
        self.calls: list[tuple[str, dict[str, str | int], float]] = []

    def get_json(
        self,
        url: str,
        params: dict[str, str | int],
        timeout_seconds: float,
    ) -> dict[str, Any]:
        self.calls.append((url, params, timeout_seconds))
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


@dataclass
class FakeClock:
    value: int

    def now_ms(self) -> int:
        current = self.value
        self.value += 1
        return current


def _stream() -> StreamKey:
    return StreamKey(InstrumentKey("BTCUSDT.P"), "1m")


def _payload() -> dict[str, Any]:
    return {
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "category": "linear",
            "symbol": "BTCUSDT",
            "list": [
                ["60000", "101.0", "103", "100", "102", "2.500", "0"],
                ["0", "100", "102", "99", "101.000", "1.50", "0"],
                ["120000", "102", "104", "101", "103", "3", "0"],
            ],
        },
    }


def _instrument_payload() -> dict[str, Any]:
    return {
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "category": "linear",
            "list": [
                {"symbol": "ETHUSDT", "launchTime": "1600000000000"},
                {"symbol": "BTCUSDT", "launchTime": "1585526400000"},
            ],
        },
    }


def test_adapter_uses_half_open_window_and_returns_ascending_closed_candles() -> None:
    transport = FakeTransport(_payload())
    source = BybitRestCandleSource(
        exchange_symbols={"BTCUSDT.P": "BTCUSDT"},
        transport=transport,
    )

    candles = source.fetch_closed_candles(
        _stream(),
        TimeWindow(0, 120000),
        observed_at_ms=180000,
    )

    assert [candle.open_time_ms for candle in candles] == [0, 60000]
    assert candles[0].ohlcv_text == ("100", "102", "99", "101", "1.5")
    _, params, _ = transport.calls[0]
    assert params == {
        "category": "linear",
        "symbol": "BTCUSDT",
        "interval": "1",
        "start": 0,
        "end": 119999,
        "limit": 2,
    }


def test_adapter_fetches_exact_instrument_launch_time() -> None:
    transport = FakeTransport(_instrument_payload())
    source = BybitRestCandleSource(
        exchange_symbols={"BTCUSDT.P": "BTCUSDT"},
        transport=transport,
    )

    launch_time_ms = source.get_launch_time_ms(InstrumentKey("BTCUSDT.P"))

    assert launch_time_ms == 1585526400000
    _, params, _ = transport.calls[0]
    assert params == {
        "category": "linear",
        "symbol": "BTCUSDT",
    }


def test_adapter_rejects_more_than_1000_candles() -> None:
    source = BybitRestCandleSource(
        exchange_symbols={"BTCUSDT.P": "BTCUSDT"},
        transport=FakeTransport(_payload()),
    )
    with pytest.raises(ValueError, match="1000"):
        source.fetch_closed_candles(
            _stream(),
            TimeWindow(0, 1001 * 60000),
            observed_at_ms=1002 * 60000,
        )


def test_adapter_raises_typed_api_error() -> None:
    source = BybitRestCandleSource(
        exchange_symbols={"BTCUSDT.P": "BTCUSDT"},
        transport=FakeTransport({"retCode": 10001, "retMsg": "bad request"}),
    )
    with pytest.raises(BybitApiError, match="10001"):
        source.fetch_closed_candles(_stream(), TimeWindow(0, 60000), observed_at_ms=120000)


def test_window_import_into_sqlite_is_idempotent(tmp_path: Path) -> None:
    path = tmp_path / "market.sqlite"
    stream = _stream()
    initialize_database(path)
    register_stream(path, stream, exchange_symbol="BTCUSDT", now_ms=1)
    source = BybitRestCandleSource(
        exchange_symbols={"BTCUSDT.P": "BTCUSDT"},
        transport=FakeTransport(_payload()),
    )
    importer = ImportHistoricalWindow(
        source,
        lambda: SqliteUnitOfWork(path),
        FakeClock(180000),
    )

    first = importer.execute(stream, TimeWindow(0, 120000))
    second = importer.execute(stream, TimeWindow(0, 120000))

    assert (first.observed, first.committed, first.duplicates) == (2, 2, 0)
    assert (second.observed, second.committed, second.duplicates) == (2, 0, 2)


def test_adapter_parses_linear_perpetual_instrument_specification() -> None:
    transport = FakeTransport(
        {
            "retCode": 0,
            "result": {
                "list": [
                    {
                        "symbol": "BTCUSDT",
                        "contractType": "LinearPerpetual",
                        "status": "Trading",
                        "settleCoin": "USDT",
                        "launchTime": "1585526400000",
                    }
                ]
            },
        }
    )
    source = BybitRestCandleSource(
        exchange_symbols={"BTCUSDT.P": "BTCUSDT"},
        transport=transport,
    )

    specification = source.get_instrument_specification(InstrumentKey("BTCUSDT.P"))

    assert specification.exchange_symbol == "BTCUSDT"
    assert specification.category == "linear"
    assert specification.contract_type == "LinearPerpetual"
    assert specification.status == "Trading"
    assert specification.settle_coin == "USDT"
    assert specification.launch_time_ms == 1585526400000


def test_adapter_classifies_approved_transient_ret_code() -> None:
    from market_data_service.adapters.bybit.errors import BybitTransientApiError

    source = BybitRestCandleSource(
        exchange_symbols={"BTCUSDT.P": "BTCUSDT"},
        transport=FakeTransport({"retCode": 10006, "retMsg": "Too many visits"}),
    )

    try:
        source.get_launch_time_ms(InstrumentKey("BTCUSDT.P"))
    except BybitTransientApiError:
        pass
    else:
        raise AssertionError("expected transient Bybit API error")


def test_adapter_retries_recoverable_http_failure_then_succeeds() -> None:
    transport = SequencedTransport([BybitHttpError("timeout"), _payload()])
    delays: list[float] = []
    source = BybitRestCandleSource(
        exchange_symbols={"BTCUSDT.P": "BTCUSDT"},
        transport=transport,
        sleeper=delays.append,
        randomizer=lambda: 0.0,
    )

    candles = source.fetch_closed_candles(
        _stream(),
        TimeWindow(0, 120000),
        observed_at_ms=180000,
    )

    assert [candle.open_time_ms for candle in candles] == [0, 60000]
    assert len(transport.calls) == 2
    assert delays == [1.0]


def test_adapter_retries_rate_limit_with_longer_backoff() -> None:
    from market_data_service.adapters.bybit.errors import BybitTransientApiError

    transport = SequencedTransport(
        [BybitTransientApiError("Bybit retCode=10006: Too many visits", ret_code=10006), _payload()]
    )
    delays: list[float] = []
    source = BybitRestCandleSource(
        exchange_symbols={"BTCUSDT.P": "BTCUSDT"},
        transport=transport,
        sleeper=delays.append,
        randomizer=lambda: 0.0,
    )

    source.fetch_closed_candles(_stream(), TimeWindow(0, 120000), observed_at_ms=180000)

    assert len(transport.calls) == 2
    assert delays == [5.0]


def test_adapter_honors_retry_after_when_present() -> None:
    transport = SequencedTransport(
        [BybitHttpError("HTTP 429", status_code=429, retry_after_seconds=3.0), _payload()]
    )
    delays: list[float] = []
    source = BybitRestCandleSource(
        exchange_symbols={"BTCUSDT.P": "BTCUSDT"},
        transport=transport,
        sleeper=delays.append,
        randomizer=lambda: 0.0,
    )

    source.fetch_closed_candles(_stream(), TimeWindow(0, 120000), observed_at_ms=180000)

    assert delays == [3.0]


def test_adapter_does_not_retry_fatal_api_error() -> None:
    transport = SequencedTransport(
        [{"retCode": 10001, "retMsg": "bad request"}, _payload()]
    )
    delays: list[float] = []
    source = BybitRestCandleSource(
        exchange_symbols={"BTCUSDT.P": "BTCUSDT"},
        transport=transport,
        sleeper=delays.append,
    )

    with pytest.raises(BybitApiError, match="10001"):
        source.fetch_closed_candles(_stream(), TimeWindow(0, 60000), observed_at_ms=120000)

    assert len(transport.calls) == 1
    assert delays == []
