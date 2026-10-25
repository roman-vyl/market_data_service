from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from market_data_service.adapters.bybit import BybitOpenInterestSource, BybitRestCandleSource
from market_data_service.config import MarketConfigError, load_market_config
from market_data_service.domain import InstrumentKey, StreamKey, TimeWindow

STEP = 300_000
T0 = 1_704_067_200_000  # 2024-01-01T00:00Z
STREAM = StreamKey(InstrumentKey("BTCUSDT.P"), "5m")


class FakeTransport:
    def __init__(self, rows: list[dict[str, str]]) -> None:
        self.rows = rows
        self.calls: list[tuple[str, dict[str, str | int]]] = []

    def get_json(
        self, url: str, params: dict[str, str | int], timeout_seconds: float
    ) -> dict[str, Any]:
        self.calls.append((url, params))
        return {
            "retCode": 0,
            "retMsg": "OK",
            "result": {"list": self.rows, "nextPageCursor": "lastid=1&lasttime=1"},
        }


def _source(rows: list[dict[str, str]]) -> tuple[BybitOpenInterestSource, FakeTransport]:
    transport = FakeTransport(rows)
    rest = BybitRestCandleSource(exchange_symbols={"BTCUSDT.P": "BTCUSDT"}, transport=transport)
    return BybitOpenInterestSource(rest), transport


def _row(index: int, value: str = "100.5") -> dict[str, str]:
    return {"openInterest": value, "timestamp": str(T0 + index * STEP)}


def test_request_is_inclusive_200_point_window_without_cursor() -> None:
    source, transport = _source([_row(i) for i in reversed(range(200))])

    points = source.fetch_open_interest(
        STREAM, TimeWindow(T0, T0 + 200 * STEP), observed_at_ms=T0 + 10**9
    )

    url, params = transport.calls[0]
    assert url.endswith("/v5/market/open-interest")
    assert params == {
        "category": "linear",
        "symbol": "BTCUSDT",
        "intervalTime": "5min",
        "startTime": T0,
        "endTime": T0 + 199 * STEP,
        "limit": 200,
    }
    assert len(transport.calls) == 1
    assert [p.open_time_ms for p in points] == [T0 + i * STEP for i in range(200)]


def test_window_over_200_points_is_rejected() -> None:
    source, _ = _source([])
    with pytest.raises(ValueError, match="200"):
        source.fetch_open_interest(STREAM, TimeWindow(T0, T0 + 201 * STEP), observed_at_ms=T0)


def test_running_interval_and_out_of_window_rows_are_dropped_and_sorted() -> None:
    rows = [
        _row(3),
        _row(2),
        _row(2, "9"),
        _row(1),
        _row(0),
        {"openInterest": "1", "timestamp": "5"},
    ]
    source, _ = _source(rows)

    # At T0 + 3 steps + 10 s bar 3 is still running: its point must not be taken.
    points = source.fetch_open_interest(
        STREAM, TimeWindow(T0, T0 + 4 * STEP), observed_at_ms=T0 + 3 * STEP + 10_000
    )

    assert [(p.open_time_ms, p.open_interest) for p in points] == [
        (T0, "100.5"),
        (T0 + STEP, "100.5"),
        (T0 + 2 * STEP, "9"),  # duplicate timestamp: the later row wins, as for klines
    ]


def test_non_5m_stream_is_rejected() -> None:
    source, _ = _source([])
    with pytest.raises(ValueError, match="5m"):
        source.fetch_open_interest(
            StreamKey(InstrumentKey("BTCUSDT.P"), "1h"),
            TimeWindow(T0, T0 + 3_600_000),
            observed_at_ms=T0,
        )


CONFIG = """
schema_version = 1
[source]
venue = "bybit"
category = "linear"
[[instruments]]
ticker = "BTCUSDT.P"
exchange_symbol = "BTCUSDT"
enabled = true
canonical_timeframes = {btc_timeframes}
history_policy = "full_available"
{btc_flag}
[[instruments]]
ticker = "ETHUSDT.P"
exchange_symbol = "ETHUSDT"
enabled = true
canonical_timeframes = ["5m", "1h"]
history_policy = "full_available"
"""


def _config(tmp_path: Path, *, timeframes: str, flag: str):  # type: ignore[no-untyped-def]
    path = tmp_path / "markets.toml"
    path.write_text(CONFIG.format(btc_timeframes=timeframes, btc_flag=flag), encoding="utf-8")
    return load_market_config(path)


def test_flag_enables_only_the_5m_stream_of_that_instrument(tmp_path: Path) -> None:
    config = _config(tmp_path, timeframes='["5m", "1h", "1d"]', flag="open_interest = true")

    assert config.open_interest_streams == (STREAM,)
    assert len(config.enabled_streams) == 5


def test_absent_flag_means_no_open_interest_work(tmp_path: Path) -> None:
    config = _config(tmp_path, timeframes='["5m"]', flag="")

    assert config.open_interest_streams == ()


def test_flag_without_5m_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(MarketConfigError, match="5m"):
        _config(tmp_path, timeframes='["1h"]', flag="open_interest = true")


def test_non_boolean_flag_is_rejected(tmp_path: Path) -> None:
    with pytest.raises(MarketConfigError, match="boolean"):
        _config(tmp_path, timeframes='["5m"]', flag='open_interest = "yes"')
