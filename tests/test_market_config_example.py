from __future__ import annotations

import tomllib
from pathlib import Path


def test_example_market_config_declares_unique_canonical_perpetuals() -> None:
    config_path = Path(__file__).parents[1] / "config" / "markets.toml"
    payload = tomllib.loads(config_path.read_text(encoding="utf-8"))

    assert payload["schema_version"] == 1
    assert payload["source"] == {"venue": "bybit", "category": "linear"}

    instruments = payload["instruments"]
    assert [item["ticker"] for item in instruments] == [
        "BTCUSDT.P",
        "ETHUSDT.P",
        "SOLUSDT.P",
        "BNBUSDT.P",
        "XRPUSDT.P",
        "TRXUSDT.P",
        "ZECUSDT.P",
        "HYPEUSDT.P",
        "DOGEUSDT.P",
    ]
    assert [item["exchange_symbol"] for item in instruments] == [
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
        "BNBUSDT",
        "XRPUSDT",
        "TRXUSDT",
        "ZECUSDT",
        "HYPEUSDT",
        "DOGEUSDT",
    ]
    assert len({item["ticker"] for item in instruments}) == len(instruments)
    assert len({item["exchange_symbol"] for item in instruments}) == len(instruments)

    for instrument in instruments:
        assert instrument["canonical_timeframes"] == ["5m", "1h", "4h", "1d"]
        assert instrument["history_policy"] == "full_available"
        assert instrument["enabled"] is True
