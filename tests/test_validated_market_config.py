from __future__ import annotations

from pathlib import Path

import pytest

from market_data_service.config import MarketConfigError, load_market_config

VALID = '''
schema_version = 1
[source]
venue = "bybit"
category = "linear"
[[instruments]]
ticker = "BTCUSDT.P"
exchange_symbol = "BTCUSDT"
enabled = true
canonical_timeframes = ["1m"]
history_policy = "full_available"
[[instruments]]
ticker = "ETHUSDT.P"
exchange_symbol = "ETHUSDT"
enabled = true
canonical_timeframes = ["1m"]
history_policy = "full_available"
'''


def _write(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "markets.toml"
    path.write_text(content, encoding="utf-8")
    return path


def test_loads_fully_validated_config_in_declared_order(tmp_path: Path) -> None:
    config = load_market_config(_write(tmp_path, VALID))

    assert config.schema_version == 1
    assert config.source.venue == "bybit"
    assert config.source.category == "linear"
    assert [item.instrument.ticker for item in config.enabled_instruments] == [
        "BTCUSDT.P",
        "ETHUSDT.P",
    ]
    assert [stream.canonical_id for stream in config.enabled_streams] == [
        "BTCUSDT.P:1m",
        "ETHUSDT.P:1m",
    ]


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        ("schema_version = 1", "schema_version = 2", "schema_version"),
        ('venue = "bybit"', 'venue = "other"', "source.venue"),
        ('category = "linear"', 'category = "spot"', "source.category"),
        ('enabled = true', 'enabled = "yes"', "enabled"),
        ('canonical_timeframes = ["1m"]', 'canonical_timeframes = ["5m"]', "1m"),
        ('history_policy = "full_available"', 'history_policy = "recent"', "HistoryPolicy"),
    ],
)
def test_rejects_invalid_normative_fields(
    tmp_path: Path,
    old: str,
    new: str,
    message: str,
) -> None:
    with pytest.raises(MarketConfigError, match=message):
        load_market_config(_write(tmp_path, VALID.replace(old, new, 1)))


def test_rejects_duplicate_canonical_ticker(tmp_path: Path) -> None:
    duplicate = VALID.replace('ticker = "ETHUSDT.P"', 'ticker = "btcusdt.p"')
    with pytest.raises(MarketConfigError, match="duplicate canonical ticker"):
        load_market_config(_write(tmp_path, duplicate))


def test_rejects_duplicate_exact_exchange_symbol(tmp_path: Path) -> None:
    duplicate = VALID.replace('exchange_symbol = "ETHUSDT"', 'exchange_symbol = "BTCUSDT"')
    with pytest.raises(MarketConfigError, match="duplicate exact exchange symbol"):
        load_market_config(_write(tmp_path, duplicate))
