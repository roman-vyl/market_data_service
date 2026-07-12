from __future__ import annotations

import pytest

from market_data_service.domain import (
    HistoryPolicy,
    InstrumentCoverage,
    InstrumentKey,
    InstrumentMetadata,
    StreamKey,
)


def test_instrument_identity_is_ticker_and_is_normalized() -> None:
    key = InstrumentKey("btcusdt.p")
    assert key.ticker == "BTCUSDT.P"
    assert key.canonical_id == "BTCUSDT.P"


def test_exchange_symbol_is_metadata_not_identity() -> None:
    key = InstrumentKey("BTCUSDT.P")
    metadata = InstrumentMetadata(key, "btcusdt", launch_time_ms=1, fetched_at_ms=2)
    assert metadata.instrument == key
    assert metadata.exchange_symbol == "BTCUSDT"


def test_instrument_key_requires_perpetual_ticker_suffix() -> None:
    with pytest.raises(ValueError, match="perpetual suffix"):
        InstrumentKey("BTCUSDT")


def test_stream_key_requires_registered_timeframe() -> None:
    key = InstrumentKey("ETHUSDT.P")
    assert StreamKey(key, "1M").canonical_id == "ETHUSDT.P:1m"
    with pytest.raises(ValueError, match="unsupported timeframe"):
        StreamKey(key, "2m")


def test_coverage_accepts_declared_streams_and_requires_unique_timeframes() -> None:
    key = InstrumentKey("BTCUSDT.P")
    coverage = InstrumentCoverage(
        instrument=key,
        exchange_symbol="btcusdt",
        enabled=True,
        canonical_timeframes=("1d", "1h"),
        history_policy=HistoryPolicy.FULL_AVAILABLE,
    )
    assert tuple(stream.canonical_id for stream in coverage.stream_keys) == (
        "BTCUSDT.P:1d",
        "BTCUSDT.P:1h",
    )
    assert coverage.exchange_symbol == "BTCUSDT"

    with pytest.raises(ValueError, match="must not contain duplicates"):
        InstrumentCoverage(key, "BTCUSDT", True, ("1m", "1M"), HistoryPolicy.FULL_AVAILABLE)


def test_same_timeframe_isolated_by_ticker() -> None:
    btc = StreamKey(InstrumentKey("BTCUSDT.P"), "1m")
    eth = StreamKey(InstrumentKey("ETHUSDT.P"), "1m")
    assert btc != eth
    assert btc.canonical_id != eth.canonical_id
