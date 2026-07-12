"""Validated versioned market configuration."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from market_data_service.domain.identity import InstrumentKey, StreamKey
from market_data_service.domain.instruments import HistoryPolicy, InstrumentCoverage

SUPPORTED_SCHEMA_VERSION = 1
SUPPORTED_VENUE = "bybit"
SUPPORTED_CATEGORY = "linear"


class MarketConfigError(ValueError):
    """Raised when operator market configuration is invalid."""


@dataclass(frozen=True, slots=True)
class MarketSourceConfig:
    venue: str
    category: str


@dataclass(frozen=True, slots=True)
class ValidatedMarketConfig:
    schema_version: int
    source: MarketSourceConfig
    instruments: tuple[InstrumentCoverage, ...]

    @property
    def enabled_instruments(self) -> tuple[InstrumentCoverage, ...]:
        return tuple(item for item in self.instruments if item.enabled)

    @property
    def enabled_streams(self) -> tuple[StreamKey, ...]:
        return tuple(
            stream
            for coverage in self.enabled_instruments
            for stream in coverage.stream_keys
        )

    @property
    def exchange_symbols(self) -> dict[str, str]:
        return {
            item.instrument.ticker: item.exchange_symbol
            for item in self.enabled_instruments
        }


def load_market_config(path: Path) -> ValidatedMarketConfig:
    try:
        payload = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise MarketConfigError(f"cannot load market config {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise MarketConfigError("market config root must be a table")

    schema_version = _required_int(payload, "schema_version", context="root")
    if schema_version != SUPPORTED_SCHEMA_VERSION:
        raise MarketConfigError(
            f"unsupported schema_version={schema_version}; expected {SUPPORTED_SCHEMA_VERSION}"
        )

    source_payload = _required_table(payload, "source", context="root")
    venue = _required_string(source_payload, "venue", context="source").lower()
    category = _required_string(source_payload, "category", context="source").lower()
    if venue != SUPPORTED_VENUE:
        raise MarketConfigError(f"unsupported source.venue={venue!r}")
    if category != SUPPORTED_CATEGORY:
        raise MarketConfigError(f"unsupported source.category={category!r}")

    raw_instruments = payload.get("instruments")
    if not isinstance(raw_instruments, list) or not raw_instruments:
        raise MarketConfigError("instruments must be a non-empty array of tables")

    coverages: list[InstrumentCoverage] = []
    for index, raw in enumerate(raw_instruments):
        context = f"instruments[{index}]"
        if not isinstance(raw, dict):
            raise MarketConfigError(f"{context} must be a table")
        ticker = _required_string(raw, "ticker", context=context)
        exchange_symbol = _required_string(raw, "exchange_symbol", context=context)
        enabled = _required_bool(raw, "enabled", context=context)
        raw_timeframes = raw.get("canonical_timeframes")
        if not isinstance(raw_timeframes, list) or not raw_timeframes:
            raise MarketConfigError(f"{context}.canonical_timeframes must be a non-empty array")
        if not all(isinstance(value, str) for value in raw_timeframes):
            raise MarketConfigError(f"{context}.canonical_timeframes must contain strings")
        history_policy_raw = _required_string(raw, "history_policy", context=context)
        try:
            history_policy = HistoryPolicy(history_policy_raw)
            coverage = InstrumentCoverage(
                instrument=InstrumentKey(ticker),
                exchange_symbol=exchange_symbol,
                enabled=enabled,
                canonical_timeframes=tuple(raw_timeframes),
                history_policy=history_policy,
            )
        except ValueError as exc:
            raise MarketConfigError(f"invalid {context}: {exc}") from exc
        coverages.append(coverage)

    _reject_duplicates(coverages)
    return ValidatedMarketConfig(
        schema_version=schema_version,
        source=MarketSourceConfig(venue=venue, category=category),
        instruments=tuple(coverages),
    )


def _reject_duplicates(coverages: list[InstrumentCoverage]) -> None:
    tickers: set[str] = set()
    symbols: set[str] = set()
    streams: set[StreamKey] = set()
    for coverage in coverages:
        ticker = coverage.instrument.ticker
        if ticker in tickers:
            raise MarketConfigError(f"duplicate canonical ticker: {ticker}")
        tickers.add(ticker)
        if coverage.exchange_symbol in symbols:
            raise MarketConfigError(
                f"duplicate exact exchange symbol: {coverage.exchange_symbol}"
            )
        symbols.add(coverage.exchange_symbol)
        for stream in coverage.stream_keys:
            if stream in streams:
                raise MarketConfigError(
                    f"duplicate normalized stream identity: {stream.canonical_id}"
                )
            streams.add(stream)


def _required_table(payload: dict[str, Any], key: str, *, context: str) -> dict[str, Any]:
    value = payload.get(key)
    if not isinstance(value, dict):
        raise MarketConfigError(f"{context}.{key} must be a table")
    return value


def _required_string(payload: dict[str, Any], key: str, *, context: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value.strip():
        raise MarketConfigError(f"{context}.{key} must be a non-empty string")
    return value.strip()


def _required_bool(payload: dict[str, Any], key: str, *, context: str) -> bool:
    value = payload.get(key)
    if not isinstance(value, bool):
        raise MarketConfigError(f"{context}.{key} must be a boolean")
    return value


def _required_int(payload: dict[str, Any], key: str, *, context: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise MarketConfigError(f"{context}.{key} must be an integer")
    return value
