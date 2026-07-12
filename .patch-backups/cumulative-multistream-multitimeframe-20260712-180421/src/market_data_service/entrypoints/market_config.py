"""Shared market-config parsing for administrative CLI commands."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path

from market_data_service.domain import InstrumentKey


@dataclass(frozen=True, slots=True)
class MarketConfigEntry:
    ticker: str
    exchange_symbol: str


def load_enabled_market_entries(path: Path) -> tuple[MarketConfigEntry, ...]:
    payload = tomllib.loads(path.read_text(encoding="utf-8"))
    entries: list[MarketConfigEntry] = []
    for item in payload.get("instruments", []):
        if item.get("enabled") is True:
            entries.append(
                MarketConfigEntry(
                    ticker=str(item["ticker"]),
                    exchange_symbol=str(item["exchange_symbol"]),
                )
            )
    return tuple(entries)


def entry_for_ticker(
    entries: tuple[MarketConfigEntry, ...],
    ticker: str,
) -> MarketConfigEntry:
    normalized = InstrumentKey(ticker).ticker
    for entry in entries:
        if entry.ticker == normalized:
            return entry
    raise ValueError(f"ticker is not enabled in market config: {normalized}")
