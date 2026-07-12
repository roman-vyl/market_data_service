"""Shared market-config helpers for administrative CLI commands."""

from __future__ import annotations

from pathlib import Path

from market_data_service.config import ValidatedMarketConfig, load_market_config
from market_data_service.domain import InstrumentCoverage, InstrumentKey


def load_enabled_market_entries(path: Path) -> tuple[InstrumentCoverage, ...]:
    """Compatibility boundary returning fully validated enabled coverages."""

    return load_market_config(path).enabled_instruments


def load_validated_market_config(path: Path) -> ValidatedMarketConfig:
    return load_market_config(path)


def entry_for_ticker(
    entries: tuple[InstrumentCoverage, ...],
    ticker: str,
) -> InstrumentCoverage:
    normalized = InstrumentKey(ticker)
    for entry in entries:
        if entry.instrument == normalized:
            return entry
    raise ValueError(f"ticker is not enabled in market config: {normalized.ticker}")
