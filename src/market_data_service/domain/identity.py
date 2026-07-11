"""Canonical instrument and candle-stream identities."""

from __future__ import annotations

from dataclasses import dataclass

from market_data_service.domain.timeframes import get_timeframe


@dataclass(frozen=True, slots=True, order=True)
class InstrumentKey:
    """Stable service identity of one configured market instrument.

    The service uses a human-facing canonical ticker such as ``BTCUSDT.P``.
    The exchange API symbol is transport metadata and is intentionally not part
    of the identity.
    """

    ticker: str

    def __post_init__(self) -> None:
        ticker = self.ticker.strip().upper()
        if not ticker:
            raise ValueError("ticker must not be empty")
        if not ticker.isascii():
            raise ValueError("ticker must contain only ASCII characters")
        if not ticker.endswith(".P"):
            raise ValueError("v1 ticker must use the perpetual suffix .P")
        core = ticker[:-2]
        if not core or not core.isalnum():
            raise ValueError("ticker core must contain only ASCII letters and digits")
        object.__setattr__(self, "ticker", ticker)

    @property
    def canonical_id(self) -> str:
        return self.ticker


@dataclass(frozen=True, slots=True, order=True)
class StreamKey:
    """Stable identity of one canonical candle stream."""

    instrument: InstrumentKey
    timeframe: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "timeframe", get_timeframe(self.timeframe).id)

    @property
    def canonical_id(self) -> str:
        return f"{self.instrument.canonical_id}:{self.timeframe}"
