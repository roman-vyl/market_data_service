"""Canonical identity for one exact ordered candle-range response."""

from __future__ import annotations

import hashlib
import json

from market_data_service.domain.candles import CanonicalCandle
from market_data_service.domain.identity import StreamKey


def _candle_document(candle: CanonicalCandle) -> dict[str, object]:
    # ohlcv_text recomputes all five decimal->text conversions on every
    # access (it is a plain @property); accessing it once per candle here
    # instead of once per field avoids 5x redundant conversions per candle.
    open_text, high_text, low_text, close_text, volume_text = candle.ohlcv_text
    return {
        "open_time_ms": candle.open_time_ms,
        "open": open_text,
        "high": high_text,
        "low": low_text,
        "close": close_text,
        "volume": volume_text,
    }


def canonical_market_data_hash(
    *,
    stream: StreamKey,
    from_ms: int,
    to_ms: int,
    candles: tuple[CanonicalCandle, ...],
) -> str:
    document = {
        "ticker": stream.instrument.ticker,
        "timeframe": stream.timeframe,
        "from_ms": from_ms,
        "to_ms": to_ms,
        "candles": [_candle_document(candle) for candle in candles],
    }
    payload = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
