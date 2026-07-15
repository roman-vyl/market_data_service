"""Canonical identity for one exact ordered candle-range response."""

from __future__ import annotations

import hashlib
import json

from market_data_service.domain.candles import CanonicalCandle
from market_data_service.domain.identity import StreamKey


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
        "candles": [
            {
                "open_time_ms": candle.open_time_ms,
                "open": candle.ohlcv_text[0],
                "high": candle.ohlcv_text[1],
                "low": candle.ohlcv_text[2],
                "close": candle.ohlcv_text[3],
                "volume": candle.ohlcv_text[4],
            }
            for candle in candles
        ],
    }
    payload = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
