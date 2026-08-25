"""Serialize canonical range results without floating-point conversion."""

from __future__ import annotations

from market_data_service.application.consumer_read.models import CandleRangeResult
from market_data_service.domain.candles import CanonicalCandle


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


def serialize_result(result: CandleRangeResult) -> dict[str, object]:
    return {
        "ticker": result.stream.instrument.ticker,
        "timeframe": result.stream.timeframe,
        "from_ms": result.from_ms,
        "to_ms": result.to_ms,
        "market_data_hash": result.market_data_hash,
        "candles": [_candle_document(candle) for candle in result.candles],
    }
