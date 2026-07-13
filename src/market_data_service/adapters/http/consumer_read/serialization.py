"""Serialize canonical range results without floating-point conversion."""

from __future__ import annotations

from market_data_service.application.consumer_read.models import CandleRangeResult


def serialize_result(result: CandleRangeResult) -> dict[str, object]:
    return {
        "ticker": result.stream.instrument.ticker,
        "timeframe": result.stream.timeframe,
        "from_ms": result.from_ms,
        "to_ms": result.to_ms,
        "candles": [
            {
                "open_time_ms": candle.open_time_ms,
                "open": candle.ohlcv_text[0],
                "high": candle.ohlcv_text[1],
                "low": candle.ohlcv_text[2],
                "close": candle.ohlcv_text[3],
                "volume": candle.ohlcv_text[4],
            }
            for candle in result.candles
        ],
    }
