"""Bybit V5 REST adapter for bounded historical candle windows."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from market_data_service.adapters.bybit.errors import BybitApiError, BybitPayloadError
from market_data_service.adapters.bybit.http_transport import (
    JsonHttpTransport,
    UrllibJsonHttpTransport,
)
from market_data_service.adapters.bybit.kline_parser import parse_kline_rows
from market_data_service.domain.candles import ObservedCandle
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.timeframes import get_timeframe
from market_data_service.domain.windows import TimeWindow


@dataclass(slots=True)
class BybitRestCandleSource:
    """Fetch closed candles without knowing storage or ingestion policy."""

    exchange_symbols: dict[str, str]
    base_url: str = "https://api.bybit.com"
    category: str = "linear"
    timeout_seconds: float = 10.0
    transport: JsonHttpTransport = field(default_factory=UrllibJsonHttpTransport)

    def fetch_closed_candles(
        self,
        stream: StreamKey,
        window: TimeWindow,
        *,
        observed_at_ms: int,
    ) -> tuple[ObservedCandle, ...]:
        symbol = self._exchange_symbol(stream)
        timeframe = get_timeframe(stream.timeframe)
        max_rows = max(1, window.duration_ms // timeframe.duration_ms)
        if max_rows > 1000:
            raise ValueError("Bybit kline window must contain no more than 1000 candles")
        payload = self.transport.get_json(
            f"{self.base_url.rstrip('/')}/v5/market/kline",
            {
                "category": self.category,
                "symbol": symbol,
                "interval": timeframe.bybit_interval,
                "start": window.start_ms,
                "end": window.end_ms - 1,
                "limit": max_rows,
            },
            self.timeout_seconds,
        )
        rows = _extract_rows(payload)
        return parse_kline_rows(
            rows,
            stream=stream,
            requested_window=window,
            observed_at_ms=observed_at_ms,
        )

    def _exchange_symbol(self, stream: StreamKey) -> str:
        try:
            return self.exchange_symbols[stream.instrument.ticker]
        except KeyError as exc:
            raise ValueError(f"no Bybit symbol configured for {stream.instrument.ticker}") from exc


def _extract_rows(payload: dict[str, Any]) -> list[Any]:
    ret_code = payload.get("retCode")
    if ret_code != 0:
        message = payload.get("retMsg", "unknown Bybit API error")
        raise BybitApiError(f"Bybit retCode={ret_code}: {message}")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise BybitPayloadError("Bybit result must be an object")
    rows = result.get("list")
    if not isinstance(rows, list):
        raise BybitPayloadError("Bybit result.list must be an array")
    return rows
