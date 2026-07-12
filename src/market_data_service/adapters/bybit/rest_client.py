"""Bybit V5 REST adapter for bounded historical candle windows."""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from market_data_service.adapters.bybit.errors import (
    BybitApiError,
    BybitHttpError,
    BybitPayloadError,
    BybitTransientApiError,
)
from market_data_service.adapters.bybit.http_transport import (
    JsonHttpTransport,
    UrllibJsonHttpTransport,
)
from market_data_service.adapters.bybit.kline_parser import parse_kline_rows
from market_data_service.domain.candles import ObservedCandle
from market_data_service.domain.identity import InstrumentKey, StreamKey
from market_data_service.domain.instruments import ExchangeInstrumentSpecification
from market_data_service.domain.timeframes import get_timeframe
from market_data_service.domain.windows import TimeWindow

_TRANSIENT_RET_CODES = {10000, 10006, 10016}


@dataclass(slots=True)
class BybitRestCandleSource:
    """Fetch closed candles and instrument facts without storage knowledge."""

    exchange_symbols: dict[str, str]
    base_url: str = "https://api.bybit.com"
    category: str = "linear"
    timeout_seconds: float = 10.0
    transport: JsonHttpTransport = field(default_factory=UrllibJsonHttpTransport)
    retry_attempts: int = 4
    retry_base_delay_seconds: float = 1.0
    retry_rate_limit_delay_seconds: float = 5.0
    retry_max_delay_seconds: float = 8.0
    retry_jitter_ratio: float = 0.25
    sleeper: Callable[[float], None] = time.sleep
    randomizer: Callable[[], float] = random.random
    _instrument_cache: dict[str, ExchangeInstrumentSpecification] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )

    def get_instrument_specification(
        self,
        instrument: InstrumentKey,
    ) -> ExchangeInstrumentSpecification:
        cached = self._instrument_cache.get(instrument.ticker)
        if cached is not None:
            return cached
        symbol = self._exchange_symbol_for_instrument(instrument)
        payload = self._get_json_with_retry(
            f"{self.base_url.rstrip('/')}/v5/market/instruments-info",
            {"category": self.category, "symbol": symbol},
        )
        rows = _extract_rows(payload)
        for row in rows:
            if not isinstance(row, dict):
                raise BybitPayloadError("Bybit instrument row must be an object")
            if row.get("symbol") != symbol:
                continue
            specification = ExchangeInstrumentSpecification(
                instrument=instrument,
                exchange_symbol=_required_string(row, "symbol", "instrument"),
                category=self.category,
                contract_type=_required_string(row, "contractType", "instrument"),
                status=_required_string(row, "status", "instrument"),
                settle_coin=_required_string(row, "settleCoin", "instrument"),
                launch_time_ms=_required_non_negative_int(row, "launchTime", "instrument"),
            )
            self._instrument_cache[instrument.ticker] = specification
            return specification
        raise BybitPayloadError(f"Bybit instrument metadata missing for symbol {symbol}")

    def get_launch_time_ms(self, instrument: InstrumentKey) -> int:
        cached = self._instrument_cache.get(instrument.ticker)
        if cached is not None:
            return cached.launch_time_ms
        symbol = self._exchange_symbol_for_instrument(instrument)
        payload = self._get_json_with_retry(
            f"{self.base_url.rstrip('/')}/v5/market/instruments-info",
            {"category": self.category, "symbol": symbol},
        )
        rows = _extract_rows(payload)
        for row in rows:
            if not isinstance(row, dict):
                raise BybitPayloadError("Bybit instrument row must be an object")
            if row.get("symbol") == symbol:
                return _required_non_negative_int(row, "launchTime", "instrument")
        raise BybitPayloadError(f"Bybit instrument metadata missing for symbol {symbol}")

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
        payload = self._get_json_with_retry(
            f"{self.base_url.rstrip('/')}/v5/market/kline",
            {
                "category": self.category,
                "symbol": symbol,
                "interval": timeframe.bybit_interval,
                "start": window.start_ms,
                "end": window.end_ms - 1,
                "limit": max_rows,
            },
        )
        rows = _extract_rows(payload)
        return parse_kline_rows(
            rows,
            stream=stream,
            requested_window=window,
            observed_at_ms=observed_at_ms,
        )

    def _exchange_symbol(self, stream: StreamKey) -> str:
        return self._exchange_symbol_for_instrument(stream.instrument)

    def _exchange_symbol_for_instrument(self, instrument: InstrumentKey) -> str:
        try:
            return self.exchange_symbols[instrument.ticker]
        except KeyError as exc:
            raise ValueError(f"no Bybit symbol configured for {instrument.ticker}") from exc

    def _get_json_with_retry(
        self,
        url: str,
        params: dict[str, str | int],
    ) -> dict[str, Any]:
        attempts = max(1, self.retry_attempts)
        for attempt in range(1, attempts + 1):
            try:
                return self.transport.get_json(url, params, self.timeout_seconds)
            except (BybitHttpError, BybitTransientApiError) as exc:
                if attempt == attempts:
                    raise
                self.sleeper(self._retry_delay_seconds(exc, attempt))
        raise AssertionError("retry loop exhausted without raising")

    def _retry_delay_seconds(
        self,
        exc: BybitHttpError | BybitTransientApiError,
        attempt: int,
    ) -> float:
        if isinstance(exc, BybitHttpError) and exc.retry_after_seconds is not None:
            return max(0.0, min(self.retry_max_delay_seconds, exc.retry_after_seconds))
        base_delay = (
            self.retry_rate_limit_delay_seconds
            if _is_rate_limit_error(exc)
            else self.retry_base_delay_seconds
        )
        delay = min(
            self.retry_max_delay_seconds,
            base_delay * (2 ** max(0, attempt - 1)),
        )
        jitter = delay * self.retry_jitter_ratio * float(self.randomizer())
        return float(min(self.retry_max_delay_seconds, delay + jitter))


def _extract_rows(payload: dict[str, Any]) -> list[Any]:
    ret_code = payload.get("retCode")
    if ret_code != 0:
        message = payload.get("retMsg", "unknown Bybit API error")
        if ret_code in _TRANSIENT_RET_CODES:
            raise BybitTransientApiError(
                f"Bybit retCode={ret_code}: {message}",
                ret_code=ret_code,
            )
        raise BybitApiError(f"Bybit retCode={ret_code}: {message}")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise BybitPayloadError("Bybit result must be an object")
    rows = result.get("list")
    if not isinstance(rows, list):
        raise BybitPayloadError("Bybit result.list must be an array")
    return rows


def _required_string(row: dict[str, Any], key: str, context: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value.strip():
        raise BybitPayloadError(f"Bybit {context}.{key} must be a non-empty string")
    return value.strip()


def _required_non_negative_int(row: dict[str, Any], key: str, context: str) -> int:
    value = row.get(key)
    if not isinstance(value, str | int):
        raise BybitPayloadError(f"Bybit {context}.{key} must be an integer")
    try:
        parsed = int(value)
    except ValueError as exc:
        raise BybitPayloadError(f"Bybit {context}.{key} must be an integer") from exc
    if parsed < 0:
        raise BybitPayloadError(f"Bybit {context}.{key} must be non-negative")
    return parsed


def _is_rate_limit_error(exc: BybitHttpError | BybitTransientApiError) -> bool:
    if isinstance(exc, BybitTransientApiError):
        return exc.ret_code == 10006
    return exc.status_code == 429
