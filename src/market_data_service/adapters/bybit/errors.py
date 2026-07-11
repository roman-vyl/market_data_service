"""Typed failures raised by the Bybit market-data adapter."""

from __future__ import annotations

from market_data_service.ports.market_data_source import RecoverableMarketDataFailure


class BybitMarketDataError(RuntimeError):
    """Base error for Bybit market-data operations."""


class BybitHttpError(BybitMarketDataError, RecoverableMarketDataFailure):
    """HTTP transport failed or returned an unusable status."""


class BybitApiError(BybitMarketDataError):
    """Bybit returned a non-zero API result code."""


class BybitPayloadError(BybitMarketDataError):
    """Bybit returned a malformed or semantically invalid payload."""
