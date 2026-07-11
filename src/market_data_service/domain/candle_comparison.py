"""Pure canonical OHLCV equality and conflict classification."""

from __future__ import annotations

from market_data_service.domain.candles import CanonicalCandle, ObservedCandle
from market_data_service.domain.classification import IngestionClassification


def classify_against_existing(
    existing: CanonicalCandle | None,
    incoming: ObservedCandle,
) -> IngestionClassification:
    """Classify a validated observation against canonical storage state."""

    if existing is None:
        return IngestionClassification.COMMITTED
    if existing.stream != incoming.stream or existing.open_time_ms != incoming.open_time_ms:
        raise ValueError("existing and incoming candle identities must match")
    if existing.ohlcv_text == incoming.ohlcv_text:
        return IngestionClassification.DUPLICATE
    return IngestionClassification.CORRECTED
