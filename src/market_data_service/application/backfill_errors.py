"""Compatibility facade for the shared source-failure classifier."""

from __future__ import annotations

from market_data_service.application.source_failure import (
    SourceFailureDecision as BackfillFailureDecision,
)
from market_data_service.application.source_failure import classify_source_failure


def classify_backfill_failure(exc: Exception) -> BackfillFailureDecision:
    return classify_source_failure(exc)
