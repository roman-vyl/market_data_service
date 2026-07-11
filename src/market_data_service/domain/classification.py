"""Canonical ingestion classification outcomes."""

from __future__ import annotations

from enum import StrEnum


class IngestionClassification(StrEnum):
    COMMITTED = "committed"
    DUPLICATE = "duplicate"
    CORRECTED = "corrected"
    REJECTED_INVALID = "rejected_invalid"
    REJECTED_UNCONFIGURED = "rejected_unconfigured"
    REJECTED_UNCONFIRMED = "rejected_unconfirmed"
