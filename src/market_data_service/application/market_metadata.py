"""Validate configured instrument mappings against exchange metadata."""

from __future__ import annotations

from dataclasses import dataclass

from market_data_service.domain.instruments import (
    ExchangeInstrumentSpecification,
    InstrumentCoverage,
)
from market_data_service.ports.market_data_source import InstrumentMetadataSource


class InstrumentMetadataMismatch(ValueError):
    """Configured identity does not match the exchange instrument."""


@dataclass(frozen=True, slots=True)
class VerifiedInstrumentMetadata:
    coverage: InstrumentCoverage
    specification: ExchangeInstrumentSpecification


class VerifyConfiguredInstrumentMetadata:
    def __init__(self, source: InstrumentMetadataSource, *, category: str) -> None:
        self._source = source
        self._category = category.strip().lower()

    def execute(self, coverage: InstrumentCoverage) -> VerifiedInstrumentMetadata:
        specification = self._source.get_instrument_specification(coverage.instrument)
        expected = {
            "exchange_symbol": coverage.exchange_symbol,
            "category": self._category,
            "contract_type": "LinearPerpetual",
            "status": "Trading",
            "settle_coin": "USDT",
        }
        actual = {
            "exchange_symbol": specification.exchange_symbol,
            "category": specification.category,
            "contract_type": specification.contract_type,
            "status": specification.status,
            "settle_coin": specification.settle_coin,
        }
        mismatches = [
            f"{name}: expected={expected[name]!r} actual={actual[name]!r}"
            for name in expected
            if actual[name] != expected[name]
        ]
        if mismatches:
            raise InstrumentMetadataMismatch(
                f"metadata mismatch for {coverage.instrument.ticker}: " + "; ".join(mismatches)
            )
        return VerifiedInstrumentMetadata(coverage=coverage, specification=specification)
