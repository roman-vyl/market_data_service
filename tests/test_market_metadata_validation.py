from __future__ import annotations

from dataclasses import replace

import pytest

from market_data_service.application.market_metadata import (
    InstrumentMetadataMismatch,
    VerifyConfiguredInstrumentMetadata,
)
from market_data_service.domain import (
    ExchangeInstrumentSpecification,
    HistoryPolicy,
    InstrumentCoverage,
    InstrumentKey,
)


class FakeMetadataSource:
    def __init__(self, specification: ExchangeInstrumentSpecification) -> None:
        self.specification = specification
        self.calls = 0

    def get_instrument_specification(
        self, instrument: InstrumentKey
    ) -> ExchangeInstrumentSpecification:
        self.calls += 1
        assert instrument == self.specification.instrument
        return self.specification

    def get_launch_time_ms(self, instrument: InstrumentKey) -> int:
        return self.get_instrument_specification(instrument).launch_time_ms


def _coverage() -> InstrumentCoverage:
    return InstrumentCoverage(
        instrument=InstrumentKey("BTCUSDT.P"),
        exchange_symbol="BTCUSDT",
        enabled=True,
        canonical_timeframes=("1m",),
        history_policy=HistoryPolicy.FULL_AVAILABLE,
    )


def _specification() -> ExchangeInstrumentSpecification:
    return ExchangeInstrumentSpecification(
        instrument=InstrumentKey("BTCUSDT.P"),
        exchange_symbol="BTCUSDT",
        category="linear",
        contract_type="LinearPerpetual",
        status="Trading",
        settle_coin="USDT",
        launch_time_ms=1,
    )


def test_accepts_exact_linear_perpetual_mapping() -> None:
    source = FakeMetadataSource(_specification())
    result = VerifyConfiguredInstrumentMetadata(source, category="linear").execute(_coverage())
    assert result.specification == source.specification
    assert source.calls == 1


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("exchange_symbol", "ETHUSDT"),
        ("category", "inverse"),
        ("contract_type", "LinearFutures"),
        ("status", "PreLaunch"),
        ("settle_coin", "USDC"),
    ],
)
def test_rejects_metadata_mismatch(field: str, value: str) -> None:
    source = FakeMetadataSource(replace(_specification(), **{field: value}))
    with pytest.raises(InstrumentMetadataMismatch, match=field):
        VerifyConfiguredInstrumentMetadata(source, category="linear").execute(_coverage())
