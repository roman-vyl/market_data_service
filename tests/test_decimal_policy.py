from __future__ import annotations

from decimal import Decimal

import pytest

from market_data_service.domain import (
    CanonicalCandle,
    CandleValidationCode,
    IngestionClassification,
    InstrumentKey,
    InvalidDecimalValue,
    ObservationSource,
    ObservedCandle,
    StreamKey,
    classify_against_existing,
    decimal_to_canonical_text,
    parse_canonical_decimal_text,
    parse_decimal,
    validate_observed_candle,
)


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("1", "1"),
        ("1.0", "1"),
        ("001.000", "1"),
        ("-0", "0"),
        ("-0.000", "0"),
        ("1E+3", "1000"),
        ("0.00100", "0.001"),
        ("12345678901234567890.00000001", "12345678901234567890.00000001"),
    ],
)
def test_decimal_text_has_one_canonical_representation(raw: str, expected: str) -> None:
    assert decimal_to_canonical_text(raw) == expected


@pytest.mark.parametrize("raw", ["NaN", "Infinity", "-Infinity", "", "abc"])
def test_non_finite_or_invalid_decimals_are_rejected(raw: str) -> None:
    with pytest.raises(InvalidDecimalValue):
        parse_decimal(raw)


def test_binary_float_input_is_rejected() -> None:
    with pytest.raises(InvalidDecimalValue, match="binary float"):
        parse_decimal(0.1)


def test_storage_parser_rejects_noncanonical_text() -> None:
    assert parse_canonical_decimal_text("1.5") == Decimal("1.5")
    with pytest.raises(InvalidDecimalValue, match="non-canonical"):
        parse_canonical_decimal_text("1.500")


def _observed(*, source: ObservationSource, close: str = "100.2500", volume: str = "12.3400") -> ObservedCandle:
    return ObservedCandle(
        stream=StreamKey(InstrumentKey("BTCUSDT.P"), "1m"),
        open_time_ms=60_000,
        close_time_ms=119_999,
        open="100.0",
        high="101.000",
        low="99.5000",
        close=close,
        volume=volume,
        confirmed=True,
        observed_at_ms=120_010,
        source=source,
    )


def test_rest_and_websocket_equivalent_decimal_spellings_are_duplicates() -> None:
    rest = _observed(source=ObservationSource.BYBIT_REST)
    existing = CanonicalCandle.from_observation(rest, committed_at_ms=120_020)
    websocket = ObservedCandle(
        stream=rest.stream,
        open_time_ms=rest.open_time_ms,
        close_time_ms=rest.close_time_ms,
        open="100",
        high="101",
        low="99.5",
        close="100.25",
        volume="12.34",
        confirmed=True,
        observed_at_ms=120_030,
        source=ObservationSource.BYBIT_WEBSOCKET,
    )
    assert existing.ohlcv_text == ("100", "101", "99.5", "100.25", "12.34")
    assert classify_against_existing(existing, websocket) is IngestionClassification.DUPLICATE


def test_changed_numeric_value_is_classified_as_correction() -> None:
    rest = _observed(source=ObservationSource.BYBIT_REST)
    existing = CanonicalCandle.from_observation(rest, committed_at_ms=120_020)
    changed = _observed(source=ObservationSource.BYBIT_WEBSOCKET, close="100.26")
    assert classify_against_existing(existing, changed) is IngestionClassification.CORRECTED


def test_candle_validation_uses_exact_decimals() -> None:
    valid = _observed(source=ObservationSource.BYBIT_REST)
    assert validate_observed_candle(valid) == ()

    negative_volume = _observed(source=ObservationSource.BYBIT_REST, volume="-0.0001")
    assert CandleValidationCode.NEGATIVE_VOLUME in {
        issue.code for issue in validate_observed_candle(negative_volume)
    }

    invalid_ohlc = ObservedCandle(
        stream=valid.stream,
        open_time_ms=valid.open_time_ms,
        close_time_ms=valid.close_time_ms,
        open="100",
        high="99.9",
        low="99.5",
        close="100.25",
        volume="1",
        confirmed=True,
        observed_at_ms=valid.observed_at_ms,
        source=valid.source,
    )
    assert CandleValidationCode.INVALID_OHLC in {
        issue.code for issue in validate_observed_candle(invalid_ohlc)
    }
