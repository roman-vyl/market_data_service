"""Exact decimal parsing and canonical text normalization."""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import TypeAlias

DecimalInput: TypeAlias = Decimal | str | int


class InvalidDecimalValue(ValueError):
    """Raised when an incoming numeric value is not a finite exact decimal."""


def parse_decimal(value: DecimalInput) -> Decimal:
    """Parse an exact finite decimal without accepting binary floats."""

    if isinstance(value, bool):
        raise InvalidDecimalValue("boolean is not a decimal value")
    if isinstance(value, float):
        raise InvalidDecimalValue("binary float input is not allowed")

    try:
        parsed = value if isinstance(value, Decimal) else Decimal(str(value).strip())
    except (InvalidOperation, ValueError) as exc:
        raise InvalidDecimalValue(f"invalid decimal value: {value!r}") from exc

    if not parsed.is_finite():
        raise InvalidDecimalValue("decimal value must be finite")

    if parsed.is_zero():
        return Decimal(0)
    return parsed


def decimal_to_canonical_text(value: DecimalInput) -> str:
    """Return one non-exponential textual representation per numeric value."""

    parsed = parse_decimal(value)
    text = format(parsed, "f")

    if "." in text:
        text = text.rstrip("0").rstrip(".")

    if text in {"", "-0"}:
        return "0"
    return text


def parse_canonical_decimal_text(value: str) -> Decimal:
    """Parse storage/API decimal text and require canonical formatting."""

    canonical = decimal_to_canonical_text(value)
    if value != canonical:
        raise InvalidDecimalValue(f"non-canonical decimal text: {value!r}; expected {canonical!r}")
    return Decimal(canonical)
