# Exact Decimal Policy

## Decision

Canonical OHLCV values use exact decimal semantics:

- domain: Python `Decimal`;
- SQLite: normalized decimal `TEXT`;
- HTTP/JSON API: decimal strings;
- research adapters: explicit conversion to numeric dataframe types when requested.

Binary floating point is not accepted by the canonical ingestion boundary.

## Why

Bybit sends price and volume values as decimal strings. Converting them to binary floats before duplicate/correction classification can introduce representation noise. Raw string comparison is also insufficient because `"1.0"` and `"1.000"` are numerically equal.

The service therefore converts each value to one canonical non-exponential text representation before persistence or equality comparison.

## Canonical examples

```text
"00104.5000" -> "104.5"
"0.000"      -> "0"
"-0.000"     -> "0"
"1E+3"       -> "1000"
"0.00100"    -> "0.001"
```

The following are rejected:

```text
NaN
Infinity
-Infinity
empty text
non-numeric text
binary float input
```

## Responsibility split

```text
Bybit adapter
  -> preserves incoming decimal strings

domain decimal parser
  -> parses finite Decimal
  -> normalizes canonical text

domain candle validation
  -> validates exact OHLC and volume relationships

application ingestion
  -> classifies insert / duplicate / correction

SQLite adapter
  -> persists already canonical decimal text
```

SQLite repositories must not invent their own numeric normalization rules.

## Equality and correction semantics

Two candles are numeric duplicates when all five normalized OHLCV texts match.

Example:

```text
REST:      close="104250.5000"
WebSocket: close="104250.5"
```

Both normalize to `"104250.5"`, so the second observation is a duplicate.

A numerically different normalized value is a correction candidate and follows the approved REST-authority/quarantine policy.

## Validation

Validation uses exact `Decimal` comparisons:

- all values must be finite;
- volume must be non-negative;
- high must be at least open, close, and low;
- low must be at most open, close, and high.

No rounding or quantization is performed during canonical persistence.
