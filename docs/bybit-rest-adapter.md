# Bybit REST Candle Adapter

## Scope

The first external adapter fetches bounded closed-candle windows from Bybit V5:

```text
GET /v5/market/kline
category=linear
symbol=BTCUSDT
interval=1
start=<inclusive ms>
end=<inclusive ms>
limit<=1000
```

The service domain keeps half-open windows `[start_ms, end_ms)`. The adapter therefore sends
`end=end_ms-1` to Bybit.

## Responsibilities

The adapter:

- maps canonical ticker `BTCUSDT.P` to exchange symbol `BTCUSDT`;
- maps service timeframes through the central timeframe registry;
- rejects windows larger than the Bybit 1000-row limit;
- validates `retCode` and payload shape;
- parses decimal fields without converting through float;
- filters rows back into the requested half-open window;
- de-duplicates by open time;
- returns candles sorted by ascending open time;
- marks a REST candle confirmed only when its close boundary is before observation time.

The adapter does not:

- write SQLite;
- classify duplicate or correction;
- mutate stream lifecycle state directly;
- retry indefinitely;
- perform full-history orchestration.

## Window import

`ImportHistoricalWindow` is a small application boundary:

```text
fetch one bounded REST window
→ pass every observation through canonical ingestion decisions
→ commit the window in one storage transaction
→ return committed/duplicate/corrected/rejected counts
```

`ImportHistoricalWindow` is the canonical historical window path. It reuses the same
validation, classification, correction, quarantine, and stream-state advancement rules as
single-candle ingestion, but opens one unit of work for the whole REST response window.

## Verification

Tests cover:

- exact Bybit request parameters;
- half-open `end_ms-1` conversion;
- reverse response ordering normalized to ascending order;
- filtering rows outside the requested window;
- exact Decimal normalization;
- typed Bybit API errors;
- 1000-candle request bound;
- first import as committed candles;
- repeated import as duplicates in SQLite.

A real public-Bybit smoke must be run in an environment with outbound DNS/network access.
