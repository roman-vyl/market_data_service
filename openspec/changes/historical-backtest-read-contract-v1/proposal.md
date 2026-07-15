# Proposal: Historical backtest read contract v1

## Motivation

The released runtime candle endpoint is correctly gated by global stream readiness, but Research Service backtests operate on an already audited explicit historical interval. A degraded realtime stream can still contain a complete historical interval. MDS must expose a separate read path without weakening runtime admission.

## Scope

- add `POST /v1/historical-candles`;
- require an explicit half-open range and `expected_market_data_hash`;
- read canonical committed candles without requiring `state == ready`;
- recompute the MDS-owned hash and reject stale coverage;
- keep `GET /v1/candles` readiness-gated;
- do not add sessions, leases, binding IDs, tokens, repair, or backfill side effects.
