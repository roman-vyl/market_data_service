# Research history integration v1

MDS now exposes the read-only facts required by Research Service history-window planning:

- `GET /v1/streams/{ticker}/{timeframe}/bounds`;
- `POST /v1/streams/{ticker}/{timeframe}/continuity-audits`;
- `market_data_hash` in successful `GET /v1/candles` responses.

The bounds endpoint reads actual committed candle minima and maxima. The audit endpoint delegates to the existing side-effect-free continuity use case and reports exact gaps. Neither endpoint requires a globally ready stream or invokes repair/backfill.

The candle hash is owned by MDS and identifies the exact ordered response range and canonical decimal candle payload.

## Integration caveat

The existing `/v1/candles` route still requires `StreamLifecycleState.READY`. This change intentionally preserves that released consumer-read contract. Bounds and audit therefore enable planning against a degraded-but-continuous historical interval, but reading candles for that interval may require a separately approved historical-read contract before the complete three-service degraded scenario can run.


## Historical backtest read

`POST /v1/historical-candles` accepts ticker, timeframe, exact bounds, and the hash returned by continuity audit. It bypasses only global runtime readiness; it still requires a complete canonical grid and the unchanged audited hash. A mismatch returns `409 coverage_stale`.
