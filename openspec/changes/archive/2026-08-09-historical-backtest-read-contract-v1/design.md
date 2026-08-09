# Design: Historical backtest read contract v1

## Contract flow

1. Research obtains committed bounds when needed.
2. Research requests a side-effect-free continuity audit for `[from_ms, to_ms)`.
3. A continuous audit returns the canonical `market_data_hash` for that exact ordered candle set.
4. Research passes the range and hash to Strategy Engine.
5. Strategy Engine and Research independently call `POST /v1/historical-candles` with the same range and expected hash.
6. MDS reads the range, validates its complete grid, recomputes the hash, and returns candles only when the hash still matches.

## Admission semantics

`GET /v1/candles` remains runtime-oriented and requires `ready`. `POST /v1/historical-candles` is backtest-oriented and ignores global lifecycle state. It does not ignore data integrity: the requested range must be complete and must match the audit hash.

## Provenance

`market_data_hash` is a deterministic provenance identity, not a secret, session, lease, or authorization token. No binding table or signing key is introduced. If storage changes between audit and read, MDS returns `409 coverage_stale`; the caller must restart planning.

## Error mapping

- malformed request: `422 invalid_request`;
- unknown configured stream: `404 configured_stream_not_found`;
- incomplete or gapped requested range: existing consumer-read range/grid error;
- hash mismatch: `409 coverage_stale`.
