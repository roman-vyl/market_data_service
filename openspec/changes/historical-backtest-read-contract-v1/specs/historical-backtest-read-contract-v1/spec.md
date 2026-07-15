# Historical backtest read contract v1

## Requirements

### Separate historical admission
MDS SHALL expose `POST /v1/historical-candles` for an explicit configured stream and aligned half-open range. The endpoint SHALL NOT require global `ready` state.

### Hash-bound read
The request SHALL include `expected_market_data_hash`. MDS SHALL recompute its canonical hash over the exact returned range and SHALL return candles only when the value matches.

### Runtime isolation
The existing `GET /v1/candles` endpoint SHALL retain its current readiness gate.

### No hidden mutation
Historical read and continuity audit SHALL NOT invoke repair, backfill, upstream REST, or lifecycle transitions.
