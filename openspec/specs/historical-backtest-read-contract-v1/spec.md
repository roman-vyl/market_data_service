# historical-backtest-read-contract-v1 Specification

## Purpose
Defines side-effect-free hash-bound historical candle reads that are independent of runtime readiness.
## Requirements
### Requirement: Separate historical admission

MDS SHALL expose `POST /v1/historical-candles` for an explicit configured stream and aligned half-open range. The endpoint SHALL NOT require global `ready` state.

#### Scenario: Historical read succeeds for a degraded stream with a matching audit hash

- **WHEN** a configured stream is `degraded` and a `POST /v1/historical-candles` request supplies an explicit aligned half-open range with the matching `expected_market_data_hash`
- **THEN** the endpoint returns the requested candles without requiring the stream's global state to be `ready`

#### Scenario: Malformed or unaligned historical request is rejected

- **WHEN** a `POST /v1/historical-candles` request has a malformed body or a range not aligned to the stream's timeframe grid
- **THEN** the endpoint rejects the request

### Requirement: Hash-bound read

The request SHALL include `expected_market_data_hash`. MDS SHALL recompute its canonical hash over the exact returned range and SHALL return candles only when the value matches.

#### Scenario: Matching expected hash admits the historical read

- **WHEN** a `POST /v1/historical-candles` request's `expected_market_data_hash` matches the recomputed canonical hash over the exact requested range
- **THEN** candles may be returned

#### Scenario: Stale or mismatched hash is rejected as coverage_stale

- **WHEN** a `POST /v1/historical-candles` request's `expected_market_data_hash` does not match the recomputed canonical hash over the requested range
- **THEN** the endpoint returns `409 coverage_stale` and no candles

### Requirement: Runtime isolation

The existing `GET /v1/candles` endpoint SHALL retain its current readiness gate.

#### Scenario: GET /v1/candles keeps requiring ready state after this change

- **WHEN** `POST /v1/historical-candles` is added to the service
- **THEN** `GET /v1/candles` continues to require the requested stream's status to be `ready` exactly as before this change

### Requirement: No hidden mutation

Historical read and continuity audit SHALL NOT invoke repair, backfill, upstream REST, or lifecycle transitions.

#### Scenario: Continuity audit is read-only and ignores global readiness without mutating state

- **WHEN** a continuity audit is requested for an explicit range on a stream regardless of its current lifecycle state
- **THEN** the audit reports continuity/gaps for that range
- **AND** it does not invoke repair, backfill, upstream REST calls, or lifecycle transitions

#### Scenario: Continuity audit over a gapped range reports the exact gap without side effects

- **WHEN** a continuity audit is requested over a range containing a gap
- **THEN** the audit reports the exact gap
- **AND** no repair, backfill, or upstream REST call is triggered as a side effect

#### Scenario: Historical read over an incomplete or gapped range fails complete-grid validation without side effects

- **WHEN** a historical read is requested over a range that is incomplete or contains a gap
- **THEN** the read fails complete-grid validation instead of returning candles
- **AND** no repair, backfill, or upstream REST call is triggered as a side effect

