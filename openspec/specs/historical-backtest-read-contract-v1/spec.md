# historical-backtest-read-contract-v1 Specification

## Purpose
TBD - created by archiving change historical-backtest-read-contract-v1. Update Purpose after archive.
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

#### Scenario: Matching hash returns candles carrying the same hash

- **WHEN** a `POST /v1/historical-candles` request's `expected_market_data_hash` matches the recomputed canonical hash over the exact requested range
- **THEN** the response returns the requested candles together with that matching `market_data_hash`

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

#### Scenario: Historical read of a gapped range reports the gap without repairing it

- **WHEN** a continuity audit or historical read is requested over a range containing a gap
- **THEN** the exact gap is reported
- **AND** no repair, backfill, or upstream REST call is triggered as a side effect

