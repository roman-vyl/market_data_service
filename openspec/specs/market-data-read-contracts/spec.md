# Market Data Read Contracts Specification

## Purpose
Defines the public read-only HTTP contracts for canonical candle consumption, history planning/provenance, and hash-bound historical reads.

## Requirements

### Requirement: Ready-only canonical candle range read
`GET /v1/candles` SHALL require exactly `ticker`, `timeframe`, `from_ms`, and `to_ms`. The ticker/timeframe SHALL resolve to an enabled configured canonical stream, and the stream SHALL currently be ready before candles are served.

#### Scenario: Non-ready configured stream is rejected
- **WHEN** a valid configured stream is not currently ready
- **THEN** the endpoint returns HTTP `409` with error code `stream_not_ready` and no candle range.

### Requirement: Candle ranges are aligned complete half-open grids
All candle-range requests SHALL use non-negative aligned half-open `[from_ms,to_ms)` boundaries with `from_ms < to_ms`. `GET /v1/candles` SHALL additionally require the range to lie inside the stream's proven available window and SHALL NOT clamp or partially satisfy it.

A successful candle response SHALL contain the complete ordered timeframe grid for the requested range.

#### Scenario: Invalid or unavailable range is rejected
- **WHEN** a range is malformed, off-grid, outside the proven available window, or resolves to an incomplete/gapped grid
- **THEN** MDS rejects it rather than returning a partial successful range.

### Requirement: Candle responses preserve exact data and provenance
A successful candle-range response SHALL include `ticker`, `timeframe`, `from_ms`, `to_ms`, `market_data_hash`, and ordered candles. Candle OHLCV SHALL be JSON decimal strings. `market_data_hash` SHALL be the canonical lowercase 64-hex SHA-256 identity of the exact stream/range/ordered OHLCV set.

#### Scenario: Same exact range has stable provenance identity
- **WHEN** the canonical stream/range and ordered candle OHLCV set are unchanged
- **THEN** MDS computes the same `market_data_hash`.

### Requirement: Public read errors use one stable envelope
All public market-data read errors SHALL use a JSON object containing `error` and `detail`.

Configured-stream lookup failures SHALL use HTTP `404` with error code `configured_stream_not_found`. Validation, malformed-body, invalid-range, alignment, and bounds failures SHALL use HTTP `422` with a typed error code. Runtime readiness and stale-provenance conflicts SHALL use HTTP `409`. Broken canonical-grid invariants and unexpected server failures SHALL use HTTP `500`.

#### Scenario: Unaligned range maps to 422
- **WHEN** either range boundary is not aligned to the requested timeframe
- **THEN** MDS returns HTTP `422` with error code `range_not_aligned` in the common `{error, detail}` envelope.

### Requirement: Stream bounds are read-only and readiness-independent
`GET /v1/streams/{ticker}/{timeframe}/bounds` SHALL expose current committed minimum/maximum open times and durable lifecycle state for an enabled configured stream without requiring runtime readiness or mutating lifecycle/history.

#### Scenario: Degraded stream bounds remain inspectable
- **WHEN** a configured stream is degraded but has committed candles
- **THEN** its bounds endpoint returns those persisted committed bounds and current state.

### Requirement: Explicit continuity audit has one canonical request shape
`POST /v1/streams/{ticker}/{timeframe}/continuity-audits` SHALL accept exactly `from_ms` and `to_ms` in its JSON body. Alternate historical field names SHALL NOT define a second public request contract.

The endpoint SHALL audit that explicit aligned range from canonical storage without invoking repair, backfill, upstream REST, or lifecycle transitions.

#### Scenario: Canonical audit request is accepted
- **WHEN** a client submits exactly integer `from_ms` and `to_ms` for a configured stream
- **THEN** MDS audits that half-open range without mutation or upstream fetching.

### Requirement: Continuity audit returns gaps and provenance
A continuity-audit response SHALL include `contract_version`, `ticker`, `timeframe`, checked range, candle count, `is_continuous`, exact `gaps`, durable `state`, and `market_data_hash`.

For a continuous range, `market_data_hash` SHALL be the canonical lowercase 64-hex hash of the exact audited candle set. For a gapped range, `market_data_hash` SHALL be `null`.

#### Scenario: Audit finds a gap without repairing it
- **WHEN** the requested canonical range contains missing timeframe-grid candles
- **THEN** the audit returns `is_continuous=false`, exact gap ranges, and `market_data_hash=null`
- **AND** it performs no repair side effect.

### Requirement: Hash-bound historical candle read bypasses readiness only
`POST /v1/historical-candles` SHALL accept exactly `ticker`, `timeframe`, `from_ms`, `to_ms`, and `expected_market_data_hash`. It SHALL NOT require current runtime readiness, but SHALL require a configured stream, valid aligned complete grid, and an exact hash match.

`expected_market_data_hash` SHALL be a canonical lowercase 64-hex SHA-256 value; malformed hashes SHALL be rejected as HTTP `422 invalid_request` rather than treated as a stale-but-valid provenance value.

#### Scenario: Degraded but unchanged audited range can be read
- **WHEN** a configured degraded stream has a complete requested range whose recomputed hash matches `expected_market_data_hash`
- **THEN** MDS returns the candle range even though runtime readiness is false.

### Requirement: Stale provenance fails closed
If the recomputed historical range hash differs from a syntactically valid `expected_market_data_hash`, MDS SHALL return HTTP `409` with error code `coverage_stale` and SHALL NOT return the stale-planned candle range.

#### Scenario: Storage changes after audit
- **WHEN** canonical candle content changes between planning audit and historical read
- **THEN** the historical read returns `coverage_stale`.

### Requirement: Historical/read planning paths are side-effect-free
Bounds reads, continuity audits, ready consumer reads, and hash-bound historical reads SHALL NOT trigger REST fetching, backfill, gap repair, or lifecycle transitions as a side effect of serving the read.

#### Scenario: Read detects an incomplete range
- **WHEN** a read path detects incomplete canonical data
- **THEN** it reports or rejects according to that read contract without repairing the range during the request.

### Requirement: Maintained OpenAPI covers every public read route
`/openapi.json` SHALL document `GET /v1/candles`, `GET /v1/streams/{ticker}/{timeframe}/bounds`, `POST /v1/streams/{ticker}/{timeframe}/continuity-audits`, and `POST /v1/historical-candles` using the canonical request, response, hash, and error contracts above.

#### Scenario: OpenAPI and runtime router expose the same read surface
- **WHEN** the maintained OpenAPI document is inspected
- **THEN** all four public read routes implemented by the runtime router are present with their canonical schemas.
