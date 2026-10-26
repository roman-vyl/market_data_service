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

Every candle of a successful `GET /v1/candles` or `POST /v1/historical-candles` response SHALL carry `open_interest`: the stored open interest of the same stream at the same `open_time_ms` as a JSON decimal string, or `null` when no open interest is stored for that bar. It SHALL NOT be taken from another bar, filled forward, or replaced by zero. Candles and their open interest SHALL be read in the same read transaction as the stream state. `open_interest` SHALL NOT enter `market_data_hash` or the `coverage_stale` comparison.

#### Scenario: Same exact range has stable provenance identity
- **WHEN** the canonical stream/range and ordered candle OHLCV set are unchanged
- **THEN** MDS computes the same `market_data_hash`, whether or not open interest is stored for those bars.

#### Scenario: Candle carries the open interest of its own bar
- **WHEN** open interest is stored for the stream at the candle's `open_time_ms`
- **THEN** that candle's `open_interest` is the stored value as a decimal string.

#### Scenario: Bar without open interest
- **WHEN** no open interest is stored for the stream at the candle's `open_time_ms` (instrument without open interest, bar before the open interest history, or a stream other than the 5m stream of an instrument with `open_interest = true`)
- **THEN** that candle's `open_interest` is `null`
- **AND** the value of a neighbouring bar is not used.

#### Scenario: Historical read with open interest stored after the audit
- **WHEN** open interest for bars of an audited range is stored after the audit and the OHLCV is unchanged
- **THEN** `POST /v1/historical-candles` with the audited hash succeeds and returns the stored open interest.

### Requirement: Public read errors use one stable envelope

All public market-data read errors — for `GET /v1/candles`,
`POST /v1/historical-candles`, `GET /v1/streams/{ticker}/{timeframe}/bounds`,
and `POST /v1/streams/{ticker}/{timeframe}/continuity-audits` alike — SHALL
use a JSON object containing exactly `error` and `detail`. No endpoint in
this capability SHALL use a different key (such as `message`) for the
human-readable text, and no endpoint SHALL omit `detail` from an error
response.

Configured-stream lookup failures SHALL use HTTP `404` with error code
`configured_stream_not_found`, on every endpoint that resolves a
ticker/timeframe pair — including the bounds and continuity-audits
endpoints, which SHALL NOT use a distinct code (such as `stream_not_found`)
for the same condition.

Validation, malformed-body, invalid-range, alignment, and bounds failures
SHALL use HTTP `422` with a typed error code:

- a malformed request — a non-JSON/unparseable body or query, a wrong or
  incomplete parameter/field set, a non-integer `from_ms`/`to_ms`, or
  `from_ms >= to_ms` — SHALL use `422 invalid_request`, on every endpoint
  that accepts a range, including the continuity-audits endpoint's request
  body;
- a range whose boundaries do not align to the requested timeframe grid
  SHALL use `422 range_not_aligned`;
- a `GET /v1/candles` range outside the stream's proven available window
  SHALL use `422 range_out_of_bounds`.

Runtime readiness and stale-provenance conflicts SHALL use HTTP `409`:
`stream_not_ready` for `GET /v1/candles` against a non-ready stream, and
`coverage_stale` for `POST /v1/historical-candles` against a
recomputed-hash mismatch.

Broken canonical-grid invariants SHALL use HTTP `500
continuity_invariant_broken`. An unexpected server failure not covered by
any typed error code above SHALL use HTTP `500 internal_error`. An
unroutable path SHALL use HTTP `404 not_found`.

#### Scenario: Unaligned range maps to 422

- **WHEN** either range boundary is not aligned to the requested timeframe
- **THEN** MDS returns HTTP `422` with error code `range_not_aligned` in
  the common `{error, detail}` envelope.

#### Scenario: Unknown configured stream uses the same code on every endpoint

- **WHEN** `GET /v1/candles`, `POST /v1/historical-candles`,
  `GET /v1/streams/{ticker}/{timeframe}/bounds`, or
  `POST /v1/streams/{ticker}/{timeframe}/continuity-audits` targets a
  ticker/timeframe pair that is not a configured stream
- **THEN** MDS returns HTTP `404` with error code
  `configured_stream_not_found` in the common `{error, detail}` envelope,
  regardless of which of the four endpoints was called.

#### Scenario: Malformed continuity-audit body maps to 422 invalid_request

- **WHEN** a `POST /v1/streams/{ticker}/{timeframe}/continuity-audits`
  request body is not valid JSON, is not a JSON object, or does not
  contain exactly `from_ms` and `to_ms` as integers
- **THEN** MDS returns HTTP `422` with error code `invalid_request` in the
  common `{error, detail}` envelope, not a bare `{error}` body and not a
  `message`-keyed body.

### Requirement: Stream bounds are read-only and readiness-independent
`GET /v1/streams/{ticker}/{timeframe}/bounds` SHALL expose the current available history bounds and durable lifecycle state for an enabled configured stream without requiring runtime readiness or mutating lifecycle/history.

For a stream without an open interest track (any stream other than the 5m stream of an instrument with `open_interest = true`), the bounds SHALL be the committed minimum/maximum candle open times.

For a stream with an open interest track (the 5m stream of an instrument with `open_interest = true`), the bounds SHALL be the intersection of its two data tracks: `earliest_committed_open_time_ms` SHALL be the later of the earliest committed candle open time and the resolved open interest lower bound, and `latest_committed_open_time_ms` SHALL be the earlier of the latest committed candle open time and the latest committed open interest open time. When either track has no bounds, or the intersection is empty, both fields SHALL be `null`; the bounds of one track SHALL NOT be returned alone.

The bounds SHALL report boundaries only and SHALL NOT be treated as proof that every bar between them is present; continuity remains proven by continuity audits, readiness and ready consumer reads.

#### Scenario: Degraded stream bounds remain inspectable
- **WHEN** a configured stream is degraded but has committed candles (and, for a stream with an open interest track, committed open interest)
- **THEN** its bounds endpoint returns those persisted bounds and current state.

#### Scenario: Unflagged stream bounds come from candles
- **WHEN** a stream without an open interest track is read, including a non-5m stream of an instrument with `open_interest = true`
- **THEN** the bounds are the committed minimum and maximum candle open times, exactly as before.

#### Scenario: Open interest starts later than candles
- **WHEN** a stream with an open interest track has candles from `A` and a resolved open interest lower bound `B > A`
- **THEN** `earliest_committed_open_time_ms` is `B`.

#### Scenario: Candle committed before its open interest
- **WHEN** a stream with an open interest track has its latest candle at `T` and its latest committed open interest at `T - step`
- **THEN** `latest_committed_open_time_ms` is `T - step`.

#### Scenario: One track has no bounds
- **WHEN** a stream with an open interest track has committed candles but no resolved open interest lower bound or no committed open interest
- **THEN** both `earliest_committed_open_time_ms` and `latest_committed_open_time_ms` are `null` and `state` is returned as persisted.

#### Scenario: Bounds read has no side effects
- **WHEN** bounds are read for any configured stream
- **THEN** no readiness is required, no lifecycle transition happens and no REST fetch, backfill or repair is started.

### Requirement: Explicit continuity audit has one canonical request shape

`POST /v1/streams/{ticker}/{timeframe}/continuity-audits` SHALL accept
exactly `from_ms` and `to_ms` in its JSON body. Alternate historical field
names — including `start_time_ms`/`end_time_ms` — SHALL NOT define a
second public request contract, whether alone, mixed with `from_ms`/
`to_ms`, or accepted silently alongside it.

The endpoint SHALL audit that explicit aligned range from canonical
storage without invoking repair, backfill, upstream REST, or lifecycle
transitions.

#### Scenario: Canonical audit request is accepted

- **WHEN** a client submits exactly integer `from_ms` and `to_ms` for a
  configured stream
- **THEN** MDS audits that half-open range without mutation or upstream
  fetching.

#### Scenario: Legacy or mixed field names are rejected, not silently accepted

- **WHEN** a continuity-audit request body uses `start_time_ms`/
  `end_time_ms` instead of `from_ms`/`to_ms`, or mixes fields from both
  shapes
- **THEN** MDS returns HTTP `422` with error code `invalid_request` in the
  common `{error, detail}` envelope
- **AND** it does not audit the requested range under either field-name
  shape.

### Requirement: Continuity audit returns gaps and provenance
A continuity-audit response SHALL include `contract_version`, `ticker`, `timeframe`, checked range, candle count, `is_continuous`, exact `gaps`, durable `state`, and `market_data_hash`.

For a continuous range, `market_data_hash` SHALL be the canonical lowercase 64-hex hash of the exact audited candle set. For a gapped range, `market_data_hash` SHALL be `null`.

#### Scenario: Audit finds a gap without repairing it
- **WHEN** the requested canonical range contains missing timeframe-grid candles
- **THEN** the audit returns `is_continuous=false`, exact gap ranges, and `market_data_hash=null`
- **AND** it performs no repair side effect.

### Requirement: Hash-bound historical candle read bypasses readiness only

`POST /v1/historical-candles` SHALL accept exactly `ticker`, `timeframe`,
`from_ms`, `to_ms`, and `expected_market_data_hash`. It SHALL NOT require
current runtime readiness, but SHALL require a configured stream, valid
aligned complete grid, and an exact hash match.

`expected_market_data_hash` SHALL be a canonical lowercase 64-hex SHA-256
value; malformed hashes SHALL be rejected as HTTP `422 invalid_request`
rather than treated as a stale-but-valid provenance value. This format
check SHALL be performed before any canonical storage read or
stale-provenance comparison, so a malformed value never reaches — and
never produces — the `coverage_stale` outcome.

#### Scenario: Degraded but unchanged audited range can be read

- **WHEN** a configured degraded stream has a complete requested range
  whose recomputed hash matches `expected_market_data_hash`
- **THEN** MDS returns the candle range even though runtime readiness is
  false.

#### Scenario: Malformed hash format is rejected before any storage read

- **WHEN** a `POST /v1/historical-candles` request's
  `expected_market_data_hash` is not a 64-character lowercase hexadecimal
  string
- **THEN** MDS returns HTTP `422` with error code `invalid_request` and no
  candles
- **AND** it performs no canonical storage read or stale-provenance
  comparison for that request.

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

`/openapi.json` SHALL document `GET /v1/candles`,
`GET /v1/streams/{ticker}/{timeframe}/bounds`,
`POST /v1/streams/{ticker}/{timeframe}/continuity-audits`, and
`POST /v1/historical-candles` using the canonical request, response, hash,
and error contracts above.

The continuity-audits request schema SHALL declare only `from_ms` and
`to_ms` as accepted body properties and SHALL NOT declare
`start_time_ms`/`end_time_ms`. The continuity-audit response schema SHALL
declare `market_data_hash` as a nullable string property. Every documented
path's error responses SHALL list the exact status/error-code pairs this
capability defines for that path, including `409 coverage_stale` for
`POST /v1/historical-candles`.

#### Scenario: OpenAPI and runtime router expose the same read surface

- **WHEN** the maintained OpenAPI document is inspected
- **THEN** all four public read routes implemented by the runtime router
  are present with their canonical schemas.

#### Scenario: OpenAPI continuity-audit schemas match the canonical request/response shape

- **WHEN** the maintained OpenAPI document's continuity-audits path is
  inspected
- **THEN** its request body schema declares only `from_ms` and `to_ms`
- **AND** its response schema declares `market_data_hash` as a nullable
  string
- **AND** its documented error responses include `422 invalid_request` and
  `404 configured_stream_not_found`.
