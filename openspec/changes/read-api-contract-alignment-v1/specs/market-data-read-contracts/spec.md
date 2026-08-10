## MODIFIED Requirements

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
