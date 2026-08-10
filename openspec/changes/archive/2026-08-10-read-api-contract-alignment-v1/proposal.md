## Why

MDS exposes four public read/planning HTTP endpoints — `GET /v1/candles`,
`POST /v1/historical-candles`, `GET /v1/streams/{ticker}/{timeframe}/bounds`,
and `POST /v1/streams/{ticker}/{timeframe}/continuity-audits` — governed by
the `market-data-read-contracts` canonical capability. That capability
already states the target wire contract this change needs (a single
`{error, detail}` envelope; `422` for validation/malformed-body/range/
alignment/bounds failures; `422 invalid_request` for a malformed
`expected_market_data_hash`, distinct from `409 coverage_stale` for a
well-formed but mismatched one; a single `from_ms`/`to_ms` continuity-audit
request shape; `market_data_hash` as a hash string on continuous coverage
and `null` on gaps; and full OpenAPI coverage of all four routes). The
running handlers do not yet implement that contract:

- Three different error envelope shapes are live at once: `{error, detail}`
  (candles, historical-candles), `{error, message}` (bounds,
  continuity-audits), and bare `{error}` (unrouted paths, and the
  `LookupError`/generic-exception branches inside the bounds/
  continuity-audits handler).
- `GET /v1/candles` and `POST /v1/historical-candles` reject malformed
  requests and reversed ranges with error code `invalid_range`
  (`application/consumer_read/errors.py::InvalidRange.code`), not the
  canonical `invalid_request` the capability spec already names.
- The bounds and continuity-audits endpoints reject an unknown
  ticker/timeframe with error code `stream_not_found`
  (`application/audit_continuity.py::UnknownStreamError`), not the
  canonical `configured_stream_not_found` used by the candles and
  historical-candles endpoints for the identical condition.
- `POST /v1/historical-candles` accepts `expected_market_data_hash` with
  only a non-empty-string check
  (`adapters/http/historical_read/handler.py::_require_str`), then compares
  it byte-for-byte against the recomputed hash. A malformed value (wrong
  length, non-hex characters) and a well-formed-but-stale value both
  produce identical `409 coverage_stale` — there is no format check ahead
  of the comparison that would produce the canonical `422 invalid_request`
  outcome the capability spec already requires for a malformed hash.
- The continuity-audits endpoint silently accepts two parallel request
  shapes — `{from_ms, to_ms}` and `{start_time_ms, end_time_ms}`
  (`adapters/http/history_planning/handler.py::_parse_audit_payload`) —
  contradicting the capability spec's "one canonical request shape"
  requirement, which this change strengthens with an explicit rejection
  scenario for the legacy shape.
- `POST /v1/historical-candles` is completely absent from the maintained
  OpenAPI document (`adapters/http/consumer_read/openapi.py`) — no path,
  no request schema, no response, no error codes.
- The continuity-audits path's OpenAPI request schema declares only
  `start_time_ms`/`end_time_ms` with `additionalProperties: false`,
  contradicting both the actual accepted `from_ms`/`to_ms` shape and this
  change's requirement to retire the legacy alias — a strict client
  following the document would be told the canonical shape is invalid.
  The response schema also omits `market_data_hash` entirely, so a client
  has no way to learn the field — or its null-on-gap behavior — exists.

This change strengthens the `market-data-read-contracts` capability spec
with the explicit error-code pins and scenarios needed to remove this
runtime/spec gap, and records the corresponding implementation work in
`tasks.md`. It supersedes an earlier draft of this same proposal that was
built against a stale baseline (the pre-`c9d9ca0` canonical specs, which
still had separate `consumer-read-api-v1` and
`historical-backtest-read-contract-v1` capabilities and a different,
now-superseded `400`/`422` split for malformed requests). That draft is
discarded; this version targets the current `market-data-read-contracts`
capability and preserves its existing canonical mapping — `422` for
malformed/validation/range/alignment/bounds failures, `422 invalid_request`
for a malformed hash, `409 coverage_stale` for a stale-but-valid one —
rather than introducing a different one.

This proposal is design-only. No code, tests, or OpenAPI document changes
are made in this change; `tasks.md` describes the follow-up implementation
work.

## What Changes

- Pin `configured_stream_not_found` (404) as the single error code for an
  unknown/unregistered configured stream across all four endpoints,
  replacing the bounds/continuity-audits endpoint's distinct
  `stream_not_found` code.
- Pin `invalid_request` (422) as the single error code for a malformed
  request — bad JSON, wrong field set/types, non-integer `from_ms`/
  `to_ms`, or `from_ms >= to_ms` — across `GET /v1/candles`,
  `POST /v1/historical-candles`, and
  `POST /v1/streams/{ticker}/{timeframe}/continuity-audits`, replacing the
  candles/historical-candles endpoints' `invalid_range` code.
- Require the `{error, detail}` envelope on every error response from all
  four endpoints, replacing the bounds/continuity-audits endpoint's
  `{error, message}` and bare `{error}` shapes.
- Require `expected_market_data_hash` to be format-validated (a 64-character
  lowercase hexadecimal string, matching the shape
  `canonical_market_data_hash` produces) before the stale-coverage
  comparison runs, so a malformed value returns `422 invalid_request` and
  only a well-formed-but-mismatched value returns `409 coverage_stale`.
- Retire the parallel `start_time_ms`/`end_time_ms` continuity-audit
  request shape; require `422 invalid_request` when it — or a mix of both
  shapes — is submitted.
- Require `POST /v1/historical-candles` to be present in the maintained
  OpenAPI document with its real request schema, response schema, and full
  canonical error status/code set including `409 coverage_stale`.
- Require the continuity-audits OpenAPI request schema to declare only
  `from_ms`/`to_ms`, and its response schema to declare a nullable
  `market_data_hash` property.

## Capabilities

### Modified Capabilities

- `market-data-read-contracts`: pin the exact error-code names shared
  across all four public read/planning endpoints, add an explicit
  malformed-hash-vs-stale-hash scenario, add an explicit
  legacy-continuity-audit-shape rejection scenario, and add explicit
  OpenAPI schema-parity scenarios for the continuity-audits and
  historical-candles routes. No new capability is introduced and no
  removed capability (`consumer-read-api-v1`,
  `historical-backtest-read-contract-v1`) is restored.

## Impact

- Affected runtime code (for the follow-up implementation change, not this
  proposal): `application/consumer_read/errors.py` (`InvalidRange.code`),
  `application/audit_continuity.py` (`UnknownStreamError` HTTP code),
  `adapters/http/consumer_read/exception_mapping.py`,
  `adapters/http/historical_read/handler.py`,
  `application/consumer_read/get_historical_candle_range.py`,
  `adapters/http/history_planning/handler.py` (envelope, `_map_exception`,
  `_parse_audit_payload`), `adapters/http/consumer_read/openapi.py`.
- Affected tests: `tests/test_consumer_read_http.py`,
  `tests/test_research_history_integration_http.py`,
  `tests/test_consumer_read_application.py`,
  `tests/test_audit_continuity.py`,
  `tests/test_smoke_audit_continuity.py` — several currently assert the
  `start_time_ms`/`end_time_ms` continuity-audit shape, the
  `invalid_range`/`stream_not_found` codes, or the `message`-keyed
  envelope that this change retires.
- No change to storage, backfill/recovery/readiness algorithms, or
  `canonical_market_data_hash` computation.
