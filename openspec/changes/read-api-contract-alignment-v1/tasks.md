## 1. Shared error envelope and error-code pins

- [ ] 1.1 Introduce one shared exception-to-HTTP mapping used by
      `consumer_read`, `historical_read`, and `history_planning` handlers,
      producing `{"error": <code>, "detail": <message>}` for every error
      response, including router-level `404 not_found`.
- [ ] 1.2 Remove `HistoryPlanningHttpHandler._map_exception`'s
      `{error, message}`/bare `{error}` shapes in favor of the shared
      mapping.
- [ ] 1.3 Rename `UnknownStreamError`'s HTTP code from `stream_not_found`
      to `configured_stream_not_found` on the bounds and continuity-audits
      endpoints.
- [ ] 1.4 Rename `InvalidRange.code` (`application/consumer_read/errors.py`)
      from `invalid_range` to `invalid_request`; keep its HTTP status at
      `422`. Verify `RangeNotAligned`/`RangeOutOfBounds` keep their
      existing `422` codes unchanged.
- [ ] 1.5 Update `tests/test_consumer_read_http.py`,
      `tests/test_research_history_integration_http.py`,
      `tests/test_consumer_read_application.py`,
      `tests/test_audit_continuity.py`, `tests/test_smoke_audit_continuity.py`
      for the new envelope shape and error-code renames.

## 2. `expected_market_data_hash` format validation

- [ ] 2.1 Add a format check for `expected_market_data_hash` (a
      64-character lowercase hexadecimal string, matching
      `canonical_market_data_hash`'s output shape) in
      `HistoricalReadHttpHandler`/`GetHistoricalCandleRange`, returning
      `422 invalid_request` before the storage read and stale-coverage
      comparison run.
- [ ] 2.2 Add tests asserting a malformed-shape hash returns `422
      invalid_request` and a well-formed-but-mismatched hash still returns
      `409 coverage_stale`, distinguishing the two paths that are
      currently indistinguishable.

## 3. Continuity-audit canonical request contract

- [ ] 3.1 Remove the `start_time_ms`/`end_time_ms` request-body alias from
      `HistoryPlanningHttpHandler._parse_audit_payload`; accept only
      `from_ms`/`to_ms`, returning `422 invalid_request` for any other
      shape (including a mix of both).
- [ ] 3.2 Update the three `start_time_ms`/`end_time_ms` scenarios in
      `tests/test_research_history_integration_http.py` to use
      `from_ms`/`to_ms`, and add a test asserting the legacy shape now
      returns `422 invalid_request`.
- [ ] 3.3 Confirm no other internal caller (tests, docs, scripts) still
      relies on the legacy body shape.

## 4. Continuity-audit `market_data_hash` contract

- [ ] 4.1 Add a test asserting `market_data_hash` is `null` in the JSON
      response when `is_continuous` is `False` (currently unasserted).
- [ ] 4.2 Add a test asserting `market_data_hash` is a well-formed hex
      digest string when `is_continuous` is `True` and matches
      independently-computed `canonical_market_data_hash`.

## 5. OpenAPI parity

- [ ] 5.1 Add `POST /v1/historical-candles` to `openapi_document()`:
      request schema, 200 response schema (shared with `/v1/candles`), and
      error responses for `422 invalid_request`, `404
      configured_stream_not_found`, `422 range_not_aligned`, `409
      coverage_stale`, `500 continuity_invariant_broken`, `500
      internal_error`.
- [ ] 5.2 Correct the continuity-audits request schema in
      `openapi_document()` to declare only `from_ms`/`to_ms` (drop
      `start_time_ms`/`end_time_ms`).
- [ ] 5.3 Add `market_data_hash` (nullable string) to the continuity-audit
      response schema in `openapi_document()`.
- [ ] 5.4 Add any missing status codes to each documented path per the
      pinned mapping (e.g. `500` on continuity-audits, `422` on bounds for
      a malformed ticker/timeframe path segment).
- [ ] 5.5 Add or update `test_consumer_read_http.py::test_openapi_document_is_served`
      (or a new test) to assert `/v1/historical-candles` is present in the
      served OpenAPI document, and that the continuity-audits request
      schema declares only `from_ms`/`to_ms`.

## 6. Documentation follow-through

- [ ] 6.1 Update any README/docs sections describing the continuity-audit
      request shape, the `invalid_range`/`stream_not_found` codes, or the
      historical-candles OpenAPI coverage to match the corrected contract.
