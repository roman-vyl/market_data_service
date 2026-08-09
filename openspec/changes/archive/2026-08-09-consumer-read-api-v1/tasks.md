# Tasks: Consumer Read API v1

## Slice 0 — Mandatory architecture gate

- [x] Audit the existing HTTP server, runtime wiring, storage ports, SQLite candle repository, stream-state repository, and relevant tests file by file.
- [x] Identify the exact existing canonical range-read method and prove whether it can be reused without a second SQL path.
- [x] Identify the exact state/metadata reads that prove `ready` and derive the available window without coupling the query to runtime orchestration.
- [x] Record the final module map and dependency direction before production implementation.
- [x] Record expected changes to `runtime_server.py`, `runtime/wiring.py`, `ports/storage.py`, and `candle_repository.py`; extract focused modules if any would gain a second responsibility.
- [x] Add or plan structural dependency guards proving application, SQLite, and HTTP layers do not import each other in the wrong direction.
- [x] Stop and amend the OpenSpec before implementation if the audit finds cyclic dependencies, duplicated range semantics, or an unavoidable mixed-responsibility module.

## Slice 1 — Read contract and application boundary

- [x] Add canonical request/response and error models for `GET /v1/candles`.
- [x] Add `GetCandleRange` application query without HTTP or SQLite dependencies.
- [x] Add or refine a `CandleReader` port with aligned half-open range semantics.
- [x] Reuse or minimally extend the existing SQLite range-read implementation.
- [x] Prove deterministic ascending order, uniqueness, and half-open boundaries.

## Slice 2 — Admission and range validation

- [x] Resolve only configured canonical ticker/timeframe pairs.
- [x] Reject every stream whose current status is not `ready`.
- [x] Derive the proven available half-open window for a ready stream.
- [x] Reject zero/reversed and non-aligned ranges.
- [x] Reject ranges outside available boundaries without clamping or partial success.
- [x] Add the ready-stream result-grid invariant and refuse partial/gapped success.

## Slice 3 — HTTP adapter and serialization

- [x] Register `GET /v1/candles` in the existing HTTP application.
- [x] Serialize OHLCV as normalized decimal text without float conversion.
- [x] Implement the agreed stable error envelope and HTTP mappings.
- [x] Add generated or maintained OpenAPI/schema documentation.
- [x] Preserve existing `/health` and `/readiness` behavior.

## Slice 4 — Acceptance and performance evidence

- [x] Test a correct complete aligned range.
- [x] Test earliest and exclusive latest available boundaries.
- [x] Test unknown canonical ticker and unsupported/unconfigured timeframe.
- [x] Test every non-ready lifecycle state is denied without candles.
- [x] Test non-aligned, zero/reversed, and out-of-bounds ranges.
- [x] Test Decimal text exactness and no JSON floating-point OHLCV.
- [x] Test multiple streams cannot mix rows.
- [x] Test ready-state continuity invariant failure does not return partial `200`.
- [x] Test a large range in one JSON response without pagination parameters.
- [x] Add a fake BBB-style HTTP consumer integration test.
- [x] Record Workbench-size, 10k, 100k, and large `5m` benchmark measurements.
- [x] Run `make verify` and architecture guards.

## Slice 5 — Documentation and future integration

- [x] Update README with range request, Decimal response, ready-only, and error examples.
- [x] Update master plan to replace the preliminary paginated/limit API sketch.
- [x] Include `docs/integrations/bbb-consumer-api-current-state.docx`.
- [x] Document all required subsequent BBB integration changes without implementing them here.
- [x] Record pagination, chunking, streaming, compact formats, and caching as future evidence-driven options, not v1 blockers.
- [x] Include every new and modified file in the cumulative installable patch.
