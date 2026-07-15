# Tasks: Research history integration v1

- [x] Add canonical committed-bounds storage operation.
- [x] Add read-only committed-bounds application use case.
- [x] Expose `market_stream_bounds.v1` over HTTP.
- [x] Wrap existing continuity audit with informational lifecycle state.
- [x] Expose `market_continuity_audit.v1` over HTTP.
- [x] Reject malformed, unknown-stream and unaligned requests deterministically.
- [x] Add MDS-owned canonical candle-range hash.
- [x] Add hash to serialization and OpenAPI.
- [x] Add SQLite/HTTP producer contract tests.
- [x] Prove degraded stream bounds/audit remain available without state mutation.
- [x] Prove audit gaps do not trigger repair.
- [x] Run full lint, strict typecheck and tests.
- [ ] Run three-service integration after Strategy Engine consumes the producer hash.
- [ ] Decide explicit historical candle-read admission if degraded-range E2E requires it.
