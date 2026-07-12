# Tasks: Market Data Service v1

## Phase 0 — Architecture baseline

- [x] Create standalone repository structure.
- [x] Add master plan.
- [x] Add source-reuse audit.
- [x] Add OpenSpec proposal.
- [x] Add OpenSpec design.
- [x] Add normative service specification.
- [x] Add phased task list.
- [x] Add mandatory agent architecture rules.
- [x] Add operational startup and recovery scenarios.
- [x] Add Python package baseline.
- [x] Add Docker baseline.
- [x] Add explicit layered package skeleton.
- [x] Add architecture boundary baseline tests.
- [x] Initialize Git repository.
- [x] Review actual old BBB Data Engine files and complete initial file-by-file reuse decisions.
- [x] Apply audited half-open window, timeframe registry, grid, gap, and bounded fetch-window semantics to the domain skeleton.
- [x] Add observed-versus-canonical candle contract boundary.
- [x] Add named application use-case and infrastructure port skeletons from the audit.
- [x] Add parity-oriented domain semantic tests.
- [x] Confirm historical lower-bound policy: launchTime seed plus observed earliest available candle.
- [x] Finalize InstrumentKey, InstrumentMetadata, InstrumentCoverage, and StreamKey semantics.
- [x] Add identity stability, timeframe validation, and multi-instrument isolation tests.
- [x] Resolve Phase 1 numeric precision representation: domain Decimal, canonical decimal TEXT in SQLite/API.
- [x] Approve readiness-first consumer recovery with consumer-owned cursors and no v1 event log.
- [ ] Decide native-versus-derived higher timeframe policy using parity evidence.
- [x] Approve minimal SQLite schema v1 and storage responsibilities.
- [x] Resolve concrete SQLite driver/schema implementation using Python sqlite3 and schema v1 validation.
- [x] Approve versioned multi-instrument configuration schema and identity/metadata/coverage split.
- [x] Add complete pre-implementation acceptance test matrix with stable scenario IDs.
- [x] Add ADRs for accepted architectural decisions.
- [x] Approve Phase 0 before production implementation.


### Architecture decision records

- [x] ADR-001 standalone service repository.
- [x] ADR-002 SQLite single-owner storage.
- [x] ADR-003 full available 1m history.
- [x] ADR-004 canonical ticker mapping.
- [x] ADR-005 one canonical ingestion path.
- [x] ADR-006 exact Decimal persistence.
- [x] ADR-007 minimal schema v1.
- [x] ADR-008 readiness-first consumers.
- [x] ADR-009 per-stream state machine.
- [x] ADR-010 sequential bounded backfill.
- [x] ADR-011 layered architecture.

## Phase 1 — Domain and atomic storage

Implementation status note: the timeframe registry/grid model, canonical candle model, SQLite schema, UoW, and canonical ingestion path are implemented and acceptance-tested.


- [x] Add canonical instrument identity contract.
- [x] Add canonical market stream identity contract with registry validation.
- [x] Add canonical timeframe model with mandatory `1m` support and grid helpers.
- [x] Add observed and canonical candle models.
- [x] Add typed candle validation.
- [x] Add duplicate/correction classification.
- [x] Implement schema v1 creation/version validation from approved DDL.
- [x] Add approved candles table DDL.
- [x] Add approved stream_state table DDL.
- [x] Add atomic unit-of-work port.
- [x] Add SQLite unit-of-work adapter.
- [x] Add ingest-observed-candle use case.
- [x] Add idempotency tests.
- [x] Add correction tests.
- [x] Add rollback tests.
- [x] Add restart persistence tests.

## Phase 2 — REST and repair

Implementation status note: single-stream bounded REST backfill, historical lower-bound discovery, shared full-bootstrap window budgeting, continuity audit, production gap repair, post-repair audit, and real Bybit smoke coverage are implemented.


- [x] Add market-data source port.
- [x] Add Bybit REST adapter.
- [x] Add timeframe-to-Bybit interval mapping.
- [x] Add bounded fetch-window iteration.
- [x] Add response normalization.
- [x] Add resumable full-minute-history backfill use case.
- [x] Approve sequential bounded REST backfill contract.
- [x] Add pure one-stream/all-stream backfill planning contracts.
- [x] Add finite administrative bounded `backfill --ticker` entrypoint.
- [x] Add gap detection.
- [x] Add production bounded gap repair use case.
- [x] Add post-repair audit.
- [x] Document production repair result, incomplete, and unexpected-row diagnostics contract.
- [x] Add fake source integration tests.
- [x] Add Bybit demo/public smoke commands.
- [x] Enforce shared full-bootstrap `max_windows` budget across lower-bound discovery and backfill.

## Deferred changes

The following capabilities are intentionally outside this base change and are specified independently:

- `validated-market-config-and-multi-stream-backfill-v1` — implemented validated market configuration, BTC/ETH metadata verification, shared source-failure classification, and deterministic bounded `backfill --all`;
- `runtime-startup-orchestration-v1` — process startup, configured-stream orchestration, health/readiness, shutdown, logging, and Docker runtime;
- `websocket-realtime-recovery-v1` — confirmed-close realtime ingestion, reconnect, stale detection, and REST recovery;
- `consumer-read-api-v1` — deterministic candle reads, OpenAPI, readiness gating, and consumer catch-up;
- `hardening-operations-v1` — fault injection, metrics, long-running validation, runbooks, and database maintenance policy.

These delegated changes are not tasks of `market-data-service-v1` and do not block archiving this base change once its remaining in-scope decisions are closed.
