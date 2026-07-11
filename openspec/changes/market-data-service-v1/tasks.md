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
- [ ] Validate BTCUSDT and ETHUSDT perpetual metadata assumptions against Bybit.
- [x] Add complete pre-implementation acceptance test matrix with stable scenario IDs.
- [ ] Add ADRs for accepted architectural decisions.
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

- [x] Add canonical instrument identity contract.
- [x] Add canonical market stream identity contract with registry validation.
- [ ] Add validated `config/markets.toml` loader.
- [ ] Reject duplicate instrument and stream identities.
- [ ] Add canonical timeframe model with mandatory `1m` support and grid helpers.
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

- [x] Add market-data source port.
- [x] Add Bybit REST adapter.
- [x] Add timeframe-to-Bybit interval mapping.
- [ ] Add bounded fetch-window iteration.
- [x] Add response normalization.
- [ ] Add retry classification.
- [ ] Add resumable full-minute-history backfill use case.
- [x] Approve sequential bounded REST backfill contract.
- [x] Add pure one-stream/all-stream backfill planning contracts.
- [ ] Add finite administrative `backfill --ticker/--all` entrypoint.
- [ ] Add gap detection.
- [ ] Add repair use case.
- [ ] Add post-repair audit.
- [x] Add fake source integration tests.
- [ ] Add Bybit demo/public smoke command.

## Phase 3 — Runtime and Docker

- [ ] Add environment settings.
- [ ] Add configured stream loading.
- [ ] Add startup catch-up.
- [x] Add per-stream persisted lifecycle state model and legal transition contract.
- [x] Add strict aggregate readiness projection.
- [x] Add restart lifecycle semantics and invalid-transition tests.
- [ ] Add health endpoint.
- [ ] Add readiness endpoint.
- [ ] Add graceful shutdown.
- [ ] Add structured logging.
- [ ] Add Docker runtime image.
- [ ] Add persistent-volume compose setup.
- [ ] Add restart smoke tests.

## Phase 4 — WebSocket realtime

- [ ] Add Bybit WebSocket adapter.
- [ ] Add multi-symbol subscription lifecycle.
- [ ] Add simple multi-symbol WebSocket subscription lifecycle without a REST worker scheduler.
- [ ] Parse confirmed candle closes.
- [ ] Ignore non-canonical partial updates.
- [ ] Add reconnect with bounded backoff.
- [ ] Add stale-stream detection.
- [ ] Add reconnect repair.
- [ ] Add duplicate suppression tests.
- [ ] Add disconnect/recovery integration tests.

## Phase 5 — Consumer API

- [ ] Add candle range endpoint.
- [ ] Add latest candle endpoint.
- [ ] Add deterministic pagination.
- [ ] Publish OpenAPI schema.
- [ ] Add API contract tests.
- [ ] Add consumer cursor catch-up and readiness-gate contract test.

## Phase 6 — Hardening

- [ ] Add malformed payload tests.
- [ ] Add timeframe-boundary tests.
- [ ] Add clock-skew tests.
- [ ] Add database fault tests.
- [ ] Add long-running smoke.
- [ ] Add metrics.
- [ ] Add operational runbook.
- [ ] Decide database maintenance policy.
