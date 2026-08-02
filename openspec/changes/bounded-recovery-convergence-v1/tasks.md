# Tasks: Bounded Recovery Convergence v1

## Slice 1 — Contract and durable lower-bound progress

- [ ] Add incident regression tests showing lower-bound discovery repeats its launch-time prefix
  when the first candle lies beyond one pass budget.
- [ ] Add `lower_bound_discovery_next_open_time_ms` to the domain stream-state snapshot with
  non-negative, timeframe-aligned, and resolved/unresolved invariant tests.
- [ ] Add an additive SQLite schema migration that preserves existing canonical data and advances
  the schema version.
- [ ] Update fresh-database DDL, repositories, unit-of-work mappings, and schema validation for the
  new field.
- [ ] Resume lower-bound discovery from the durable probe cursor.
- [ ] Persist every successfully classified empty probe and do not advance on source or storage
  failure.
- [ ] Persist the resolved earliest candle and clear the probe cursor atomically.
- [ ] Add multi-timeframe and multi-symbol isolation tests for independent discovery cursors.
- [ ] Add fresh-workflow restart tests proving discovery resumes beyond the previous pass.
- [ ] Compare the implementation with the old BBB first-available-candle scan and document any
  intentional semantic difference.

## Slice 2 — Realtime admission and recovery convergence

- [ ] Add an incident regression test where late admission retains a stale supervisor cursor and
  emits an obsolete sequence-discontinuity hint.
- [ ] Add a forward-only supervisor progress synchronization operation.
- [ ] Read durable stream progress and synchronize the supervisor immediately before opening the
  per-stream admission gate.
- [ ] Keep a stream unadmitted and unready when the durable handoff anchor is missing.
- [ ] Add an incident regression test where a recovery hint spans more windows than one bounded
  realtime pass and the same prefix is otherwise retried.
- [ ] Freeze the aligned recovery window across non-terminal attempts.
- [ ] Route realtime recovery through full-window `RepairStreamGaps` preflight, bounded repair, and
  post-audit instead of backfilling an unaudited old prefix.
- [ ] Preserve the original suspected start through final post-audit so an internal gap cannot be
  hidden by cursor advancement.
- [ ] Start another finite tail cycle when the latest-closed boundary advances before readiness.
- [ ] Prove restart reconstructs unfinished realtime gaps through normal historical preflight
  without a persisted recovery queue.

## Slice 3 — Progress-aware scheduling and diagnostics

- [ ] Define explicit lower-bound and repair progress markers at the lowest useful layer.
- [ ] Requeue progressing `INCOMPLETE` results fairly without treating them as failures.
- [ ] Detect unchanged `INCOMPLETE` results and apply capped exponential per-stream backoff.
- [ ] Preserve existing recoverable-failure backoff and fatal per-stream isolation.
- [ ] Add `MDS_REALTIME_RECOVERY_WINDOWS_PER_STREAM` and stop coupling realtime load to the startup
  discovery budget.
- [ ] Log fixed interval, before/after progress, next cursor, window counts, ingestion counts,
  result/error, delay, and no-progress attempt count.
- [ ] Suppress repeated identical `repair_incomplete_gap` quarantine rows within one unchanged
  recovery cycle.
- [ ] Add tests proving one stream's no-progress backoff does not block another due stream.
- [ ] Add tests proving no tight REST loop occurs when Bybit returns no new usable data.

## Slice 4 — Acceptance and documentation

- [ ] Add deterministic regressions for both streams described by the 2026-08-02 readiness
  incident using small fake windows and budgets.
- [ ] Prove lower-bound discovery converges across multiple passes and a process restart.
- [ ] Prove old-hint realtime recovery converges across multiple passes and repairs an internal
  gap before readiness.
- [ ] Prove late historical admission cannot generate a stale-cursor recovery interval.
- [ ] Prove a moving historical-target-to-live-admission tail is audited and repaired.
- [ ] Prove repeated no-progress attempts back off and emit bounded actionable diagnostics.
- [ ] Add a fake REST + fake WebSocket + temporary SQLite multi-stream convergence test.
- [ ] Run a Docker empty-volume autonomous convergence smoke.
- [ ] Run a Docker restart smoke without administrative backfill.
- [ ] Publish the new normative database-schema document, update references that currently point
  to `docs/database-schema-v1.md`, and update `docs/stream-state-machine.md`,
  `docs/operational-scenarios.md`, runtime recovery documentation, and the acceptance matrix.
- [ ] Run `make verify` and architecture dependency tests.
