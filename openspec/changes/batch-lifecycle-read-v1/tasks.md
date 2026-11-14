## 1. Batch lifecycle snapshot read

- [x] 1.1 Extend the stream-state read port and runtime lifecycle recorder with an ordered multi-stream snapshot operation while retaining the existing point read.
- [x] 1.2 Implement the SQLite batch reader with one read-only connection, one read transaction and one joined parameterized SELECT that decodes requested snapshots in order and preserves missing-stream errors.
- [x] 1.3 Add adapter tests for ordering, stream isolation, missing rows, connection/transaction cleanup, one-connection/one-SELECT counting, WAL visibility of committed versus uncommitted state, and a fresh subsequent read.

## 2. Full-sweep integration

- [x] 2.1 Change complete runtime lifecycle synchronization to capture realtime facts before storage I/O, obtain one preliminary batch and process those captured facts in configured order; keep explicitly scoped one-stream synchronization on the point-read path.
- [x] 2.2 Adapt lifecycle synchronization to consume a supplied preliminary snapshot without changing READY/DEGRADED/FAILED decisions, status updates or the mandatory current-state read inside transition write transactions.
- [x] 2.3 Add a concurrency integration test in which recovery commits newer progress after the batch read and before a lifecycle transition, proving the transition validates current state and preserves that progress.
- [x] 2.4 Test the no-write DEGRADED/AUDITING interleaving without re-reading. Force stale public READY and verify that current SQLite range admission and supervisor-based OI publication still reject; normal recovery-pending facts must project ready=false even with an old durable READY.

## 3. Semantic regression and verification

- [x] 3.1 Update lifecycle sweep tests to assert one batch read per complete pass, unchanged stream processing order, and a point read for a scoped refresh.
- [x] 3.2 Run the existing READY/DEGRADED/FAILED cases and close-plus-grace boundary matrix at `-1 ms`, `0`, and `+1 ms` through the batch path and assert existing predicates. Add a test that advances the clock across close plus grace during batch I/O: this pass uses pre-read facts and the next sweep uses the new time.
- [x] 3.3 Run focused SQLite and lifecycle tests, `make verify`, and `openspec validate batch-lifecycle-read-v1 --strict`; confirm no REST recovery, WebSocket, open-interest ingestion, public API or pause-diagnostics code changed.

## Apply verification (2026-11-14)

- Focused batch/SQLite/lifecycle/consumer regressions: 34 passed.
- `make verify`: Ruff and strict mypy passed (162 source files); final full test run: 439 passed, one existing HTTP notifier test failed with `ConnectionResetError` (`test_non_json_success_body_raises_delivery_error`). Repeated runs showed the same existing HTTP test instability; no HTTP code or test changes were included. The six HTTP notifier tests also passed together in an isolated baseline run.
- `openspec validate batch-lifecycle-read-v1 --strict` and `git diff --check` passed.
- Full preliminary sweeps use one read-only connection/transaction/SELECT; scoped refreshes and transactional state rereads remain unchanged. The existing `snapshot` diagnostic count now counts one batch read operation per full sweep, not one per decoded stream row.
- No production, configuration, recovery, ingestion, publication, merge or deployment actions.

## Review follow-up: concurrent lifecycle state change (2026-11-14)

- Added a deterministic characterization test: batch captures CONNECTING with ready facts, then the real recovery recorder commits DEGRADED before lifecycle processing resumes.
- Confirmed: mark_ready raises InvalidStreamTransition (`degraded -> ready`) through the full synchronization call; its transaction does not overwrite the committed recovery state. The stale worker has no handler for this exception, so the failure can propagate to its TaskGroup. No conflict-handling behavior has been changed.
- Merge remains pending a separate decision on safe stale-transition handling; the new test documents the defect, rather than asserting it is fixed.
- Focused regressions: 35 passed. Follow-up make verify: Ruff and mypy passed; 439 tests passed, with the two previously known HTTP notifier ConnectionResetError failures.
