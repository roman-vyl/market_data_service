## 1. Batch lifecycle snapshot read

- [ ] 1.1 Extend the stream-state read port and runtime lifecycle recorder with an ordered multi-stream snapshot operation while retaining the existing point read.
- [ ] 1.2 Implement the SQLite batch reader with one read-only connection, one read transaction and one joined parameterized SELECT that decodes requested snapshots in order and preserves missing-stream errors.
- [ ] 1.3 Add adapter tests for ordering, stream isolation, missing rows, connection/transaction cleanup, one-connection/one-SELECT counting, WAL visibility of committed versus uncommitted state, and a fresh subsequent read.

## 2. Full-sweep integration

- [ ] 2.1 Change complete runtime lifecycle synchronization to capture realtime facts before storage I/O, obtain one preliminary batch and process those captured facts in configured order; keep explicitly scoped one-stream synchronization on the point-read path.
- [ ] 2.2 Adapt lifecycle synchronization to consume a supplied preliminary snapshot without changing READY/DEGRADED/FAILED decisions, status updates or the mandatory current-state read inside transition write transactions.
- [ ] 2.3 Add a concurrency integration test in which recovery commits newer progress after the batch read and before a lifecycle transition, proving the transition validates current state and preserves that progress.
- [ ] 2.4 Test the no-write DEGRADED/AUDITING interleaving without re-reading. Force stale public READY and verify that current SQLite range admission and supervisor-based OI publication still reject; normal recovery-pending facts must project ready=false even with an old durable READY.

## 3. Semantic regression and verification

- [ ] 3.1 Update lifecycle sweep tests to assert one batch read per complete pass, unchanged stream processing order, and a point read for a scoped refresh.
- [ ] 3.2 Run the existing READY/DEGRADED/FAILED cases and close-plus-grace boundary matrix at `-1 ms`, `0`, and `+1 ms` through the batch path and assert existing predicates. Add a test that advances the clock across close plus grace during batch I/O: this pass uses pre-read facts and the next sweep uses the new time.
- [ ] 3.3 Run focused SQLite and lifecycle tests, `make verify`, and `openspec validate batch-lifecycle-read-v1 --strict`; confirm no REST recovery, WebSocket, open-interest ingestion, public API or pause-diagnostics code changed.
