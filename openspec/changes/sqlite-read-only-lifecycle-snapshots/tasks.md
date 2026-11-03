Implementation is pending review and explicit approval. All tasks below remain unstarted.

## 1. Confirm implementation baseline

- [ ] 1.1 Confirm the target source revision relative to the deployed `40cca637…` image; enumerate recorder construction sites without importing unrelated OI functionality.

## 2. Add read-only state access

- [ ] 2.1 Add the synchronous `StreamStateReader` port and focused SQLite adapter using existing catalog/state mapping, a per-call `mode=ro` connection, ordinary read transaction, current safety pragmas, and explicit connection cleanup.
- [ ] 2.2 Inject the reader through runtime composition and update `RuntimeLifecycleRecorder.snapshot()` and affected test constructors directly. Keep the write UnitOfWork and all transition decisions/call ordering unchanged.

## 3. Verify read and mutation boundaries

- [ ] 3.1 Add a synchronized WAL integration test proving the real recorder snapshot completes with the prior committed value while an independent writer still holds an uncommitted write transaction; always clean up both connections/threads after failure.
- [ ] 3.2 Verify fresh snapshots after commit and rollback, stream isolation, existing missing-stream errors, failure without database creation, connection cleanup on success/error, enforced read-only access, and correctly escaped file paths.
- [ ] 3.3 Add a lifecycle regression test proving transaction-local re-reading preserves progress committed after a preliminary snapshot and retains domain transition validation; retain existing atomic candle/state rollback and restart-idempotency coverage.

## 4. Validate the approved change

- [ ] 4.1 Run focused storage/lifecycle/runtime-composition tests and `make verify`; report any independently reproduced baseline HTTP failures without fixing them in this change.
- [ ] 4.2 Review the final diff for no changes to async execution, WS-ingestion, lifecycle coordination, `operation_gate`, OI progress access, publication, schema, journal settings, or busy-timeout policy; document that synchronous writes can still block the event loop.
