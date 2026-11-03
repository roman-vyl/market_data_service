## Context

See `proposal.md` for motivation and scope. The review branch is based on `40cca637…`, matching the deployed image examined during investigation. The existing `mds-open-interest-prefetch-current-bar-v1` change describes baseline OI behavior and is outside this proposal. `docs/master-plan.md`, `docs/stream-state-machine.md`, and the canonical storage spec remain authoritative.

`RuntimeLifecycleRecorder.snapshot()` currently opens a write UnitOfWork. Its callers include runtime projection and lifecycle methods' preliminary checks. Actual transitions use `_transition()`, which opens another write transaction and re-reads state before validation and persistence. Replacing the preliminary read must preserve this distinction.

## Goals / Non-Goals

**Goals:** Keep synchronous execution and existing lifecycle behavior while allowing snapshots of committed stream state during an active WAL writer transaction. Retain one canonical storage implementation and explicit dependencies.

**Non-Goals:** Move work between threads, add awaits, change coordination or transition rules, optimize all storage reads, change OI progress access, batch all stream snapshots, cache state, or remove the write lock from mutation workflows. Existing synchronous mutation waits remain possible.

## Decisions

### 1. A narrow state-reader port

Add `StreamStateReader` in a focused `ports/stream_state_reader.py` with one synchronous method, `get_stream_state(stream: StreamKey) -> StreamStateSnapshot`. Add a focused `SqliteStreamStateReader` adapter. Runtime composition supplies the reader alongside the existing write UnitOfWork factory to `RuntimeLifecycleRecorder`.

The reader returns the same immutable domain snapshot and preserves `KeyError` for a missing stream/state. Reuse `SqliteCatalogRepository` and `SqliteStreamStateRepository` for lookup and conversion; do not duplicate state mapping or expose repositories through the port. Update construction sites and test fakes directly, without a legacy constructor fallback.

Alternative rejected: a read/write mode flag on the general UnitOfWork would enlarge an API that intentionally owns mutation transactions and make accidental mode selection possible. A dedicated state reader keeps its interface read-only.

### 2. One read-only connection per call

Open an existing database using a correctly escaped SQLite file URI with `mode=ro`; retain foreign-key enforcement and the current finite busy timeout. Do not change the ordinary write connection factory's behavior. The adapter must not initialize/migrate a database, set journal mode, reserve the writer slot, enable dirty reads, or use `immutable=1` on a live WAL database.

Use an explicit ordinary `BEGIN` read transaction for the catalog/state lookup, then close the connection in `finally` on success or error. A SQLite connection context manager alone does not close the connection. Each call owns its connection; there is no shared connection or cache. This provides a consistent per-call view and permits subsequent calls to see newly committed state.

`mode=ro` enforces database-level read-only access, not just a convention in the port. Normal SQLite WAL/SHM coordination still applies; this is not a guarantee of zero filesystem activity or zero I/O latency.

Alternative rejected: changing every `BEGIN IMMEDIATE` to `BEGIN` could alter mutation locking and read-modify-write behavior. Only the new reader uses a read transaction.

### 3. Preserve mutation transactions and current-state validation

Only `RuntimeLifecycleRecorder.snapshot()` switches to the injected reader. All `mark_*`, bootstrap preparation, and transition call sequences remain synchronous and retain their current decisions. `_transition()` continues to create a write UnitOfWork, read current state, validate through the domain, save, and commit under `BEGIN IMMEDIATE`.

A preliminary snapshot is advisory, not a reservation or authorization to overwrite a row. No saved snapshot may replace the fresh transaction-local read. Concurrent state changes can already occur between the current preliminary read and `_transition()`; this step preserves the existing legal-transition/error behavior rather than adding retries or coordination. Candle/state atomic ingestion, correction/quarantine behavior, and progress monotonicity are unchanged.

### 4. Validate concurrency through synchronization, not a speed threshold

Use a temporary initialized WAL database and independent connections. A writer starts `BEGIN IMMEDIATE`, changes a state field without committing, and holds the transaction. A separate reader calls the real recorder's `snapshot()`. The test must observe reader completion and the previously committed value while the writer is still held. Release the writer only after this assertion; use a bounded safety timeout and cleanup in `finally` to avoid hanging a failed test. Test threads are a harness only, not a production architecture change.

After commit, a fresh call must see the committed change; after rollback, it must see the original value. Also cover stream isolation, missing-stream errors, connection cleanup, and prevention of writes through the reader connection. Preserve existing mutation rollback and restart tests. A lifecycle regression test must demonstrate that a stale preliminary snapshot cannot overwrite newer progress fields because the transition re-reads the current row in its write transaction.

## Risks / Trade-offs

- **Synchronous storage writes still block the event loop** → State this limitation explicitly; async offloading is a separate change requiring review.
- **An older committed snapshot is returned while a writer is active** → This is normal read isolation; keep transaction-local revalidation for mutations and existing readiness rules.
- **A new connection leaks on error and retains a read snapshot** → Explicitly close on every path and verify cleanup.
- **File URI construction mishandles special characters** → Construct escaped absolute file URIs and test a path containing spaces and URI-sensitive characters.
- **The implementation baseline may advance after proposal review** → Recheck construction sites before coding; do not include unrelated feature changes in this proposal's implementation.
- **Two baseline HTTP tests are intermittent** → Keep separate from this change. Prior isolated repeats produced two resets in five malformed-body runs, and none in five non-JSON runs; temporary fixtures that consumed the POST body passed five times each. Record baseline failures honestly if they recur during verification.

## Migration Plan

No schema or data migration is required. Deployment is outside this change. A later release may roll back to the prior code without converting data; the previous writer contention would return.
