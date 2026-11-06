## Why

`RuntimeLifecycleRecorder.snapshot()` currently opens `SqliteUnitOfWork`, whose `BEGIN IMMEDIATE` reserves SQLite's writer slot even though the operation only reads state. A concurrent writer can therefore make a synchronous runtime snapshot wait unnecessarily; the blocking mechanism was reproduced using the deployed package.

## What Changes

- Add a narrow read-only stream-state port and SQLite implementation for an existing initialized database.
- Route `RuntimeLifecycleRecorder.snapshot()` through that reader, preserving synchronous calls, returned domain values, and missing-stream errors.
- Keep every mutation on the existing `SqliteUnitOfWork` with `BEGIN IMMEDIATE`, including re-reading current state inside lifecycle transition transactions.
- Prove that a snapshot completes while another connection holds an uncommitted write transaction, returns only committed state, and releases its connection.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `canonical-storage`: Read-only lifecycle snapshots must not reserve the writer slot or mutate storage; mutation transaction semantics remain unchanged.

## Impact

- Expected implementation areas: a focused port under `ports/`, a SQLite state reader, `runtime/lifecycle.py`, runtime composition, and relevant test fixtures.
- No schema migration, new database, dependency, HTTP contract, or configuration setting.
- No changes to `to_thread`, WS-ingestion, lifecycle coordination, `operation_gate`, recovery scheduling, publication, SQLite journal settings, or busy-timeout policy.
- Other read-only callers using the write UnitOfWork, including OI progress, are outside this step.
- Synchronous writes and disk I/O remain possible event-loop blockers; this change does not promise to eliminate event-loop lag or WebSocket timeouts.
- This proposal is based on `40cca637…` (`origin/main` when the review branch was created), matching the deployed image examined during investigation. Existing OI functionality is baseline context, not part of this change.
