## Context

See `proposal.md` for motivation. A complete `RuntimeRealtimeCoordinator._sync_lifecycle()` pass currently iterates realtime facts in configured order. For each fact, lifecycle synchronization asks `RuntimeLifecycleRecorder` for one preliminary durable snapshot; the SQLite reader opens a read-only connection, resolves the stream id through the catalog, reads `stream_state`, and closes the connection. A transition then deliberately opens a separate immediate write transaction and re-reads the state before validation and persistence.

SQLite runs in WAL mode, so a read transaction can observe a consistent committed snapshot while recovery or ingestion holds an uncommitted writer transaction. The optimization must retain that property and must not turn a preliminary read snapshot into write authority.

## Goals / Non-Goals

**Goals:**

- Collapse the preliminary reads of one complete lifecycle pass into one read-only connection, one read transaction and one SQL statement.
- Keep configured processing order, lifecycle decisions, status publication and exact time-boundary behavior unchanged.
- Preserve the fresh read inside every conditional lifecycle write transaction.

**Non-Goals:**

- Changing lifecycle states, legal transitions, readiness rules or recovery scheduling.
- Batching or coalescing lifecycle writes.
- Changing scoped one-stream synchronization.
- Changing REST recovery, WebSocket ingestion/supervision, open-interest ingestion/publication, public APIs or loop-pause diagnostics.

## Decisions

### 1. Extend the read port with an ordered batch snapshot operation

The stream-state read port and runtime lifecycle recorder will expose a batch operation for a sequence of `StreamKey` values. Its result will retain the request order and contain the existing `StreamStateSnapshot` domain objects. A missing requested stream will retain the existing `KeyError` contract; the reader will not create catalog or state rows.

The existing single-stream operation remains for explicitly scoped synchronization and other call sites. Replacing every point read is unnecessary and would expand the change.

Alternative considered: call the existing point operation in a loop behind a batch-looking method. Rejected because it preserves the connection/query amplification and does not meet the optimization contract.

### 2. Use one joined batch SELECT in one read transaction

The SQLite adapter will open one read-only connection, begin one read transaction and execute one SELECT that joins requested ticker/timeframe identities to `instruments`, `streams` and `stream_state`. The query will carry request ordinals so results can be reconstructed in caller order and missing identities can be detected deterministically. Snapshot decoding remains the same as for a point read.

The connection and transaction are closed on success and on every error. No cache survives the call, so a later call sees commits completed after the earlier snapshot.

Alternative considered: query every registered state and filter in Python. Rejected because the port requests an explicit stream set and the database can select it directly without loading unrelated catalog rows.

### 3. Batch only the preliminary read phase of a complete sweep

For a complete sweep, the coordinator obtains all preliminary snapshots once, then iterates the existing realtime facts in the same configured order and applies the existing synchronization rules to each fact/snapshot pair. A one-stream synchronization continues to read only that stream.

“One connection and one SELECT” applies to the preliminary read phase. If a lifecycle decision requires a durable mutation, its existing write transaction and mandatory state re-read still occur. Those conditional writes cannot be included in the read batch without violating the concurrency invariant.

Alternative considered: precompute decisions from the batch and write them in one transaction. Rejected because it would serialize unrelated streams, enlarge the writer critical section and risk overwriting concurrent recovery/ingestion progress.

### 4. A batch snapshot is never persisted directly

Lifecycle decision code may use the preliminary batch snapshot to detect a no-op and to select the same target as today. When a transition is needed, the recorder re-reads current durable state inside its write transaction, runs the existing domain transition validation against that state, preserves its current progress fields and saves only the validated result.

This also defines the concurrency test: recovery advances a progress field after the batch read but before the transition write; the final row must contain both the legal lifecycle transition and the newer recovery progress.

### 5. Verification counts the intended boundary and preserves semantic matrices

Adapter tests will instrument read-only connection creation and SQL execution to prove one connection and one batch SELECT for a multi-stream batch, including cleanup and missing-stream cases. Runtime tests will prove one batch request per complete sweep, unchanged stream order, and no batch request for a scoped stream refresh.

Existing READY/DEGRADED/FAILED cases and the close-plus-grace matrix at `boundary - 1 ms`, `boundary`, and `boundary + 1 ms` will run through the batch path. A concurrency integration test will interleave the preliminary batch read with a committed recovery update before the lifecycle write.

## Risks / Trade-offs

- **[Risk] Dynamic requested-stream SQL is assembled incorrectly or exceeds SQLite parameter limits.** → Keep identifiers parameterized, generate only the fixed `VALUES` shape, and cover a representative multi-stream set. The configured v1 stream count is bounded far below SQLite's supported parameter count.
- **[Risk] Pairing snapshots by result position could associate a state with the wrong stream.** → Carry an explicit request ordinal and validate the decoded `StreamKey` before returning ordered snapshots.
- **[Risk] A stale batch snapshot overwrites newer recovery facts.** → Never save it; retain the current read inside the write transaction and test the interleaving.
- **[Trade-off] A pass that performs transitions still opens their existing write connections.** → This is required for correctness; the optimization removes repeated preliminary read connections and SELECTs, not conditional mutation work.

## Migration Plan

No database or API migration is required. Deploy the read-port, adapter and runtime call-site change together. Rollback is a code rollback to per-stream preliminary reads; persisted rows and schema are unchanged.
