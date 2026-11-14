## Why

A full realtime lifecycle synchronization currently opens one read-only SQLite connection and performs separate catalog/state lookups for every configured stream. As the stream set grows, this local polling path creates avoidable connection and query overhead even when no lifecycle transition is needed.

## What Changes

- Add a batch lifecycle-snapshot read for a complete synchronization pass: one read-only SQLite connection, one read transaction and one SELECT returning all requested stream states.
- Preserve the configured stream order and the existing missing-stream failure semantics when the batch result is projected back to the requested streams.
- Use the batch snapshot only for full lifecycle sweeps; a synchronization explicitly scoped to one stream keeps the existing single-stream read.
- Preserve all READY/DEGRADED/FAILED decisions, full-sweep processing order, status updates and time-boundary behavior.
- Preserve the write-safety rule: any lifecycle transition re-reads the current durable state inside its own write transaction before validating and saving it, so a concurrent recovery or ingestion commit is not overwritten by the preliminary batch snapshot.
- Add regression coverage for one-connection/one-SELECT full sweeps, concurrent recovery during a sweep, and readiness transitions immediately before, at and after the existing time boundary.
- Do not change REST recovery, WebSocket behavior, open-interest ingestion, public APIs or unexplained-pause diagnostics.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `canonical-storage`: lifecycle snapshot reads gain a batch form for complete runtime synchronization passes while retaining read-only snapshot and transition re-read guarantees.

## Impact

- Affected code: the stream-state read port, SQLite stream-state reader, runtime lifecycle recorder/coordinator, and focused tests.
- No schema migration, external API change, new dependency or cross-service change.
- This is a local read-path optimization, not a lifecycle or recovery redesign.
