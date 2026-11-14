## Why

A full realtime lifecycle synchronization currently opens one read-only SQLite connection and performs separate catalog/state lookups for every configured stream. As the stream set grows, this local polling path creates avoidable connection and query overhead even when no lifecycle transition is needed.

## What Changes

- Add a batch lifecycle-snapshot read for a complete synchronization pass: one read-only SQLite connection, one read transaction and one SELECT returning all requested stream states.
- Preserve the configured stream order and the existing missing-stream failure semantics when the batch result is projected back to the requested streams.
- Use the batch snapshot only for full lifecycle sweeps; a synchronization explicitly scoped to one stream keeps the existing single-stream read.
- Preserve READY/DEGRADED/FAILED predicates and processing order. Capture realtime facts before batch I/O. Public durable-state projection uses the batch read view and may briefly lag concurrent commits until a subsequent sweep; exact equivalence to sequential point-read projections is not promised.
- Preserve the write-safety rule: any lifecycle transition re-reads the current durable state inside its own write transaction before validating and saving it, so a concurrent recovery or ingestion commit is not overwritten by the preliminary batch snapshot.
- Guard realtime lifecycle decisions by comparing the preliminary source state with the current state inside the existing write transaction. On a mismatch, preserve and project the current row and abandon that stream's remaining transitions until a later pass; do not suppress domain errors when the states match.
- Add regression coverage for one-connection/one-SELECT full sweeps, concurrent recovery including the no-write path, stale projection isolation from candle admission/publication, and a readiness boundary crossed during batch I/O.
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
