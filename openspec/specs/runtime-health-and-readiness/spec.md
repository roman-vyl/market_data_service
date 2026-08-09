# Runtime Health and Readiness Specification

## Purpose
Defines process health, per-stream data readiness, realtime-live diagnostics, lifecycle projection, and public health/readiness endpoints.

## Requirements

### Requirement: Runtime is a thin composition boundary
Runtime orchestration SHALL compose validated configuration, canonical storage, historical reconciliation, realtime ingestion/recovery, status projection, and HTTP serving without reimplementing candle validation, ingestion, gap detection, repair, or WebSocket protocol rules inside the runtime layer.

#### Scenario: Runtime coordinates existing use cases
- **WHEN** startup or recovery requires historical or realtime work
- **THEN** runtime dispatches to the owning application/realtime components instead of introducing a second data algorithm.

### Requirement: Runtime settings have validated precedence
Supported explicit CLI values SHALL override environment values, which SHALL override documented defaults. Invalid numeric/policy settings SHALL fail validation before the long-running service loop begins.

#### Scenario: CLI overrides an environment value
- **WHEN** a supported runtime setting is provided both by environment and an explicit CLI option
- **THEN** the CLI value is used after normal settings validation.

### Requirement: Process health is separate from data readiness
MDS process health SHALL represent whether the runtime is operating without a fatal process error. Process health SHALL NOT imply that configured market streams are data-ready.

#### Scenario: Runtime is healthy while historical work remains
- **WHEN** background runtime workers are operating but one or more streams are still reconciling
- **THEN** `/health` may return healthy while `/readiness` remains not ready.

### Requirement: Public health endpoint
`GET /health` SHALL return HTTP `200` when the process is healthy and HTTP `503` when it is unhealthy, with a body containing `status` and `fatal_error`.

#### Scenario: Fatal runtime error affects health
- **WHEN** the runtime records a fatal process error
- **THEN** `/health` returns `503` and exposes the fatal error detail.

### Requirement: Per-stream readiness requires durable and realtime readiness
A stream SHALL be publicly ready only when its durable lifecycle is `ready` and realtime supervision reports an active subscription, restored recovery, no pending recovery, and no fatal realtime error.

#### Scenario: Restored subscription without durable ready is insufficient
- **WHEN** realtime facts are data-ready but the durable stream state is not `ready`
- **THEN** the public stream readiness remains false.

### Requirement: Aggregate readiness is strict
Service readiness SHALL be true only when at least one configured stream exists and every configured stream is ready.

#### Scenario: One stream blocks aggregate readiness
- **WHEN** any configured stream is not ready
- **THEN** `GET /readiness` returns HTTP `503` even if all other streams are ready.

### Requirement: Public readiness endpoint exposes stream diagnostics
`GET /readiness` SHALL return the aggregate `ready` flag plus one status object per configured stream containing durable state, realtime status, data-ready flag, realtime-live flag, ready flag, and blocking reason.

#### Scenario: Stream is blocked by historical reconciliation
- **WHEN** a stream still owns pending historical work
- **THEN** its readiness object remains false and exposes a corresponding blocking reason.

### Requirement: Realtime-live is stricter than data readiness
`realtime_live` SHALL be true only after a stream is data-ready and confirmed realtime activity has been observed at or after successful recovery completion.

#### Scenario: Restored stream has not yet seen a live close
- **WHEN** recovery completes and the stream becomes data-ready before another confirmed realtime close arrives
- **THEN** `ready` may be true while `realtime_live` remains false.

### Requirement: Realtime degradation revokes readiness
A ready/connecting stream that requires realtime recovery or becomes stale/disconnected SHALL be represented as unavailable until recovery restores it. Fatal realtime/storage failure SHALL prevent readiness for the affected stream.

#### Scenario: Ready stream becomes stale
- **WHEN** realtime supervision marks a ready stream stale and recovery pending
- **THEN** its readiness is revoked until successful recovery restores the required conditions.

### Requirement: Runtime startup remains restart-reconciled
On process start, persisted stream lifecycle SHALL be reconciled again before current realtime readiness is trusted; transient connection/supervision facts SHALL be rebuilt in memory.

#### Scenario: Process restarts from previously ready storage
- **WHEN** MDS restarts with a stream previously persisted as `ready`
- **THEN** runtime performs reconciliation/reconnection rather than treating the old ready flag alone as proof of current readiness.
### Requirement: Graceful shutdown preserves committed state
SIGINT/SIGTERM-driven shutdown SHALL stop new runtime work, cancel/finish background workers at their existing safe boundaries, close HTTP/WebSocket resources, and preserve every already committed SQLite transaction for restart reconciliation.

#### Scenario: Shutdown occurs with unfinished historical work
- **WHEN** shutdown begins while a stream still has pending reconciliation
- **THEN** no committed canonical progress is discarded and restart can reconstruct the unfinished work from canonical state.
