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

### Requirement: Readiness of an open-interest stream covers both data tracks
A stream whose instrument has `open_interest = true` SHALL be ready only when its candle track and its open interest track are both proven continuous from their lower bounds, both are current up to the expected bar under the same expected-bar rule and grace, and no recovery of either track is pending or unfinished. The same condition SHALL decide publication. Unflagged streams keep the existing definition.

#### Scenario: Open interest behind after the grace
- **WHEN** a ready flagged stream has candle T but no open interest at T after the expected-bar grace
- **THEN** the stream leaves ready through the existing recovery and degraded path

#### Scenario: Open interest history not loaded
- **WHEN** a flagged stream starts with gaps in its open interest history
- **THEN** it is not ready until those gaps are repaired

#### Scenario: Both tracks current
- **WHEN** candle and open interest frontiers of a flagged stream are both at the expected bar and no recovery is pending
- **THEN** the stream is ready

### Requirement: Transport-only activity avoids lifecycle storage reads
Unconfirmed candle updates, transport heartbeats and activity-only connection establishment SHALL update their existing transport facts without initiating lifecycle synchronization or lifecycle snapshot reads. Confirmed candle events SHALL retain synchronization both before ingestion and after their outcome. Subscription, disconnect, recovery and admission scenarios SHALL retain their current synchronization behavior.

#### Scenario: Intermediate candle
- **WHEN** an admitted stream receives an unconfirmed candle update
- **THEN** transport activity is updated without a lifecycle snapshot read, canonical candle mutation or notification.

#### Scenario: Heartbeat or connection establishment
- **WHEN** a heartbeat or an activity-only connected event is observed
- **THEN** its transport facts are updated without lifecycle storage access.

#### Scenario: Confirmed candle
- **WHEN** a confirmed candle is observed and its ingestion produces an outcome
- **THEN** both existing lifecycle synchronization points execute with the existing handling of committed, duplicate, corrected, rejected and failed outcomes.

### Requirement: Periodic synchronization owns time-driven projection refresh
The existing stale worker SHALL continue to evaluate every configured stream and perform its full lifecycle synchronization on every periodic turn, including turns that produce no stale signals. Time-driven changes to the stored readiness projection SHALL be reflected on the next such turn according to the configured interval and event-loop scheduling. Current supervisor readiness and publication predicates SHALL continue to evaluate the expected bar using current time and grace.

#### Scenario: Grace expires between periodic turns
- **WHEN** a required candle or OI frontier falls behind at close plus grace and only transport-only events arrive before the next turn
- **THEN** those events do not refresh lifecycle storage
- **AND** the next periodic turn revokes readiness through the existing stale and lifecycle rules.

#### Scenario: Multiple overdue streams
- **WHEN** several streams become overdue without receiving events
- **THEN** the periodic turn evaluates all of them and refreshes every affected public projection.

#### Scenario: No new stale signal
- **WHEN** stale detection produces no signal for a periodic turn
- **THEN** the full lifecycle and status refresh still executes.

### Requirement: OI observation explicitly refreshes its stream projection
After a live OI observation changes its proven frontier or its recovery/status facts, MDS SHALL explicitly synchronize that stream's lifecycle and public status before the publication eligibility decision. The callback SHALL reuse the existing lifecycle transition rules and SHALL NOT synchronize unrelated streams. Prefetch storage alone SHALL NOT advance the proven frontier.

#### Scenario: OI completes the required data set
- **WHEN** a stored OI observation advances the frontier and makes a stream data-ready
- **THEN** its lifecycle and public projection are refreshed without waiting for another WS event
- **AND** notification eligibility is evaluated with the existing published-through rule.

#### Scenario: OI reveals a discontinuity
- **WHEN** an OI observation requires recovery even though the frontier cannot advance
- **THEN** that stream's recovery requirement is propagated and its lifecycle and projection are synchronized before publication is evaluated.

#### Scenario: Duplicate OI or failed append
- **WHEN** an OI append fails or a duplicate observation changes no frontier or recovery/status facts
- **THEN** no additional lifecycle synchronization is initiated by that observation.

#### Scenario: Stream isolation
- **WHEN** one stream's OI facts change
- **THEN** the explicit OI-triggered lifecycle synchronization reads and updates only that stream.
