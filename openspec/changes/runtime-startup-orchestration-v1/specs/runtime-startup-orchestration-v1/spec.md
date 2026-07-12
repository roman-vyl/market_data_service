# Specification: Runtime Startup Orchestration v1

## Requirement: Thin runtime composition

The runtime SHALL compose existing configuration, historical, realtime, storage, and readiness components and SHALL NOT reimplement their domain or application rules.

Runtime settings, startup reconciliation, realtime dispatch, status projection, HTTP serving, and process entrypoint responsibilities SHALL remain in separate cohesive modules.

## Requirement: Validated settings

CLI values SHALL override environment values, which SHALL override documented defaults. Invalid paths, ports, budgets, reconnect policies, stale policies, or log levels SHALL be rejected before network or storage mutation.

## Requirement: Deterministic bounded startup reconciliation

Startup SHALL register and process every enabled configured `ticker × canonical_timeframe` stream in deterministic configuration order.

Each stream SHALL receive explicit positive startup backfill and repair window budgets. Startup SHALL NOT silently initiate unlimited historical work.

Interrupted durable states SHALL be reconciled from canonical storage facts. Persisted `ready` SHALL NOT be trusted after restart.

## Requirement: Historical proof before realtime

A stream SHALL enter `connecting` only after its historical target is reached, continuity audit is successful, and any required repair has passed post-repair audit.

Incomplete bootstrap, unresolved gaps, recoverable source failure, or fatal failure SHALL leave the stream not ready with an explicit outcome.

## Requirement: Realtime dispatch and recovery

Connector events and ingestion outcomes SHALL be passed to the existing realtime supervisor. `RecoveryRequired` signals SHALL be processed by the existing realtime recovery coordinator outside WebSocket transport callbacks.

Recovery SHALL be bounded, serialized per stream, and duplicate pending signals SHALL be coalesced.

## Requirement: Strict readiness

A stream SHALL be public-ready only when durable lifecycle state is `ready` and supervisor facts report realtime readiness after subscription, successful recovery reconciliation, and a fresh confirmed close.

Disconnect, stale state, sequence discontinuity, rejected observation, pending recovery, or fatal ingestion failure SHALL make the affected stream not ready without mutating independent streams.

Aggregate readiness SHALL be true only when at least one required stream exists and every enabled required stream is ready.

## Requirement: Health and readiness HTTP contract

`GET /health` SHALL return HTTP 200 with a typed JSON process-health document when the runtime is operational, and HTTP 503 for fatal process initialization/runtime failure.

`GET /readiness` SHALL return HTTP 200 only when aggregate readiness is true; otherwise it SHALL return HTTP 503. The response SHALL list every configured stream with durable state, realtime status, ready flag, and blocking reason.

## Requirement: Failure isolation

Recoverable failure for one stream SHALL leave that stream degraded/unready and SHALL NOT prevent independent streams from starting or remaining ready. Fatal configuration, schema, or process-invariant failure SHALL make process health unsuccessful.

## Requirement: Graceful shutdown

The runtime SHALL stop HTTP acceptance, signal realtime cancellation, stop stale/recovery workers, close network clients, and release process resources cleanly. Committed SQLite transactions SHALL remain durable.

## Requirement: Docker persistence and restart

The production container SHALL run one `serve` process and SHALL persist SQLite on a mounted volume. Container restart SHALL preserve data and SHALL repeat historical/realtime reconciliation before readiness.
