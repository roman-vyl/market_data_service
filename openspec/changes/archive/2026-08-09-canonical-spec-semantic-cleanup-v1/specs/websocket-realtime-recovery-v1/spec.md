# Specification: WebSocket Realtime Ingestion and Recovery v1 — canonical semantic cleanup

## REMOVED Requirements

### Requirement: Fake end-to-end realtime matrix

Reason: this requires the existence of a specific verification artifact (a
fake-transport integration test matrix), not production behavior. It is
acceptance/verification history, preserved in
`openspec/changes/archive/2026-08-09-websocket-realtime-recovery-v1/` and in
the current test suite itself.

### Requirement: Real bounded WebSocket smoke

Reason: same as above — this requires the existence of a bounded real-Bybit
smoke script, not production runtime behavior. Preserved as historical
evidence in the archive.

## MODIFIED Requirements

### Requirement: Physical module cohesion and dependency direction

The realtime implementation SHALL preserve separate modules for exchange transport/protocol,
connection lifecycle, confirmed-candle handling, per-stream supervision, and historical recovery.

A single runtime, manager, service, or adapter SHALL NOT own more than one of the following
primary responsibilities: socket/protocol I/O, canonical candle ingestion, operational
supervision, and historical recovery orchestration.

Application connector code SHALL depend on realtime port contracts rather than concrete Bybit
adapter classes.

Transport/protocol modules SHALL NOT import SQLite, repositories, Unit of Work, canonical
ingestion, audit, backfill, repair, or readiness projection.

The supervisor SHALL NOT write canonical candles or invoke historical workflows. The recovery
coordinator SHALL NOT parse WebSocket frames or manage socket lifecycle.

Connection/subscription/stale/reconnect facts SHALL remain in-memory in v1 and SHALL be rebuilt
after restart. No WebSocket-event, connection-state, or recovery-job persistence table SHALL be
introduced.

#### Scenario: Realtime modules stay separated by primary responsibility

- **WHEN** the realtime implementation is inspected for module responsibilities
- **THEN** exchange transport/protocol, connection lifecycle, confirmed-candle handling, per-stream supervision, and historical recovery each live in separate modules
- **AND** no single runtime/manager/service/adapter module owns more than one of socket/protocol I/O, canonical candle ingestion, operational supervision, or historical recovery orchestration

#### Scenario: Architecture tests enforce forbidden dependency boundaries

- **WHEN** the architecture test suite runs against the realtime modules
- **THEN** it rejects a transport/protocol module importing SQLite, repositories, Unit of Work, canonical ingestion, audit, backfill, repair, or readiness projection
- **AND** it rejects the supervisor writing canonical candles or invoking historical workflows
- **AND** it rejects the recovery coordinator parsing WebSocket frames or managing socket lifecycle
