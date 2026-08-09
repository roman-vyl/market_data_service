# websocket-realtime-recovery-v1 Specification

## Purpose
TBD - created by archiving change websocket-realtime-recovery-v1. Update Purpose after archive.
## Requirements
### Requirement: Separated realtime responsibilities

The realtime subsystem SHALL separate exchange transport, connection/subscription lifecycle, confirmed-candle ingestion, per-stream supervision, and historical recovery coordination.

No single adapter, connector, handler, supervisor, or coordinator SHALL own all five responsibilities.

The implementation MAY use functions or small objects rather than classes, but the responsibility boundaries SHALL remain observable in dependencies and tests.

#### Scenario: Realtime subsystem is composed of five separated roles

- **WHEN** the realtime subsystem processes a confirmed candle close from connect through canonical ingestion, supervision, and recovery
- **THEN** exchange transport, connection/subscription lifecycle, candle ingestion, per-stream supervision, and historical recovery are each owned by a distinct module
- **AND** no single module implements all five responsibilities

### Requirement: WebSocket adapter is transport-only

The Bybit WebSocket adapter SHALL own exchange protocol parsing, heartbeat, subscription messages, and transport lifecycle events.

The adapter SHALL NOT write SQLite, call canonical ingestion, mutate persisted stream lifecycle, run continuity audit, invoke backfill/repair, or decide readiness.

#### Scenario: Adapter parses protocol events without touching storage or readiness

- **WHEN** the Bybit WebSocket adapter receives a heartbeat, subscription acknowledgement, or kline event
- **THEN** it parses and emits the corresponding transport-neutral event
- **AND** it does not write SQLite, call canonical ingestion, mutate persisted stream lifecycle, run continuity audit, invoke backfill/repair, or decide readiness

### Requirement: Connector owns bounded connection lifecycle

The realtime connector SHALL connect, subscribe to the deterministic configured topic set, receive normalized adapter events, support cancellation, and stop cleanly.

Reconnect SHALL use a bounded, cancellable policy and SHALL report exhaustion. The connector SHALL NOT retry forever.

REST recovery SHALL NOT execute synchronously inside the WebSocket receive callback.

#### Scenario: Connector stops cleanly on cancellation

- **WHEN** the connector's cancellation is invoked
- **THEN** the connector stops cleanly and releases its resources

#### Scenario: Bounded reconnect reports exhaustion instead of retrying forever

- **WHEN** the connector's reconnect attempts exhaust the configured bounded, cancellable reconnect policy
- **THEN** the connector reports reconnect exhaustion instead of continuing to retry

### Requirement: All configured streams are supported

Realtime subscription routing SHALL cover every enabled configured `ticker × canonical_timeframe` stream.

The canonical routing key SHALL be derived deterministically from the exact exchange symbol and registered Bybit interval.

Unknown, duplicate, or ambiguous mappings SHALL be rejected before live processing begins.

A failure or stale condition for one stream SHALL NOT mutate another stream's candle, state, freshness, or recovery progress.

#### Scenario: Topic map expands every configured symbol and timeframe

- **WHEN** the validated market configuration enables multiple instruments and timeframes
- **THEN** the realtime topic map deterministically expands to cover every enabled `ticker × canonical_timeframe` stream, keyed by exact exchange symbol and registered Bybit interval

#### Scenario: Unknown topic is rejected before live processing

- **WHEN** an incoming topic does not map to a configured, unambiguous `StreamKey`
- **THEN** the mapping is rejected before live processing begins

#### Scenario: One stream's failure does not affect another stream

- **WHEN** one configured stream experiences a failure or stale condition
- **THEN** other configured streams' candle, state, freshness, and recovery progress remain unaffected

### Requirement: Confirmed closes only enter canonical storage

Only exchange-confirmed closed candle observations SHALL enter canonical persistence.

Unconfirmed or in-progress updates SHALL NOT be persisted as canonical closed candles.

Confirmed closes SHALL be normalized and passed to the existing `IngestObservedCandle` use case.

The realtime path SHALL NOT implement a second duplicate, correction, validation, quarantine, or SQLite mutation algorithm.

#### Scenario: Unconfirmed update is ignored for canonical persistence

- **WHEN** a WebSocket candle update is not exchange-confirmed
- **THEN** it is not persisted as a canonical closed candle

#### Scenario: Confirmed close is normalized and routed through existing ingestion

- **WHEN** a WebSocket candle update is exchange-confirmed
- **THEN** it is normalized to the existing transport-neutral candle observation
- **AND** it is passed to the existing `IngestObservedCandle` use case rather than a separate realtime-only mutation path

### Requirement: Observable realtime ingestion outcomes

Processing a confirmed close SHALL produce an outcome identifying the canonical stream, candle open time, and one of:

- `committed`;
- `duplicate`;
- `corrected`;
- `rejected`;
- `failed`.

`duplicate` and `corrected` SHALL be valid canonical outcomes and SHALL NOT by themselves require historical recovery.

Rejected observations and failures SHALL include typed detail for supervision and recovery decisions.

#### Scenario: Confirmed close reports one of the defined outcome classifications

- **WHEN** a confirmed close is processed by the realtime candle handler
- **THEN** the resulting outcome identifies the canonical stream and candle open time
- **AND** classifies the outcome as `committed`, `duplicate`, `corrected`, `rejected`, or `failed`

#### Scenario: Duplicate and corrected outcomes do not by themselves require recovery

- **WHEN** a confirmed close is classified `duplicate` or `corrected`
- **THEN** the outcome is a valid canonical outcome
- **AND** it does not by itself trigger historical recovery

#### Scenario: Rejected or failed outcomes carry typed detail

- **WHEN** a confirmed close is classified `rejected` or `failed`
- **THEN** the outcome includes typed detail usable for supervision and recovery decisions

### Requirement: Supervisor reports symptoms, not historical truth

The realtime supervisor SHALL track connection, subscription, activity, stale status, confirmed-close activity, and ingestion outcomes per stream in memory.

The supervisor MAY detect an expected-grid sequence discontinuity and emit a recovery-required signal.
Sequence and rejected-observation signals SHALL carry the earliest suspected open time
when it is known. This value is a bounded recovery hint, not continuity proof.

A sequence signal SHALL NOT be treated as a full continuity proof. The supervisor SHALL NOT invoke REST backfill, continuity audit, or gap repair directly.

#### Scenario: Supervisor tracks per-stream operational state in memory

- **WHEN** the supervisor observes connection, subscription, transport activity, confirmed-close activity, or ingestion outcomes for a stream
- **THEN** it updates that stream's in-memory operational state independently of other streams

#### Scenario: Sequence discontinuity emits a bounded hint, not a continuity proof

- **WHEN** an incoming confirmed close skips an expected grid point
- **THEN** the supervisor emits a recovery-required signal carrying the earliest suspected open time when known
- **AND** it does not treat that signal as a full continuity proof
- **AND** it does not itself invoke REST backfill, continuity audit, or gap repair

### Requirement: Recovery triggers

The supervisor SHALL emit a recovery-required signal after at least:

- disconnect followed by transport restoration;
- stale-stream detection;
- confirmed-close sequence discontinuity;
- rejected realtime candle requiring REST verification.

A storage failure SHALL make realtime ingestion unavailable for the affected stream and SHALL NOT be treated as an ordinary REST-repair case.

#### Scenario: Recovery-required signal follows reconnection after disconnect

- **WHEN** a stream disconnects and its transport is subsequently restored
- **THEN** the supervisor emits a recovery-required signal for that stream

#### Scenario: Recovery-required signal follows stale-stream detection

- **WHEN** a stream is detected stale
- **THEN** the supervisor emits a recovery-required signal for that stream

#### Scenario: Storage failure is fatal to realtime ingestion, not an ordinary repair case

- **WHEN** a storage failure occurs while ingesting a confirmed close
- **THEN** realtime ingestion becomes unavailable for that affected stream
- **AND** the failure is not treated as an ordinary REST-repair case

### Requirement: REST-authoritative recovery coordinator

The realtime recovery coordinator SHALL reconcile an affected stream through existing bounded historical use cases.

It SHALL derive a bounded interval from durable latest committed state and the latest fully closed boundary, then compose existing trailing backfill, continuity audit, gap repair, and post-recovery audit as required.

The coordinator SHALL NOT implement a second window splitter, gap detector, candle importer, or ingestion path.

Recovery SHALL be serialized per affected stream. Independent streams SHALL remain isolated.

Runtime SHALL retain ownership of non-terminal realtime recovery results.
`incomplete` SHALL be requeued for another bounded recovery attempt.
`recoverable_failure` SHALL enter per-stream backoff and then be requeued.
`fatal_failure` SHALL fail the affected stream without retry. One stream in
recovery backoff SHALL NOT prevent another due stream from running.

The recovery result SHALL distinguish at least:

- `restored`;
- `incomplete`;
- `recoverable_failure`;
- `fatal_failure`.

`restored` SHALL require successful post-recovery continuity audit and SHALL expose the
latest canonical open time covered by that proof.

#### Scenario: Recovery derives a bounded interval and composes existing historical use cases

- **WHEN** the recovery coordinator reconciles an affected stream
- **THEN** it derives a bounded recovery interval from durable latest committed state and the latest fully closed boundary
- **AND** it composes existing trailing backfill, continuity audit, and gap repair rather than implementing a second window splitter, gap detector, candle importer, or ingestion path

#### Scenario: Restored requires successful post-recovery continuity audit

- **WHEN** recovery for an affected stream completes with successful post-recovery continuity audit
- **THEN** the recovery result is `restored`
- **AND** it exposes the latest canonical open time covered by that proof

#### Scenario: Incomplete recovery is requeued for another bounded attempt

- **WHEN** a bounded recovery attempt for a stream returns `incomplete`
- **THEN** runtime requeues that stream for another bounded recovery attempt

#### Scenario: Recoverable failure backs off then is requeued without blocking other streams

- **WHEN** a bounded recovery attempt for a stream returns `recoverable_failure`
- **THEN** that stream enters per-stream backoff and is later requeued
- **AND** another due stream is not prevented from running while this stream is in backoff

#### Scenario: Fatal recovery failure fails the stream without retry

- **WHEN** a bounded recovery attempt for a stream returns `fatal_failure`
- **THEN** that stream fails without further retry

### Requirement: Reconnect does not imply readiness

A restored WebSocket transport or subscription SHALL NOT by itself make a stream data-ready.

Data readiness MAY be true only after:

- the configured subscription is active;
- required REST recovery is complete;
- post-recovery continuity is proven;
- no fatal ingestion/storage failure is active.

Realtime-live diagnostics MAY be true only after acceptable confirmed realtime activity is observed after recovery completion.

Aggregate process readiness and HTTP readiness exposure belong to `runtime-startup-orchestration-v1`.

#### Scenario: Restored transport alone does not make a stream data-ready

- **WHEN** a stream's WebSocket transport and subscription are restored after a disconnect
- **THEN** the stream is not, by that fact alone, considered data-ready

#### Scenario: Data readiness requires active subscription, completed recovery, proven continuity, and no fatal failure

- **WHEN** a stream's configured subscription is active, required REST recovery is complete, post-recovery continuity is proven, and no fatal ingestion/storage failure is active
- **THEN** the stream may be considered data-ready

#### Scenario: Realtime-live diagnostics require confirmed activity after recovery completion

- **WHEN** acceptable confirmed realtime activity is observed for a stream after its recovery completes
- **THEN** that stream's realtime-live diagnostics may be considered true

### Requirement: No realtime event or recovery-job persistence

The v1 realtime subsystem SHALL NOT require a persisted WebSocket event log, replay broker, subscription-event table, recovery-job table, or server-owned consumer cursor.

Connection, subscription, stale, and reconnecting status SHALL be rebuilt after process restart.

Canonical candles, existing lifecycle state, and quarantine SHALL continue to use existing persistence contracts.

#### Scenario: Runtime connection facts are rebuilt after restart without new persistence tables

- **WHEN** the process restarts
- **THEN** connection, subscription, stale, and reconnecting status are rebuilt from existing durable facts
- **AND** no persisted WebSocket-event, subscription-event, recovery-job table, or server-owned consumer cursor is required to do so

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

