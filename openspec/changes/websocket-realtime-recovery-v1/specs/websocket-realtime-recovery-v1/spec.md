# Specification: WebSocket Realtime Ingestion and Recovery v1

## Requirement: Separated realtime responsibilities

The realtime subsystem SHALL separate exchange transport, connection/subscription lifecycle, confirmed-candle ingestion, per-stream supervision, and historical recovery coordination.

No single adapter, connector, handler, supervisor, or coordinator SHALL own all five responsibilities.

The implementation MAY use functions or small objects rather than classes, but the responsibility boundaries SHALL remain observable in dependencies and tests.

## Requirement: WebSocket adapter is transport-only

The Bybit WebSocket adapter SHALL own exchange protocol parsing, heartbeat, subscription messages, and transport lifecycle events.

The adapter SHALL NOT write SQLite, call canonical ingestion, mutate persisted stream lifecycle, run continuity audit, invoke backfill/repair, or decide readiness.

## Requirement: Connector owns bounded connection lifecycle

The realtime connector SHALL connect, subscribe to the deterministic configured topic set, receive normalized adapter events, support cancellation, and stop cleanly.

Reconnect SHALL use a bounded, cancellable policy and SHALL report exhaustion. The connector SHALL NOT retry forever.

REST recovery SHALL NOT execute synchronously inside the WebSocket receive callback.

## Requirement: All configured streams are supported

Realtime subscription routing SHALL cover every enabled configured `ticker × canonical_timeframe` stream.

The canonical routing key SHALL be derived deterministically from the exact exchange symbol and registered Bybit interval.

Unknown, duplicate, or ambiguous mappings SHALL be rejected before live processing begins.

A failure or stale condition for one stream SHALL NOT mutate another stream's candle, state, freshness, or recovery progress.

## Requirement: Confirmed closes only enter canonical storage

Only exchange-confirmed closed candle observations SHALL enter canonical persistence.

Unconfirmed or in-progress updates SHALL NOT be persisted as canonical closed candles.

Confirmed closes SHALL be normalized and passed to the existing `IngestObservedCandle` use case.

The realtime path SHALL NOT implement a second duplicate, correction, validation, quarantine, or SQLite mutation algorithm.

## Requirement: Observable realtime ingestion outcomes

Processing a confirmed close SHALL produce an outcome identifying the canonical stream, candle open time, and one of:

- `committed`;
- `duplicate`;
- `corrected`;
- `rejected`;
- `failed`.

`duplicate` and `corrected` SHALL be valid canonical outcomes and SHALL NOT by themselves require historical recovery.

Rejected observations and failures SHALL include typed detail for supervision and recovery decisions.

## Requirement: Supervisor reports symptoms, not historical truth

The realtime supervisor SHALL track connection, subscription, activity, stale status, confirmed-close activity, and ingestion outcomes per stream in memory.

The supervisor MAY detect an expected-grid sequence discontinuity and emit a recovery-required signal.
Sequence and rejected-observation signals SHALL carry the earliest suspected open time
when it is known. This value is a bounded recovery hint, not continuity proof.

A sequence signal SHALL NOT be treated as a full continuity proof. The supervisor SHALL NOT invoke REST backfill, continuity audit, or gap repair directly.

## Requirement: Recovery triggers

The supervisor SHALL emit a recovery-required signal after at least:

- disconnect followed by transport restoration;
- stale-stream detection;
- confirmed-close sequence discontinuity;
- rejected realtime candle requiring REST verification.

A storage failure SHALL make realtime ingestion unavailable for the affected stream and SHALL NOT be treated as an ordinary REST-repair case.

## Requirement: REST-authoritative recovery coordinator

The realtime recovery coordinator SHALL reconcile an affected stream through existing bounded historical use cases.

It SHALL derive a bounded interval from durable latest committed state and the latest fully closed boundary, then compose existing trailing backfill, continuity audit, gap repair, and post-recovery audit as required.

The coordinator SHALL NOT implement a second window splitter, gap detector, candle importer, or ingestion path.

Recovery SHALL be serialized per affected stream. Independent streams SHALL remain isolated.

The recovery result SHALL distinguish at least:

- `restored`;
- `incomplete`;
- `recoverable_failure`;
- `fatal_failure`.

`restored` SHALL require successful post-recovery continuity audit and SHALL expose the
latest canonical open time covered by that proof.

## Requirement: Reconnect does not imply readiness

A restored WebSocket transport or subscription SHALL NOT by itself make a stream ready.

Realtime readiness MAY be true only after:

- the configured subscription is active;
- required REST recovery is complete;
- post-recovery continuity is proven;
- acceptable confirmed realtime activity is observed after recovery completion;
- no fatal ingestion/storage failure is active.

Aggregate process readiness and HTTP readiness exposure belong to `runtime-startup-orchestration-v1`.

## Requirement: No realtime event or recovery-job persistence

The v1 realtime subsystem SHALL NOT require a persisted WebSocket event log, replay broker, subscription-event table, recovery-job table, or server-owned consumer cursor.

Connection, subscription, stale, and reconnecting status SHALL be rebuilt after process restart.

Canonical candles, existing lifecycle state, and quarantine SHALL continue to use existing persistence contracts.

## Requirement: Fake end-to-end realtime matrix

The project SHALL include a deterministic fake WebSocket transport, fake REST source, and temporary SQLite integration matrix.

The matrix SHALL cover multiple symbols and multiple timeframes, confirmed and unconfirmed updates, canonical ingestion outcomes, disconnect/reconnect, stale detection, sequence discontinuity, bounded REST recovery, gap repair, post-recovery audit, and stream isolation.

## Requirement: Real bounded WebSocket smoke

The project SHALL provide a bounded real Bybit public WebSocket smoke that subscribes to configured streams, observes transport/subscription events, processes at least one confirmed candle close when available within the configured bound, and exits cleanly without consumer dependencies.

## Requirement: Physical module cohesion and dependency direction

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

Architecture tests SHALL enforce these dependency boundaries. Implementations SHALL split
cohesive modules before increasing file-size limits or introducing a broad manager/service object.
