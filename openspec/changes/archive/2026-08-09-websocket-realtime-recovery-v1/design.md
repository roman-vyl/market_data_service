# Design: WebSocket Realtime Ingestion and Recovery v1

## Mandatory module decomposition

The realtime subsystem SHALL be physically decomposed into cohesive modules. A single
`websocket_runtime`, `manager`, `service`, or equivalent module SHALL NOT own transport,
protocol parsing, subscription routing, canonical ingestion, supervision, and historical
recovery together.

The expected responsibility map is:

```text
adapters/bybit/websocket/
  transport.py       socket open/send/receive/close and heartbeat I/O only
  protocol.py        Bybit envelopes, acknowledgements, errors, and kline parsing
  topics.py          configured topic construction and topic ↔ StreamKey routing
  adapter.py         small protocol-facing facade only

application/realtime/
  events.py          transport-neutral realtime event contracts
  connector.py       one cancellable connection/subscription receive lifecycle
  handler.py         confirmed candle → canonical ingestion → observable outcome
  supervisor.py      per-stream operational supervision and recovery signals
  supervisor_state.py in-memory state mutation only
  supervisor_types.py supervisor facts/status/policy models
  recovery.py        orchestration of existing historical recovery use cases
  recovery_plan.py   bounded recovery interval planning only
  recovery_state.py  persisted lifecycle transition recording only
  recovery_types.py  recovery request/result contracts
  outcomes.py        realtime ingestion outcome models

ports/realtime.py    transport/session protocols used by application code
```

Exact filenames MAY differ when a smaller implementation is clearer, but the responsibility
boundaries and dependency direction are mandatory. Additional small modules are preferred
over expanding a multi-purpose runtime object.

### Allowed dependency direction

```text
transport → protocol/adapter → connector → handler → canonical ingestion
connector/handler outcomes → supervisor → recovery coordinator → historical workflows
```

The connector SHALL depend on realtime ports/contracts, not on a concrete Bybit adapter.
The recovery coordinator MAY depend on existing application historical use cases, but not on
WebSocket frame parsing or transport implementation.

### Forbidden dependencies

- transport or protocol modules SHALL NOT import SQLite adapters, repositories, UoW, audit, backfill, repair, or readiness projection;
- connector SHALL NOT execute SQL, canonical candle mutation, continuity audit, or REST recovery;
- handler SHALL NOT own socket lifecycle, reconnect, stale detection, audit, or repair;
- supervisor SHALL NOT parse WebSocket frames, write candles, execute SQL, or invoke historical use cases directly;
- recovery coordinator SHALL NOT open WebSocket connections, parse Bybit frames, or implement duplicate/correction/gap logic;
- no module SHALL introduce a second candle ingestion path, second gap detector, second window splitter, or direct candle write path.

### Runtime versus persisted state

Connection, subscription, heartbeat, stale, reconnecting, and last-activity facts are in-memory
runtime state. They SHALL be rebuilt after process restart and SHALL NOT require a new
WebSocket-event or connection-state table in v1. Canonical candles, quarantine, and persisted
stream lifecycle continue to use the existing storage contracts.

### Cohesion guard

Every realtime production module SHALL have one primary responsibility. Architecture tests
SHALL reject imports that cross the forbidden boundaries above. File-size limits remain a
secondary guard: the implementation SHALL split responsibilities before raising size limits or
creating a broad manager/service module.

## Architectural shape

The realtime subsystem is intentionally split into five small responsibilities:

```text
BybitWebSocketAdapter
        ↓ normalized transport events
RealtimeConnector
        ↓ connection/subscription events and candle observations
RealtimeCandleHandler
        ↓ canonical ingestion outcomes
RealtimeSupervisor
        ↓ recovery-required / freshness state
RealtimeRecoveryCoordinator
        ↓ bounded REST reconciliation result
```

These names describe responsibilities, not mandatory class names. Implementations may use functions or small objects when that is simpler. No role should become a broad manager/service container.

## 1. Bybit WebSocket Adapter

The adapter owns only Bybit-specific protocol details:

- opening and closing the public linear WebSocket transport;
- subscription and unsubscription messages;
- heartbeat/ping/pong handling;
- parsing transport envelopes;
- mapping exchange `symbol + interval` topics to configured canonical streams;
- emitting transport-neutral connection, subscription, heartbeat, and candle events.

The adapter does not:

- write SQLite;
- call canonical ingestion;
- change persisted stream lifecycle;
- detect historical gaps;
- run REST catch-up, audit, or repair;
- decide readiness.

## 2. Realtime Connector

The connector owns one active WebSocket connection lifecycle:

- connect;
- subscribe to the deterministic configured topic set;
- receive normalized adapter events;
- expose cancellation and clean stop;
- report connected, subscribed, disconnected, transport-error, and stopped events.

The connector delivers candle observations to the handler and lifecycle events to the supervisor. It does not block transport callbacks on REST recovery work.

Reconnect attempts use a bounded, cancellable policy supplied by configuration. The connector reports exhaustion rather than retrying forever.

## 3. Realtime Candle Handler

The handler owns the normal candle path:

1. resolve the configured canonical `StreamKey`;
2. ignore non-confirmed candle updates for canonical persistence;
3. normalize a confirmed close to the existing transport-neutral candle observation;
4. call the existing canonical `IngestObservedCandle` use case;
5. return a small observable outcome.

The outcome distinguishes at least:

- `committed`;
- `duplicate`;
- `corrected`;
- `rejected`;
- `failed`.

`duplicate` and `corrected` are valid canonical outcomes and do not automatically trigger recovery. A rejected observation or storage failure is reported upward with typed detail.

The handler does not perform full continuity audit or invoke backfill/repair.

## 4. Realtime Supervisor

The supervisor maintains in-memory operational state per canonical stream:

- expected subscription;
- subscription confirmed or absent;
- last transport activity;
- last confirmed candle observation;
- last successful ingestion outcome;
- live, stale, disconnected, reconnecting, recovery-required, or stopped status.

Connection status is runtime state, not a new persisted event history.

The supervisor may perform a cheap sequence check using the last successfully committed/open-time observation and the stream timeframe. If an incoming confirmed close skips an expected grid point, the supervisor emits `recovery_required`; it does not claim a proven gap and does not repair it directly.

A recovery-required signal identifies the stream and reason. Sequence-discontinuity
and rejected-observation signals also carry the earliest suspected open time so recovery
can inspect an internal range even when a later candle already advanced durable tail.
The hint narrows the bounded range; continuity audit remains authoritative.

The supervisor emits recovery-required signals for at least:

- disconnect followed by reconnection;
- stale stream;
- confirmed-close sequence discontinuity;
- rejected realtime candle requiring REST verification.

A storage failure is fatal to realtime ingestion for the affected stream and is not treated as a normal REST-repair case.

## 5. Realtime Recovery Coordinator

The recovery coordinator owns historical reconciliation after a supervisor signal.

For one affected stream it:

1. marks the stream unavailable for readiness projection;
2. derives a bounded recovery interval from durable storage and the latest fully closed boundary;
3. runs existing REST historical workflows rather than implementing a new repair path;
4. performs continuity audit;
5. performs bounded trailing backfill and/or `RepairStreamGaps` as required by existing contracts;
6. performs post-recovery audit;
7. reports `restored`, `incomplete`, `recoverable_failure`, or `fatal_failure`;
8. on `restored`, reports the canonical open time through which history was proven.

The coordinator does not run inside a WebSocket message callback. Runtime owns
`incomplete` and `recoverable_failure` recovery results for the process lifetime:
`incomplete` is requeued immediately, `recoverable_failure` is requeued after
per-stream backoff, and `fatal_failure` is not retried. Recovery work is
serialized per stream and scheduled fairly so one affected stream cannot starve
another due stream. Independent streams remain isolated.

REST remains the authority for reconnect recovery and conflict resolution. WebSocket observations use canonical duplicate/correction classification but do not silently override REST-authoritative recovery results.

## Multi-symbol and multi-timeframe routing

The validated market configuration expands every enabled instrument into its configured canonical streams.

The realtime topic map is deterministic:

```text
(exchange_symbol, bybit_interval) → StreamKey
```

For example:

```text
(BTCUSDT, 1)  → BTCUSDT.P:1m
(BTCUSDT, 5)  → BTCUSDT.P:5m
(BTCUSDT, 60) → BTCUSDT.P:1h
(ETHUSDT, 1)  → ETHUSDT.P:1m
```

Unknown, duplicate, or ambiguous topics are rejected before the connection begins. State, freshness, sequence checks, ingestion outcomes, and recovery are tracked independently per stream.

## Normal-path transaction boundary

A confirmed candle close uses the existing canonical ingestion transaction. The WebSocket transport does not hold SQLite transactions.

No audit or REST request occurs inside the candle-ingestion transaction.

## Reconnect and readiness ordering

Reconnect transport success alone is insufficient for readiness.

The required ordering is:

```text
disconnect/stale/sequence signal
→ stream not ready
→ connector re-establishes transport and subscriptions
→ recovery coordinator proves historical continuity
→ coordinator reports the restored-through canonical boundary
→ data readiness may be projected true
→ supervisor later observes a confirmed close at or after recovery completion
→ realtime-live diagnostics may be projected true
```

The final service-level readiness projection and HTTP exposure belong to `runtime-startup-orchestration-v1`. This change provides the realtime and recovery facts required by that projection.

## Persistence policy

No new realtime-event, subscription-event, or recovery-job table is introduced in v1.

Canonical candles, existing stream lifecycle state, and quarantine remain persisted through existing repositories. Connection and subscription status are rebuilt after process restart.

A schema change requires a separate explicit decision and is not implied by this design.

## Testing strategy

Testing uses two levels:

1. deterministic fake WebSocket transport plus fake REST API and temporary SQLite;
2. bounded real Bybit WebSocket smoke without consumer dependencies.

The fake matrix covers multiple symbols and timeframes, confirmed/unconfirmed updates, duplicates, corrections, disconnect, stale detection, sequence discontinuity, bounded recovery, and stream isolation.
