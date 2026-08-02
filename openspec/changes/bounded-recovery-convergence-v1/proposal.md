# Proposal: Bounded Recovery Convergence v1

## Why

The 2026-08-02 readiness incident exposed two independent bounded-work loops that retain
runtime ownership but cannot make durable progress:

1. unresolved historical lower-bound discovery restarts from instrument `launchTime` after
   every exhausted pass because empty probe progress is not persisted;
2. realtime recovery reuses an old `suspected_start_time_ms`, repeats the same bounded
   backfill prefix, and requeues `INCOMPLETE` immediately without changing its cursor.

Both defects violate the intended meaning of a bounded window budget. A budget is a fair work
quantum, not permission to repeat an identical prefix forever. They can keep one or more streams
permanently unready while continuously calling Bybit.

The incident also exposed an admission handoff race. A realtime supervisor is initialized before
long historical reconciliation finishes. Opening the admission gate later does not first refresh
the supervisor from the newly advanced durable stream cursor, so the first admitted candle can
produce an obsolete sequence-discontinuity hint.

## What changes

This change establishes an explicit convergence contract for every bounded recovery workflow:

- persist the next per-stream lower-bound discovery probe after each successfully classified
  empty window;
- atomically clear that probe cursor when the actual earliest available candle is resolved;
- resume lower-bound discovery from durable progress after the next pass or process restart;
- synchronize realtime supervisor progress from durable `stream_state` immediately before
  opening a stream's admission gate;
- replace repeated realtime prefix backfill with a fixed-window preflight/repair/post-audit cycle
  through the existing `RepairStreamGaps` use case;
- retain the same fixed recovery window across bounded `INCOMPLETE` passes;
- distinguish progressing `INCOMPLETE` work from unchanged no-progress attempts;
- apply bounded backoff and actionable diagnostics to unchanged attempts;
- stop reusing the startup discovery budget as the realtime recovery budget;
- bound repeated identical unresolved-gap quarantine diagnostics.

## Persistence impact

`stream_state` gains one nullable, stream-scoped operational field:

```text
lower_bound_discovery_next_open_time_ms
```

It is a discovery cursor only. It is not an observed lower bound, candle fact, continuity proof,
gap job, or readiness signal.

The schema change is additive and preserves existing instruments, streams, candles, lifecycle
state, and quarantine rows. It SHALL be delivered as an explicit forward SQLite schema migration
for existing service-owned volumes and as part of fresh-database creation. No dual-read,
compatibility shim, new job table, or second state store is introduced.

## Intended outcome

For every configured stream:

```text
bounded pass makes durable progress
or returns a typed source failure
or enters bounded no-progress backoff
or reaches a fatal terminal result
```

An unchanged bounded prefix SHALL NOT be retried continuously. A restart SHALL resume
lower-bound discovery from its last successful empty probe, while unfinished realtime recovery
SHALL be reconstructed from canonical candles and the normal startup full-window audit.

## Scope

This change includes:

- lower-bound discovery persistence and schema migration;
- historical worker progress/no-progress scheduling;
- realtime admission cursor synchronization;
- realtime recovery planning and orchestration;
- fair `INCOMPLETE` retry and no-progress backoff;
- structured recovery logging and bounded duplicate diagnostics;
- regression, restart, multi-stream, fake-runtime, and Docker smoke coverage;
- updates to database, operational-scenario, and runtime-recovery documentation.

## Capabilities

### New capability

- `bounded-recovery-convergence-v1`: guarantees durable forward progress, finite recovery scopes,
  safe realtime admission, and bounded retry behavior for lower-bound discovery and realtime
  continuity recovery.

This capability refines the previously approved historical reconciliation and WebSocket recovery
contracts without adding another market-data ingestion or persistence path.

## Existing behavior reused

This change preserves and reuses:

- canonical `StreamKey` identity and timeframe registry;
- half-open aligned fetch windows;
- `AuditStreamContinuity` as continuity authority;
- `RepairStreamGaps` preflight/repair/post-audit behavior;
- `ImportHistoricalWindow` and the single canonical ingestion path;
- atomic per-window SQLite commits;
- source-failure classification and per-stream lifecycle;
- process-level serialization of historical and realtime REST-authoritative work.

## Non-goals

- adding parallel REST workers or a second scheduler;
- adding a persisted realtime recovery queue or gap-job table;
- treating transport events or supervisor memory as canonical truth;
- changing candle validation, duplicate, correction, or numeric semantics;
- changing Bybit protocol parsing or REST window limits;
- introducing strategy, signal, consumer-cursor, or execution behavior;
- masking a no-progress loop solely by increasing REST budgets.
