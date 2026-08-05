# Proposal: MDS → Strategy Runtime Committed-Bar Webhook v1

## Why

The chain that should close is:

```text
Bybit confirmed candle
→ MDS canonical ingestion
→ SQLite commit
→ RealtimeIngestionOutcome(COMMITTED)
→ [gap]
→ Runtime POST /v1/webhooks/closed-bar
```

Today `RealtimeCandleHandler` (`application/realtime/handler.py`) produces a
`RealtimeIngestionOutcome` and `RuntimeRealtimeCoordinator.on_outcome`
(`runtime/realtime.py:128`) consumes it only to drive supervisor
recovery-signal bookkeeping. Nothing calls out to Strategy Runtime. There is
no notifier, no Strategy Runtime URL, no webhook timeout, and no outbound
queue anywhere in `market_data_service` (confirmed by exhaustive grep — zero
matches for `webhook`, `notifier`, `strategy_runtime`, `closed-bar`).

Boundary timestamps — the top of an hour, four hours, or midnight UTC — are
exactly when many configured `ticker × timeframe` streams confirm a candle
close within the same wall-clock second. Each of those closes independently
produces a `COMMITTED` outcome on its own asyncio task inside the same
process. Without an explicit bound between "candle committed" and "Runtime
notified," a burst of near-simultaneous commits would turn into a burst of
near-simultaneous outbound HTTP calls to Runtime, all competing for the same
network path and the same downstream Runtime intake, with no backpressure on
the MDS side to slow that fan-out down.

MDS ingestion correctness does not depend on Runtime notification succeeding
— committing the canonical candle is already complete and durable by the
time an outcome is available. The problem this change solves is exclusively
about *not blocking or bursting* the outbound edge: MDS must not wait on
Runtime, and MDS must not send Runtime more than one HTTP request at a time.

### Why best-effort, not durable delivery

A transactional outbox, a persisted notification table, or a message broker
would give Runtime a durable, replayable stream of committed-bar events. That
is deliberately out of scope for Live V1: this system runs as one MDS process
and one Runtime process, canonical candle history is already recoverable from
SQLite via existing backfill/repair paths regardless of whether a
notification was ever delivered, and Runtime's own downstream state
(`InMemoryStrategyInstanceRuntimeStateRepository`) is itself non-durable in
Live V1 — a durable notification pipe feeding an in-memory consumer would not
buy end-to-end durability, only a false sense of it. Given that, the accepted
V1 losses are: a notification enqueued but never sent because the process
crashes before the sender drains it, a notification dropped because the
bounded queue is full, and a notification the sender attempted but which
timed out, failed transport, or received a non-success response. None of
these losses corrupt canonical MDS history; they only mean Runtime may miss
a chance to react to one closed bar in real time, which is the accepted
Live V1 boundary condition already recorded for the equivalent gap on the
Runtime side (see `strategy_runtime`'s
`runtime-production-composition` capability, "Non-durable Live V1 limitation
is accepted, not open").

### Why this is split across two repositories

`market_data_service` owns creating the notification (deciding *when* a
commit is "real, live, and normal" versus historical/duplicate/corrected/
rejected/recovered) and owns the outbound HTTP delivery attempt. It has no
visibility into and no opinion about strategy deployments, strategy
instances, or how Runtime processes an accepted event once delivered — that
is `strategy_runtime`'s owned behavior, proposed separately as
`runtime-bounded-committed-bar-intake-v1`. Folding both into one
cross-repository change would make one proposal own two different runtime
processes' concurrency models and two different teams' deployable units;
each side can be implemented, reviewed, and rolled back independently as
long as the wire contract between them (documented in this proposal's
"Contractual dependency" section) is held fixed.

## What changes

- Add a `CommittedBarNotifier` application port and a lifecycle-owned HTTP
  adapter, following the existing port/adapter split
  (`ports/market_data_source.py` + `adapters/bybit/rest_client.py`).
- Add a bounded in-memory FIFO notification queue with exactly one consumer
  (`CommittedBarNotificationWorker`), modeled on the existing
  `RealtimeRecoveryWorker` (`runtime/realtime_recovery_worker.py`) shape:
  `enqueue(...)`, `run(stop_event)`, status-store integration, no unbounded
  retry.
- Hook the worker into `RuntimeRealtimeCoordinator.on_outcome`
  (`runtime/realtime.py:128`), gated on
  `classification is RealtimeIngestionClassification.COMMITTED`, so only a
  genuine new normal realtime commit ever enqueues a notification.
- Add four validated `RuntimeSettings` fields (`MDS_RUNTIME_WEBHOOK_ENABLED`,
  `MDS_STRATEGY_RUNTIME_BASE_URL`, `MDS_RUNTIME_WEBHOOK_TIMEOUT_SECONDS`,
  `MDS_RUNTIME_WEBHOOK_QUEUE_CAPACITY`) following the exact
  `RuntimeSettings.__post_init__`/`from_environment()` pattern already used
  for every other setting.
- Wire the worker into `RuntimeWiring`/`RuntimeService.run`'s existing
  `TaskGroup` so it starts and stops exactly once, alongside the historical
  and realtime workers.

## What does not change

- No transactional outbox, no SQLite notification table, no message broker.
- No automatic retry, no exactly-once delivery, no horizontal scaling.
- No change to canonical ingestion, classification, or SQLite commit
  behavior — the notifier observes an already-committed outcome, it never
  participates in the commit transaction.
- No new notification for unconfirmed candles, `DUPLICATE`, `CORRECTED`,
  `REJECTED`, `FAILED`, or any historical bootstrap/backfill/repair/import/
  realtime-recovery/startup-reconciliation path.
- No strategy, deployment, or trading semantics — the payload is exactly
  `{"instrument", "timeframe", "open_time_ms"}` and nothing else.

## Accepted losses (Live V1)

- A notification queued but not yet sent is lost on process crash or restart
  (in-memory queue, no persistence).
- A notification is lost outright when the queue is full (bounded capacity,
  no blocking, no rollback of ingestion).
- A notification is lost when the HTTP attempt times out, fails transport,
  returns a non-success status, or returns a malformed success body — exactly
  one attempt is made, with no retry.
- None of these losses affect canonical MDS candle history, which remains
  fully recoverable through existing backfill/repair regardless of
  notification outcome.

## Contractual dependency on `runtime-bounded-committed-bar-intake-v1`

This change's outbound payload is a strict subset of, and byte-compatible
with, the `POST /v1/webhooks/closed-bar` request body documented in
`strategy_runtime`'s `http-closed-bar` capability spec: exactly
`instrument` (non-empty string), `timeframe` (non-empty string), and
`open_time_ms` (non-negative JSON integer), with `extra="ignore"` on the
Runtime side. This proposal does not require Runtime's companion change to
land first or land at all: MDS's notifier degrades to "attempted, logged,
discarded" on any Runtime-side failure, including a `503` returned by
Runtime's new queue-full response. The two changes may ship, roll out, and
roll back independently; neither one's tests depend on the other repository.
