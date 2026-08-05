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

MDS's realtime path is not concurrent today. `RealtimeConnector` runs one
WebSocket session receive loop that processes each inbound message
strictly sequentially: receive message → `on_event` → `candle_handler
.handle` → `on_outcome` → receive next message. There is no per-stream or
per-candle asyncio task, and no existing `on_outcome` side effect (supervisor
bookkeeping, recovery-signal enqueue) runs concurrently with another —
everything already happens one after another on that single loop.

That sequential shape is exactly why an outbound Runtime notification
cannot be a direct, synchronous call made from inside `on_outcome`: doing so
would make the WebSocket receive loop wait for Runtime's network round-trip
before it could process the next inbound message, for every single commit.
At a shared boundary — the top of an hour, four hours, or midnight UTC —
many independently configured streams can each produce a `COMMITTED`
outcome within a short window; if each one blocked the receive loop on its
own Runtime call, ingestion of every other stream would stall behind
Runtime's latency, one call at a time, for as long as that latency lasts.

The queue this change adds exists to prevent that coupling — not to tame an
already-existing burst of concurrent outbound calls (none exist today; there
is no notifier of any kind in MDS yet). Its purpose is:

- decouple Runtime network latency from the WebSocket receive/ingestion
  loop, so a slow or unresponsive Runtime never delays the next candle's
  ingestion;
- provide bounded, volatile buffering between "commit observed" and
  "notification sent";
- keep exactly one HTTP attempt in flight at a time, so the notifier itself
  never becomes a second source of concurrent outbound calls;
- guarantee that a stalled or failed Runtime call never stops or slows
  candle ingestion, even transiently.

MDS ingestion correctness does not depend on Runtime notification succeeding
— committing the canonical candle is already complete and durable by the
time an outcome is available. The problem this change solves is exclusively
about *not letting outbound delivery block ingestion*: MDS must not wait on
Runtime while processing the next inbound WebSocket message, and MDS must
not send Runtime more than one HTTP request at a time regardless.

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

- Add a `CommittedBarNotifier` application port (a plain synchronous
  `send(notification) -> None`) and an `HttpCommittedBarNotifier` adapter,
  following the existing port/adapter split (`ports/market_data_source.py`
  + `adapters/bybit/rest_client.py`). The adapter is constructed exactly
  once per process, alongside the worker; it has no independent
  start/stop/close lifecycle of its own — it is a plain object holding
  `base_url`/`timeout_seconds` and becomes unreachable together with the
  worker that owns it once the worker stops, with no explicit close call
  required or performed.
- Add a bounded in-memory FIFO notification queue with exactly one consumer
  (`CommittedBarNotificationWorker`), modeled on the existing
  `RealtimeRecoveryWorker` (`runtime/realtime_recovery_worker.py`) shape:
  `enqueue(...)`, `run(stop_event)`, status-store integration, no unbounded
  retry. Because the adapter's `send(...)` is a blocking synchronous call,
  the worker offloads each send via `asyncio.to_thread(...)` rather than
  calling it directly on the event-loop thread, and awaits its completion
  before dequeuing the next item.
- Hook the worker into `RuntimeRealtimeCoordinator.on_outcome`
  (`runtime/realtime.py:128`), gated on the outcome's stream being admitted
  (`RealtimeAdmissionGate.allows(...)`, the existing early-return check)
  **and** `classification is RealtimeIngestionClassification.COMMITTED`, so
  only a genuine new normal realtime commit for a currently admitted stream
  ever enqueues a notification.
- Add four validated `RuntimeSettings` fields (`MDS_RUNTIME_WEBHOOK_ENABLED`,
  `MDS_STRATEGY_RUNTIME_BASE_URL`, `MDS_RUNTIME_WEBHOOK_TIMEOUT_SECONDS`,
  `MDS_RUNTIME_WEBHOOK_QUEUE_CAPACITY`) following the exact
  `RuntimeSettings.__post_init__`/`from_environment()` pattern already used
  for every other setting, plus one composition-time check (queue capacity
  no smaller than the configured enabled-stream count) that `RuntimeSettings`
  cannot perform on its own because it has no visibility into
  `ValidatedMarketConfig`.
- Wire the worker into `RuntimeService`'s existing composition:
  `RuntimeService` already owns `RuntimeSettings` and `ValidatedMarketConfig`,
  so `RuntimeService._build_realtime` constructs the optional notifier
  adapter and worker directly from those values and passes the
  already-constructed worker into `RuntimeRealtimeCoordinator`, which only
  starts/stops/enqueues it — it does not parse configuration itself.
  `RuntimeWiring` is not extended for this component; it does not currently
  own `RuntimeSettings`, and this change does not change that.

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
  (in-memory queue, no persistence). On a graceful shutdown, any notification
  still queued (not yet dequeued) is discarded the same way; the queue is
  never drained before the worker exits.
- If one notification's HTTP send is already in flight (offloaded to a
  thread) when shutdown begins, shutdown waits for that specific send to
  finish or time out — bounded by its own configured
  `MDS_RUNTIME_WEBHOOK_TIMEOUT_SECONDS`, not by the worker's idle-poll
  interval. This is a bounded wait, not a hang, but it is not instantaneous.
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
