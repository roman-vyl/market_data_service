# Proposal: MDS → Strategy Runtime Committed-Bar Webhook v1

## Why

Today `RealtimeCandleHandler` produces a `RealtimeIngestionOutcome`, and
`RuntimeRealtimeCoordinator.on_outcome` (`runtime/realtime.py:128`) consumes
it only for supervisor bookkeeping — nothing notifies Strategy Runtime of a
closed bar. There is no notifier, no Runtime URL, no webhook timeout, and no
outbound queue anywhere in `market_data_service`.

MDS's realtime path is currently sequential: one WebSocket receive loop
processes each message strictly one after another (receive → `on_event` →
`candle_handler.handle` → `on_outcome` → next message), with no per-candle
concurrency. A direct, synchronous Runtime call from inside `on_outcome`
would therefore block the receive loop on Runtime's network latency for
every commit — and at a shared timeframe boundary (top of an hour, midnight
UTC), many streams can commit within a short window, each one stalling
ingestion of every other stream behind Runtime's response time.

This change adds a bounded, single-consumer outbound queue to decouple
Runtime's latency from ingestion: enqueue is non-blocking, exactly one HTTP
attempt is in flight at a time, and a slow or failed Runtime call never
delays or blocks candle ingestion. MDS ingestion correctness never depends
on notification delivery succeeding — the canonical candle is already
durably committed by the time an outcome exists.

### Why best-effort, not durable delivery

A transactional outbox or message broker is out of scope for Live V1: this
is one MDS process talking to one Runtime process, canonical history is
already recoverable from SQLite regardless of notification delivery, and
Runtime's own downstream state is itself non-durable in Live V1 — a durable
pipe feeding a non-durable consumer buys no real end-to-end durability.
Accepted V1 losses: a notification lost to a process crash before it's
sent, one dropped because the bounded queue is full, and one that was
attempted but failed or timed out. None of these corrupt canonical MDS
history.

### Why this is split across two repositories

`market_data_service` owns deciding when a commit is a genuine new live
commit and owns the outbound delivery attempt. It has no visibility into
strategy deployments or how Runtime processes an accepted event — that's
`strategy_runtime`'s own change (`runtime-bounded-committed-bar-intake-v1`).
Each side ships, reviews, and rolls back independently as long as the wire
contract (below) holds.

## What changes

- Add a `CommittedBarNotifier` port (`send(notification) -> None`) and an
  `HttpCommittedBarNotifier` adapter, following the existing port/adapter
  split. Constructed once per process; no independent lifecycle beyond the
  worker that owns it.
- Add a bounded in-memory FIFO queue with exactly one consumer,
  `CommittedBarNotificationWorker`, modeled on `RealtimeRecoveryWorker`.
  Sends are offloaded via `asyncio.to_thread(...)` since the adapter's
  `send(...)` is a blocking synchronous call.
- Hook the worker into `RuntimeRealtimeCoordinator.on_outcome`, gated on
  admission (`RealtimeAdmissionGate.allows(...)`) **and**
  `classification is COMMITTED`.
- Add four validated `RuntimeSettings` fields
  (`MDS_RUNTIME_WEBHOOK_ENABLED`, `MDS_STRATEGY_RUNTIME_BASE_URL`,
  `MDS_RUNTIME_WEBHOOK_TIMEOUT_SECONDS`, `MDS_RUNTIME_WEBHOOK_QUEUE_CAPACITY`),
  plus a composition-time check that queue capacity is at least the
  enabled-stream count.
- Wire construction through `RuntimeService._build_realtime`, which already
  owns `RuntimeSettings` and `ValidatedMarketConfig`.

## What does not change

- No transactional outbox, no SQLite notification table, no message broker.
- No automatic retry, no exactly-once delivery, no horizontal scaling.
- No change to canonical ingestion, classification, or SQLite commit
  behavior.
- No notification for unconfirmed candles, `DUPLICATE`, `CORRECTED`,
  `REJECTED`, `FAILED`, or any historical/backfill/repair/import/recovery
  path.
- No strategy or trading semantics in the payload — exactly
  `{"instrument", "timeframe", "open_time_ms"}`.

## Accepted losses (Live V1)

- A queued-but-unsent notification is lost on crash or graceful shutdown
  (in-memory, no persistence; the queue is never drained at shutdown).
- If a send is already in flight when shutdown begins, shutdown waits for
  that one send (bounded by `MDS_RUNTIME_WEBHOOK_TIMEOUT_SECONDS`) — not
  instantaneous, but bounded.
- A notification is lost when the queue is full, or when the HTTP attempt
  times out, fails transport, or returns non-success — exactly one attempt,
  no retry.
- None of these affect canonical MDS candle history, which remains fully
  recoverable through existing backfill/repair regardless of notification
  outcome.

## Contractual dependency on `runtime-bounded-committed-bar-intake-v1`

The outbound payload is a strict subset of, and byte-compatible with, the
`POST /v1/webhooks/closed-bar` body documented in `strategy_runtime`'s
`http-closed-bar` capability: `instrument` (non-empty string), `timeframe`
(non-empty string), `open_time_ms` (non-negative integer), with
`extra="ignore"` on the Runtime side. This proposal does not require
Runtime's companion change to land first: MDS degrades to "attempted,
logged, discarded" on any Runtime-side failure, including a `503`. Either
change may ship, roll out, or roll back independently.
