# Tasks: MDS → Strategy Runtime Committed-Bar Webhook v1

## 1. Contracts

- [x] `ports/committed_bar_notifier.py`: `CommittedBarNotification` (frozen
      dataclass: `instrument`, `timeframe`, `open_time_ms`) and
      `CommittedBarNotifier` (`Protocol` with `send(...)`), co-located to
      avoid a `ports`/`runtime` import cycle.

## 2. Settings

- [x] Four `RuntimeSettings` fields with a disabled-by-default posture:
      `runtime_webhook_enabled`, `strategy_runtime_base_url`,
      `runtime_webhook_timeout_seconds`, `runtime_webhook_queue_capacity`.
- [x] `__post_init__` validates the three non-enabled fields only when
      `runtime_webhook_enabled` is `True`.
- [x] `from_environment()` reads `enabled` first and skips parsing the other
      three variables when disabled, so a malformed leftover value for a
      disabled feature never fails startup.
- [x] Composition-time check (in the factory, not `RuntimeSettings`, which
      has no `ValidatedMarketConfig` visibility): queue capacity must be at
      least the enabled-stream count.

## 3. HTTP adapter

- [x] `adapters/http/committed_bar_notifier.py`: `HttpCommittedBarNotifier`,
      one constructor (`base_url`, `timeout_seconds`), stdlib
      `urllib.request`-based POST, no per-call client construction.
- [x] Success is exactly HTTP `200` with body `{"status": "accepted"}`;
      anything else raises a typed adapter-local exception, no retry.
- [x] Configured timeout enforced on every call.

## 4. Worker

- [x] `CommittedBarNotificationWorker` (`runtime/committed_bar_notification.py`),
      modeled on `RealtimeRecoveryWorker`: `enqueue(...)` (non-blocking,
      `QueueFull`-safe) and `run(stop_event)`.
- [x] `run(...)` processes exactly one item at a time, offloads `send(...)`
      via `asyncio.to_thread(...)`, never drains the queue on exit, polls
      `stop_event` at a bounded idle interval.
- [x] An in-flight send is never abandoned by external cancellation,
      including repeated cancellations arriving while a wait is already in
      progress.
- [x] Delivery failures are caught, logged once at `WARNING`, and never
      stop the loop.

## 5. Wiring and hook point

- [x] `RuntimeWiring` is not extended — it does not own `RuntimeSettings`
      and does not gain that dependency here.
- [x] `RuntimeService._build_realtime` constructs the optional worker via
      `build_committed_bar_notifier_worker(settings, config)`
      (`runtime/committed_bar_notification_factory.py`) and passes it into
      `RuntimeRealtimeCoordinator`.
- [x] `RuntimeRealtimeCoordinator.__init__` accepts the already-constructed
      worker (or `None`); it never constructs one itself.
- [x] `on_outcome` enqueues immediately after the existing admission check,
      gated on admission **and** `classification is COMMITTED`.
- [x] `run()` adds the worker's `run(stop_event)` as a fourth `TaskGroup`
      task, only when a worker is present.

## 6. Verification — enqueue gating

- [x] Exactly one notification enqueued per genuine `COMMITTED` outcome on
      an admitted stream, with the correct payload.
- [x] Zero notifications for `DUPLICATE`/`CORRECTED`/`REJECTED`/`FAILED`,
      for unconfirmed candles, for a not-yet-admitted stream, and for every
      historical/backfill/repair/import/recovery write path (the last
      enforced by an architecture-level import-absence guard,
      `tests/test_architecture_baseline.py`).
- [x] `send(...)` is never invoked before the corresponding commit's
      unit-of-work has committed.

## 7. Verification — queue and delivery behavior

- [x] FIFO: successful enqueue order equals delivery order under rapid
      multi-stream commits.
- [x] At most one outbound call in flight at any instant.
- [x] Queue-full drop does not raise into the caller, does not affect the
      committed candle, and logs exactly once with the dropped item's
      identity and capacity.
- [x] Timeout, transport failure, non-200, and malformed-200 each result in
      exactly one logged failure, zero retries, and the worker proceeding
      to the next item.

## 8. Verification — configuration and lifecycle

- [x] Disabled (unset or `false`): no worker, queue, or adapter
      constructed; `TaskGroup` contains only its pre-existing tasks.
- [x] Enabled with a missing/invalid base URL, or a non-positive
      timeout/capacity, raises `ValueError` from `RuntimeSettings` before
      any component is built.
- [x] Enabled with capacity smaller than the enabled-stream count raises
      `ValueError` from the composition-time check; an equal or larger
      value passes.
- [x] Worker and adapter are each constructed exactly once per process
      start; `run(...)` starts exactly once.
- [x] Shutdown while idle exits within the idle-poll bound; shutdown or
      cancellation (including repeated cancellation) while a send is in
      flight waits for that one send before exiting, without starting
      another.

## 9. Documentation

- [x] The four new `MDS_*` environment variables, their defaults, and the
      disabled-by-default posture are recorded in the runtime configuration
      reference.
- [x] The accepted Live V1 losses (crash, queue overflow, HTTP failure — no
      retry) are recorded in the same location.

## 10. Architecture guard

- [x] An architecture test asserts the HTTP adapter and notifier port are
      never imported by `domain/` or the SQLite unit-of-work/repository
      layer.
