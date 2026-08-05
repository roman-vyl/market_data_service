# Tasks: MDS → Strategy Runtime Committed-Bar Webhook v1

## 1. Contracts

- [ ] Add `runtime/committed_bar_notification.py` with a frozen
      `CommittedBarNotification` dataclass: `instrument: str`,
      `timeframe: str`, `open_time_ms: int` — no other fields.
- [ ] Add `ports/committed_bar_notifier.py` with a `CommittedBarNotifier`
      `Protocol` exposing exactly `send(self, notification:
      CommittedBarNotification) -> None`.

## 2. Settings

- [ ] Add four fields to `RuntimeSettings`
      (`runtime_webhook_enabled: bool = False`,
      `strategy_runtime_base_url: str = ""`,
      `runtime_webhook_timeout_seconds: float = 2.0`,
      `runtime_webhook_queue_capacity: int = 256`) with defaults matching the
      documented disabled-by-default posture.
- [ ] Extend `RuntimeSettings.__post_init__` to validate, only when
      `runtime_webhook_enabled` is `True`: `strategy_runtime_base_url` is a
      non-empty absolute `http`/`https` URL,
      `runtime_webhook_timeout_seconds` is a finite positive number, and
      `runtime_webhook_queue_capacity` is a positive integer. Raise
      `ValueError` on any violation, in the same constructor pass as every
      other existing validation.
- [ ] Extend `RuntimeSettings.from_environment()` to read
      `MDS_RUNTIME_WEBHOOK_ENABLED`, `MDS_STRATEGY_RUNTIME_BASE_URL`,
      `MDS_RUNTIME_WEBHOOK_TIMEOUT_SECONDS`, and
      `MDS_RUNTIME_WEBHOOK_QUEUE_CAPACITY`, matching the exact
      `env.get(...)`/coercion style already used for every other field.
- [ ] Add a composition-time check in `RuntimeService` (not in
      `RuntimeSettings`, which has no access to `ValidatedMarketConfig`):
      when `settings.runtime_webhook_enabled` is `True`, raise `ValueError`
      before constructing the notifier if
      `settings.runtime_webhook_queue_capacity < len(config.enabled_streams)`.
- [ ] Confirm (via the settings tests in section 8) that the distinction
      between "absent" and "present but invalid" is preserved for the two
      numeric fields: an absent `MDS_RUNTIME_WEBHOOK_TIMEOUT_SECONDS` or
      `MDS_RUNTIME_WEBHOOK_QUEUE_CAPACITY` SHALL resolve to its documented
      default (`2.0`, `256`) via `from_environment()`'s `env.get(key,
      default)` and SHALL NOT raise; only a *present* value that fails its
      own check (non-finite/non-positive timeout, non-positive capacity)
      SHALL raise. `MDS_STRATEGY_RUNTIME_BASE_URL` is the only field whose
      mere absence raises, because it has no usable default.

## 3. HTTP adapter

- [ ] Add `adapters/http/committed_bar_notifier.py` with
      `HttpCommittedBarNotifier` implementing `CommittedBarNotifier`: one
      constructor taking `base_url` and `timeout_seconds`, one internal POST
      helper (stdlib `urllib.request`, matching the existing
      no-new-HTTP-dependency posture of `adapters/bybit/rest_client.py`), no
      per-call client construction.
- [ ] Implement success detection as exactly: HTTP status `200` and a JSON
      body equal to `{"status": "accepted"}`. Anything else raises a typed
      adapter-local exception that the worker catches and logs — the
      adapter itself never retries.
- [ ] Enforce the configured timeout on every call; a timeout raises the
      same typed exception as any other transport failure.

## 4. Worker

- [ ] Add `CommittedBarNotificationWorker` in
      `runtime/committed_bar_notification.py`, modeled on
      `RealtimeRecoveryWorker` (`runtime/realtime_recovery_worker.py`):
      constructor takes the `CommittedBarNotifier` adapter and a bounded
      `asyncio.Queue[CommittedBarNotification]` capacity; exposes
      `async def enqueue(self, notification) -> None` and
      `async def run(self, stop_event: asyncio.Event) -> None`.
- [ ] `enqueue(...)` SHALL use `put_nowait` and catch `asyncio.QueueFull`
      itself (never `await queue.put(...)`), logging one `ERROR`-level
      message containing `instrument`, `timeframe`, `open_time_ms`, and the
      configured capacity, then returning without raising.
- [ ] `run(stop_event)` SHALL process exactly one queued notification at a
      time: dequeue, `await asyncio.to_thread(self._notifier.send, item)`
      (never call `send(...)` directly on the event-loop thread — it is a
      blocking synchronous call), log on failure, then dequeue the next
      item. No `asyncio.gather`, no task pool. It SHALL poll `stop_event`
      with a bounded timeout while idle (matching `RealtimeRecoveryWorker`'s
      `timeout=0.2` idiom), and SHALL NOT attempt to drain remaining queued
      items before exiting.
- [ ] Shutdown SHALL NOT be claimed to always complete within the 0.2s idle
      -poll bound: when `stop_event` is set while one item's
      `asyncio.to_thread(self._notifier.send, item)` call is already in
      flight, `run(...)` SHALL wait for that specific call to return or
      raise (bounded by that notification's own configured
      `runtime_webhook_timeout_seconds`) before exiting, and SHALL NOT begin
      sending any further queued item once `stop_event` is observed.
- [ ] `send(...)` failures (adapter exception of any kind) SHALL be caught
      inside the worker's loop, logged at `WARNING` with
      `instrument`/`timeframe`/`open_time_ms`/error detail, and SHALL NOT
      propagate out of `run(...)` or stop the loop from processing the next
      item.
- [ ] Add a deterministic (non-sleep-based) test proving the event loop is
      not blocked while a send is offloaded: use a fake notifier whose
      `send(...)` blocks on a controllable thread-safe gate; while that gate
      is held, assert a concurrently scheduled event-loop task (e.g. a
      second `on_outcome` call, or a trivial `asyncio.sleep(0)`-based probe
      task) still runs and completes; then release the gate and assert the
      offloaded send completes.

## 5. Wiring and hook point

- [ ] `RuntimeWiring` is NOT extended for this component — it does not
      currently own `RuntimeSettings` and this change does not add that
      dependency. Do not add a `RuntimeWiring.notifier()` factory.
- [ ] Modify `RuntimeService._build_realtime` (`runtime/service.py`) to, when
      `self._settings.runtime_webhook_enabled` is `True` (after the
      composition-time capacity check from Task 2), construct exactly one
      `HttpCommittedBarNotifier` (from `self._settings.strategy_runtime_base_url`
      / `self._settings.runtime_webhook_timeout_seconds`) and exactly one
      `CommittedBarNotificationWorker` (queue capacity from
      `self._settings.runtime_webhook_queue_capacity`); when `False`,
      construct neither and pass `None`.
- [ ] Modify `RuntimeRealtimeCoordinator.__init__` to accept an optional,
      already-constructed notifier worker reference (`None` when disabled)
      as a plain constructor parameter — it does not read `RuntimeSettings`
      or construct the worker itself.
- [ ] Modify `RuntimeRealtimeCoordinator.on_outcome`
      (`runtime/realtime.py:128`) to, immediately after the existing
      admission check (the existing early `return` when
      `not self._admission.allows(outcome.stream)`) and before the existing
      `observe_outcome` call: if a notifier worker is present and
      `outcome.classification is RealtimeIngestionClassification.COMMITTED`,
      build a `CommittedBarNotification` from `outcome.stream` and
      `outcome.open_time_ms` and call
      `await self._notifier_worker.enqueue(notification)`. Both the
      admission check and the classification check gate the notification —
      neither alone is sufficient.
- [ ] Modify `RuntimeRealtimeCoordinator.run` (`runtime/realtime.py:66`) to
      add the notifier worker's `run(stop_event)` as a fourth task inside
      the existing `asyncio.TaskGroup`, only when a notifier worker is
      present.

## 6. Verification — enqueue gating

- [ ] Test: a `COMMITTED` realtime outcome enqueues exactly one
      `CommittedBarNotification` with the correct `instrument`, `timeframe`,
      `open_time_ms`.
- [ ] Test: `DUPLICATE`, `CORRECTED`, `REJECTED`, and `FAILED` outcomes
      enqueue zero notifications.
- [ ] Test: an unconfirmed candle (handler returns `None`) never reaches
      `on_outcome` and therefore enqueues zero notifications.
- [ ] Test: a `COMMITTED` outcome for a stream that `RealtimeAdmissionGate
      .allows(...)` currently returns `False` for (not yet admitted, or no
      longer admitted) enqueues zero notifications — the admission check's
      existing early return in `on_outcome` is reached before the
      classification check, regardless of classification.
- [ ] Test: historical bootstrap (`full_bootstrap`), backfill
      (`backfill_stream`/`multi_stream_backfill`), gap repair
      (`repair_gaps`), REST import (`import_window`), and realtime recovery
      (`application/realtime/recovery.py`) each commit at least one canonical
      candle through their existing test fixtures and enqueue zero
      notifications, asserted by inspecting the notifier's captured calls
      (fake `CommittedBarNotifier` double) across a full existing
      integration path for each.
- [ ] Test: the notifier's `send(...)` is never invoked before the
      corresponding canonical commit's unit-of-work has committed (assert
      ordering via a fake UoW/notifier pair that records call order).

## 7. Verification — queue and delivery behavior

- [ ] Test: multiple rapid stream commits (fake multi-stream fixture) are
      delivered in the same order they were successfully enqueued — i.e.
      successful enqueue order equals delivery order. This test does not
      assert anything about SQLite commit-completion ordering across
      streams; the queue only owns and guarantees FIFO between its own
      enqueue and dequeue.
- [ ] Deterministic (non-sleep-based) test proving at most one outbound
      call is in flight at any instant: use a fake notifier whose `send(...)`
      blocks on a controllable gate, enqueue two notifications concurrently,
      assert the second `send` is not invoked until the first is released.
- [ ] Test: enqueue past the configured queue capacity does not raise into
      the caller, does not block `on_outcome`, does not affect the already
      -committed candle, and logs one `ERROR` containing the exact
      `instrument`/`timeframe`/`open_time_ms`/capacity of the dropped item.
- [ ] Test: a simulated timeout, a simulated transport failure, a simulated
      non-200 response, and a simulated malformed-but-200 response each
      result in exactly one logged failure, zero retries, and the worker
      proceeding to process the next queued item.

## 8. Verification — configuration and lifecycle

- [ ] Test: `MDS_RUNTIME_WEBHOOK_ENABLED` unset or `false` results in
      `RuntimeService._build_realtime` constructing no notifier worker (`None`
      passed into `RuntimeRealtimeCoordinator`), no queue constructed, no
      HTTP adapter constructed, and `RuntimeRealtimeCoordinator.run`'s
      `TaskGroup` containing exactly its pre-existing three tasks.
- [ ] Test: `MDS_RUNTIME_WEBHOOK_ENABLED=true` with a missing or malformed
      `MDS_STRATEGY_RUNTIME_BASE_URL`, a non-positive
      `MDS_RUNTIME_WEBHOOK_TIMEOUT_SECONDS`, or a non-positive
      `MDS_RUNTIME_WEBHOOK_QUEUE_CAPACITY` raises `ValueError` from
      `RuntimeSettings` construction, before any component is built.
- [ ] Test: `MDS_RUNTIME_WEBHOOK_ENABLED=true` with
      `MDS_RUNTIME_WEBHOOK_QUEUE_CAPACITY` set to a positive integer smaller
      than `len(config.enabled_streams)` raises `ValueError` from the
      `RuntimeService`-level composition-time check, before the notifier
      worker is constructed; an equal or larger value passes.
- [ ] Test: with the notifier enabled, the worker task and its HTTP adapter
      are each constructed exactly once per process start and the worker's
      `run(...)` starts exactly once.
- [ ] Test: on `stop_event.set()` while the worker is idle (no send in
      flight), the notifier worker's `run(...)` returns within the same
      bounded idle-poll budget as the existing recovery worker's shutdown
      test, without waiting for the queue to drain.
- [ ] Test: on `stop_event.set()` while one notification's
      `asyncio.to_thread(self._notifier.send, item)` call is in flight
      (use a fake notifier whose `send(...)` blocks until released), `run(...)`
      does not return until that call completes or its own configured
      timeout elapses, does not start sending any further queued item, and
      any notification still queued at that point is discarded.

## 9. Documentation

- [ ] Record the four new `MDS_*` environment variables, their defaults,
      and the disabled-by-default posture in the project's existing runtime
      configuration reference (wherever `MDS_DATABASE_PATH` and friends are
      currently documented).
- [ ] Record the accepted Live V1 losses (crash, queue overflow, HTTP
      failure — no retry) in the same location, matching the equivalent
      accepted-limitation language already used for
      `bounded-recovery-convergence-v1`.

## 10. Architecture guard

- [ ] Add or extend an architecture test asserting the HTTP adapter
      (`adapters/http/committed_bar_notifier.py`) is not imported by any
      `domain/` module and that the SQLite unit-of-work/repository layer
      never imports `ports/committed_bar_notifier.py` or the HTTP adapter —
      the notifier stays a runtime-owned side effect, never a storage
      -layer dependency.
