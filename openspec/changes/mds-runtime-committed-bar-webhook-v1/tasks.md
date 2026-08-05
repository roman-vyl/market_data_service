# Tasks: MDS → Strategy Runtime Committed-Bar Webhook v1

## 1. Contracts

- [x] Add `ports/committed_bar_notifier.py` with a frozen
      `CommittedBarNotification` dataclass (`instrument: str`,
      `timeframe: str`, `open_time_ms: int` — no other fields) and a
      `CommittedBarNotifier` `Protocol` exposing exactly `send(self,
      notification: CommittedBarNotification) -> None`. **Deviation from
      the original plan**: both are co-located in `ports/` rather than
      splitting the dataclass into `runtime/committed_bar_notification.py`
      as originally sketched — that split would have made
      `ports/committed_bar_notifier.py` import the dataclass from
      `runtime/` while `runtime/committed_bar_notification.py` imports the
      Protocol from `ports/`, a circular import between the two modules.
      Co-locating both in the lower `ports/` layer (which `runtime/`
      already depends on one-directionally) resolves this without
      weakening either contract.

## 2. Settings

- [x] Add four fields to `RuntimeSettings`
      (`runtime_webhook_enabled: bool = False`,
      `strategy_runtime_base_url: str = ""`,
      `runtime_webhook_timeout_seconds: float = 2.0`,
      `runtime_webhook_queue_capacity: int = 256`) with defaults matching the
      documented disabled-by-default posture.
- [x] Extend `RuntimeSettings.__post_init__` to validate, only when
      `runtime_webhook_enabled` is `True`: `strategy_runtime_base_url` is a
      non-empty absolute `http`/`https` URL,
      `runtime_webhook_timeout_seconds` is a finite positive number, and
      `runtime_webhook_queue_capacity` is a positive integer. Raise
      `ValueError` on any violation, in the same constructor pass as every
      other existing validation.
- [x] Extend `RuntimeSettings.from_environment()` to read
      `MDS_RUNTIME_WEBHOOK_ENABLED`, `MDS_STRATEGY_RUNTIME_BASE_URL`,
      `MDS_RUNTIME_WEBHOOK_TIMEOUT_SECONDS`, and
      `MDS_RUNTIME_WEBHOOK_QUEUE_CAPACITY`, matching the exact
      `env.get(...)`/coercion style already used for every other field.
- [x] Add a composition-time check in `RuntimeService` (not in
      `RuntimeSettings`, which has no access to `ValidatedMarketConfig`):
      when `settings.runtime_webhook_enabled` is `True`, raise `ValueError`
      before constructing the notifier if
      `settings.runtime_webhook_queue_capacity < len(config.enabled_streams)`.
- [x] Confirm (via the settings tests in section 8) that the distinction
      between "absent" and "present but invalid" is preserved for the two
      numeric fields: an absent `MDS_RUNTIME_WEBHOOK_TIMEOUT_SECONDS` or
      `MDS_RUNTIME_WEBHOOK_QUEUE_CAPACITY` SHALL resolve to its documented
      default (`2.0`, `256`) via `from_environment()`'s `env.get(key,
      default)` and SHALL NOT raise; only a *present* value that fails its
      own check (non-finite/non-positive timeout, non-positive capacity)
      SHALL raise. `MDS_STRATEGY_RUNTIME_BASE_URL` is the only field whose
      mere absence raises, because it has no usable default.

## 3. HTTP adapter

- [x] Add `adapters/http/committed_bar_notifier.py` with
      `HttpCommittedBarNotifier` implementing `CommittedBarNotifier`: one
      constructor taking `base_url` and `timeout_seconds`, one internal POST
      helper (stdlib `urllib.request`, matching the existing
      no-new-HTTP-dependency posture of `adapters/bybit/rest_client.py`), no
      per-call client construction.
- [x] Implement success detection as exactly: HTTP status `200` and a JSON
      body equal to `{"status": "accepted"}`. Anything else raises a typed
      adapter-local exception that the worker catches and logs — the
      adapter itself never retries.
- [x] Enforce the configured timeout on every call; a timeout raises the
      same typed exception as any other transport failure.

## 4. Worker

- [x] Add `CommittedBarNotificationWorker` in
      `runtime/committed_bar_notification.py`, modeled on
      `RealtimeRecoveryWorker` (`runtime/realtime_recovery_worker.py`):
      constructor takes the `CommittedBarNotifier` adapter and a bounded
      `asyncio.Queue[CommittedBarNotification]` capacity; exposes
      `async def enqueue(self, notification) -> None` and
      `async def run(self, stop_event: asyncio.Event) -> None`.
- [x] `enqueue(...)` SHALL use `put_nowait` and catch `asyncio.QueueFull`
      itself (never `await queue.put(...)`), logging one `ERROR`-level
      message containing `instrument`, `timeframe`, `open_time_ms`, and the
      configured capacity, then returning without raising.
- [x] `run(stop_event)` SHALL process exactly one queued notification at a
      time: dequeue, `await asyncio.to_thread(self._notifier.send, item)`
      (never call `send(...)` directly on the event-loop thread — it is a
      blocking synchronous call), log on failure, then dequeue the next
      item. No `asyncio.gather`, no task pool. It SHALL poll `stop_event`
      with a bounded timeout while idle (matching `RealtimeRecoveryWorker`'s
      `timeout=0.2` idiom), and SHALL NOT attempt to drain remaining queued
      items before exiting.
- [x] Shutdown SHALL NOT be claimed to always complete within the 0.2s idle
      -poll bound: when `stop_event` is set while one item's
      `asyncio.to_thread(self._notifier.send, item)` call is already in
      flight, `run(...)` SHALL wait for that specific call to return or
      raise (bounded by that notification's own configured
      `runtime_webhook_timeout_seconds`) before exiting, and SHALL NOT begin
      sending any further queued item once `stop_event` is observed.
- [x] `send(...)` failures (adapter exception of any kind) SHALL be caught
      inside the worker's loop, logged at `WARNING` with
      `instrument`/`timeframe`/`open_time_ms`/error detail, and SHALL NOT
      propagate out of `run(...)` or stop the loop from processing the next
      item.
- [x] Add a deterministic (non-sleep-based) test proving the event loop is
      not blocked while a send is offloaded: use a fake notifier whose
      `send(...)` blocks on a controllable thread-safe gate; while that gate
      is held, assert a concurrently scheduled event-loop task (e.g. a
      second `on_outcome` call, or a trivial `asyncio.sleep(0)`-based probe
      task) still runs and completes; then release the gate and assert the
      offloaded send completes.

## 5. Wiring and hook point

- [x] `RuntimeWiring` is NOT extended for this component — it does not
      currently own `RuntimeSettings` and this change does not add that
      dependency. Do not add a `RuntimeWiring.notifier()` factory.
- [x] Modify `RuntimeService._build_realtime` (`runtime/service.py`) to
      construct the optional notifier worker and pass it into
      `RuntimeRealtimeCoordinator`. **Deviation from the original plan**:
      the construction logic (enabled check, composition-time capacity
      check, adapter + worker construction) was extracted into a new
      `build_committed_bar_notifier_worker(settings, config)` function in
      `runtime/committed_bar_notification_factory.py` rather than a private
      `RuntimeService` method, because inlining it pushed `service.py` to
      218 of the 220-line architecture limit enforced by
      `test_python_modules_remain_laconic`. `_build_realtime` now just calls
      that one function; behavior is identical to what was originally
      specified.
- [x] Modify `RuntimeRealtimeCoordinator.__init__` to accept an optional,
      already-constructed notifier worker reference (`None` when disabled)
      as a plain constructor parameter — it does not read `RuntimeSettings`
      or construct the worker itself.
- [x] Modify `RuntimeRealtimeCoordinator.on_outcome`
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
- [x] Modify `RuntimeRealtimeCoordinator.run` (`runtime/realtime.py:66`) to
      add the notifier worker's `run(stop_event)` as a fourth task inside
      the existing `asyncio.TaskGroup`, only when a notifier worker is
      present.

## 6. Verification — enqueue gating

- [x] Test: a `COMMITTED` realtime outcome enqueues exactly one
      `CommittedBarNotification` with the correct `instrument`, `timeframe`,
      `open_time_ms`.
- [x] Test: `DUPLICATE`, `CORRECTED`, `REJECTED`, and `FAILED` outcomes
      enqueue zero notifications.
- [x] Test: an unconfirmed candle (handler returns `None`) never reaches
      `on_outcome` and therefore enqueues zero notifications.
- [x] Test: a `COMMITTED` outcome for a stream that `RealtimeAdmissionGate
      .allows(...)` currently returns `False` for (not yet admitted, or no
      longer admitted) enqueues zero notifications — the admission check's
      existing early return in `on_outcome` is reached before the
      classification check, regardless of classification.
- [x] Historical bootstrap (`full_bootstrap`), backfill
      (`backfill_stream`/`multi_stream_backfill`), gap repair
      (`repair_gaps`), REST import (`import_window`), and realtime recovery
      (`application/realtime/recovery.py`) enqueue zero notifications.
      **Verified via a stronger mechanism than the originally planned
      per-path fixture test**: `test_non_live_write_paths_never_reference_the_committed_bar_notifier`
      (`tests/test_architecture_baseline.py`) asserts by import analysis
      that none of these modules (plus `application/ingest.py` and
      `application/realtime/handler.py`) import the notifier port, HTTP
      adapter, or worker module at all — they have no reference to enqueue
      through, which a per-fixture "zero calls observed" test would only
      have demonstrated empirically for the specific scenarios exercised.
      Combined with the gating tests in section 6 (which prove the *only*
      reachable enqueue site, `on_outcome`, requires `COMMITTED` on an
      admitted stream), this closes the same gap the original bullet asked
      for.
- [x] Test: the notifier's `send(...)` is never invoked before the
      corresponding canonical commit's unit-of-work has committed (assert
      ordering via a fake UoW/notifier pair that records call order).

## 7. Verification — queue and delivery behavior

- [x] Test: multiple rapid stream commits (fake multi-stream fixture) are
      delivered in the same order they were successfully enqueued — i.e.
      successful enqueue order equals delivery order. This test does not
      assert anything about SQLite commit-completion ordering across
      streams; the queue only owns and guarantees FIFO between its own
      enqueue and dequeue.
- [x] Deterministic (non-sleep-based) test proving at most one outbound
      call is in flight at any instant: use a fake notifier whose `send(...)`
      blocks on a controllable gate, enqueue two notifications concurrently,
      assert the second `send` is not invoked until the first is released.
- [x] Test: enqueue past the configured queue capacity does not raise into
      the caller, does not block `on_outcome`, does not affect the already
      -committed candle, and logs one `ERROR` containing the exact
      `instrument`/`timeframe`/`open_time_ms`/capacity of the dropped item.
- [x] Test: a simulated timeout, a simulated transport failure, a simulated
      non-200 response, and a simulated malformed-but-200 response each
      result in exactly one logged failure, zero retries, and the worker
      proceeding to process the next queued item.

## 8. Verification — configuration and lifecycle

- [x] Test: `MDS_RUNTIME_WEBHOOK_ENABLED` unset or `false` results in
      `RuntimeService._build_realtime` constructing no notifier worker (`None`
      passed into `RuntimeRealtimeCoordinator`), no queue constructed, no
      HTTP adapter constructed, and `RuntimeRealtimeCoordinator.run`'s
      `TaskGroup` containing exactly its pre-existing three tasks.
- [x] Test: `MDS_RUNTIME_WEBHOOK_ENABLED=true` with a missing or malformed
      `MDS_STRATEGY_RUNTIME_BASE_URL`, a non-positive
      `MDS_RUNTIME_WEBHOOK_TIMEOUT_SECONDS`, or a non-positive
      `MDS_RUNTIME_WEBHOOK_QUEUE_CAPACITY` raises `ValueError` from
      `RuntimeSettings` construction, before any component is built.
- [x] Test: `MDS_RUNTIME_WEBHOOK_ENABLED=true` with
      `MDS_RUNTIME_WEBHOOK_QUEUE_CAPACITY` set to a positive integer smaller
      than `len(config.enabled_streams)` raises `ValueError` from the
      `RuntimeService`-level composition-time check, before the notifier
      worker is constructed; an equal or larger value passes.
- [x] Test: with the notifier enabled, the worker task and its HTTP adapter
      are each constructed exactly once per process start and the worker's
      `run(...)` starts exactly once.
- [x] Test: on `stop_event.set()` while the worker is idle (no send in
      flight), the notifier worker's `run(...)` returns within the same
      bounded idle-poll budget as the existing recovery worker's shutdown
      test, without waiting for the queue to drain.
- [x] Test: on `stop_event.set()` while one notification's
      `asyncio.to_thread(self._notifier.send, item)` call is in flight
      (use a fake notifier whose `send(...)` blocks until released), `run(...)`
      does not return until that call completes or its own configured
      timeout elapses, does not start sending any further queued item, and
      any notification still queued at that point is discarded.

## 9. Documentation

- [x] Record the four new `MDS_*` environment variables, their defaults,
      and the disabled-by-default posture in the project's existing runtime
      configuration reference (wherever `MDS_DATABASE_PATH` and friends are
      currently documented).
- [x] Record the accepted Live V1 losses (crash, queue overflow, HTTP
      failure — no retry) in the same location, matching the equivalent
      accepted-limitation language already used for
      `bounded-recovery-convergence-v1`.

## 10. Architecture guard

- [x] Add or extend an architecture test asserting the HTTP adapter
      (`adapters/http/committed_bar_notifier.py`) is not imported by any
      `domain/` module and that the SQLite unit-of-work/repository layer
      never imports `ports/committed_bar_notifier.py` or the HTTP adapter —
      the notifier stays a runtime-owned side effect, never a storage
      -layer dependency.
