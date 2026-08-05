# Design: MDS → Strategy Runtime Committed-Bar Webhook v1

## Module decomposition

```text
ports/
  committed_bar_notifier.py      CommittedBarNotifier Protocol (port)

adapters/http/
  committed_bar_notifier.py      HttpCommittedBarNotifier adapter
                                  (implements CommittedBarNotifier)

runtime/
  committed_bar_notification.py  CommittedBarNotification value object,
                                  CommittedBarNotificationWorker
  settings.py                    + 4 new validated fields (existing file)
  service.py                     + notifier adapter/worker construction
                                  inside _build_realtime, using the
                                  RuntimeSettings and ValidatedMarketConfig
                                  it already owns; TaskGroup wiring
                                  (existing file)
  realtime.py                    on_outcome gains the enqueue call;
                                  RuntimeRealtimeCoordinator receives an
                                  already-constructed optional worker
                                  reference — it does not construct one
                                  itself (existing file)
```

`wiring.py` is **not** modified by this change. `RuntimeWiring` does not
currently own `RuntimeSettings` (it owns `database`, `config`, `rest_source`,
`clock` — see `wiring.py:38-42`), and extending its constructor to also
accept `RuntimeSettings` (or the four notifier fields individually) purely
to support one optional component was considered and rejected: `RuntimeWiring`
composes already-validated pieces from data it already has, and every one of
its existing factory methods (`candle_handler()`, `recovery()`, etc.) needs
no configuration beyond what it already holds. `RuntimeService` already
loads and holds both `RuntimeSettings` and `ValidatedMarketConfig`
(`service.py:44-45`), so it is the one component with everything the
notifier needs to be constructed without a signature change anywhere else.

`ports/committed_bar_notifier.py` mirrors `ports/market_data_source.py`: a
plain `typing.Protocol`, no framework dependency, domain types only:

```python
class CommittedBarNotifier(Protocol):
    def send(self, notification: CommittedBarNotification) -> None: ...
```

`adapters/http/committed_bar_notifier.py` mirrors
`adapters/bybit/rest_client.py`'s layering: a small adapter class owning
timeout/error handling around one POST call. It does not need
`adapters/bybit/http_transport.py`'s `JsonHttpTransport` Protocol reused
directly (that Protocol is Bybit-response-shaped); it gets its own minimal
`urllib.request`-based POST helper so the notifier adapter carries no new
runtime dependency (`pyproject.toml` currently declares neither `httpx` nor
`requests`).

## Ownership and object lifetime

- **Queue**: a single bounded `asyncio.Queue[CommittedBarNotification]` with
  `maxsize=settings.runtime_webhook_queue_capacity`, owned exclusively by
  `CommittedBarNotificationWorker`. No other component holds a reference to
  it.
- **Worker**: `CommittedBarNotificationWorker` is constructed once per
  process, inside `RuntimeService._build_realtime`, directly from the
  `RuntimeSettings` and `ValidatedMarketConfig` that `RuntimeService` already
  holds (not via a `RuntimeWiring` factory — see "Module decomposition"
  above for why). It is the single consumer of the queue and owns exactly
  one `HttpCommittedBarNotifier` instance for its process lifetime.
- **Producer**: `RuntimeRealtimeCoordinator.on_outcome` calls
  `await self._notifier_worker.enqueue(outcome)` — a non-blocking
  `queue.put_nowait(...)` wrapped with `QueueFull` handling, never
  `queue.put(...)` (which would block the caller and, transitively, the
  connector's receive loop). `RuntimeRealtimeCoordinator` receives the
  already-constructed worker (or `None`) through its constructor; it never
  constructs one itself and never reads `RuntimeSettings`.
- **HTTP client**: one adapter object, constructed once and reused for every
  outbound call — never per-notification. The adapter's internal POST
  helper uses stdlib `urllib.request.urlopen(...)`, which does **not**
  guarantee a persistent, reused TCP connection across calls: each call may
  open a fresh connection. This change does not implement or claim
  connection-reuse/keep-alive behavior — only that the same *adapter object*
  (and therefore the same configured `base_url`/`timeout_seconds`) is reused,
  not that the same socket is. Persistent-connection reuse, if desired, is a
  possible future optimization requiring a different transport, not a
  property this change provides.

## Non-blocking event loop

`CommittedBarNotifier.send(...) -> None` is a plain **synchronous** method —
the adapter's `urllib`-based POST call blocks the calling thread for up to
`runtime_webhook_timeout_seconds`. The worker's consumer loop MUST NOT call
`send(...)` directly on the event-loop thread, because that would stall
every other coroutine scheduled on the same loop (including, transitively,
the WebSocket receive loop this design exists to protect) for the duration
of the call. Instead, the worker offloads each send to a thread:

```python
while not stop_event.is_set():
    try:
        item = await asyncio.wait_for(queue.get(), timeout=0.2)
    except TimeoutError:
        continue
    try:
        await asyncio.to_thread(self._notifier.send, item)
    except Exception:
        self._logger.warning(..., instrument=item.instrument, ...)
```

`await asyncio.to_thread(...)` yields control of the event loop back to the
scheduler for the duration of the blocking call, so other tasks (the
connector's receive loop, the recovery worker, the stale worker) continue to
run normally while one notification's HTTP send is in progress on its own
thread. The worker still awaits that call's completion before dequeuing the
next item — offloading to a thread changes *where* the blocking happens, not
*whether* sends remain strictly sequential (see "Concurrency limits" below).

## Concurrency limits

```text
max concurrent outbound committed-bar HTTP calls from MDS = 1
```

This is a direct consequence of there being exactly one
`CommittedBarNotificationWorker.run(stop_event)` task, which processes its
queue with a plain sequential loop (`while not stop_event.is_set(): item =
await queue.get(); await asyncio.to_thread(self._notifier.send, item)`),
never `asyncio.gather` or a task pool. The next queued item is not dequeued
until the previous `asyncio.to_thread(...)` call has returned (success or
failure) — this is what makes "one MDS HTTP sender" true even though enqueue
can be called concurrently from multiple in-flight `on_outcome` invocations,
and even though the send itself runs on a worker thread rather than the
event-loop thread (offloading to a thread bounds *where* the blocking I/O
happens, not *how many* sends can be in flight — the worker awaits each
`to_thread` call before starting the next).

## Enqueue is gated, not the write path

The gate lives entirely in `on_outcome`, not in `IngestObservedCandle` or
`RealtimeCandleHandler`:

```python
async def on_outcome(self, outcome: RealtimeIngestionOutcome) -> None:
    if not self._admission.allows(outcome.stream):
        return
    if outcome.classification is RealtimeIngestionClassification.COMMITTED:
        await self._notifier_worker.enqueue(
            CommittedBarNotification(
                instrument=outcome.stream.instrument.canonical_id,
                timeframe=outcome.stream.timeframe,
                open_time_ms=outcome.open_time_ms,
            )
        )
    for signal in self._supervisor.observe_outcome(outcome):
        await self._enqueue(signal)
    self._sync_lifecycle()
```

This is provably sufficient to exclude every non-live path because
`RealtimeIngestionOutcome` objects are only ever constructed by
`RealtimeCandleHandler.handle` (`application/realtime/handler.py:43`), which
is only ever called from `RealtimeConnector`'s live WebSocket receive loop,
which is the only caller of the `on_outcome` callback wired in
`RuntimeService._build_realtime` (`runtime/service.py:149-150`). Historical
bootstrap (`full_bootstrap.py`), backfill (`backfill_stream.py`,
`multi_stream_backfill.py`), gap repair (`repair_gaps.py`), import
(`import_window.py`), and realtime recovery (`application/realtime/
recovery.py`) all write through `IngestObservedCandle.execute_in_unit_of_work`
directly and never construct a `RealtimeIngestionOutcome` or call
`on_outcome`. No additional "is this a live commit" flag needs to be
threaded through the storage layer — the existing call-graph shape already
enforces the separation. This is treated as an architectural invariant of
this change: the notifier hook SHALL live at `on_outcome` and SHALL NOT be
duplicated inside `RealtimeCandleHandler`, `IngestObservedCandle`, or any
historical/recovery use case.

The full notification gate has two independent parts, and both are required:

1. **Admission**: `self._admission.allows(outcome.stream)` must be `True`.
   A stream that has not yet completed startup admission (or has since been
   marked failed/not-admitted) returns from `on_outcome` before the
   `COMMITTED` check is even reached — no notification is enqueued for it
   regardless of classification. This is the same early-return every other
   `on_outcome` side effect (supervisor bookkeeping, recovery-signal enqueue)
   already relies on; the notifier adds no separate admission check of its
   own.
2. **Classification**: `outcome.classification is
   RealtimeIngestionClassification.COMMITTED`, which is what makes the
   historical/backfill/repair/import/recovery exclusion correct — via the
   call-graph separation described above, not via admission.

An outcome must pass both to enqueue a notification. Admission answers "is
this stream currently live and ready," classification answers "was this
specific outcome a genuine new commit" — neither alone is the complete gate.

## Payload

```json
{
  "instrument": "BTCUSDT.P",
  "timeframe": "1m",
  "open_time_ms": 1735689600000
}
```

`instrument` uses `StreamKey.instrument.canonical_id`
(`domain/identity.py:34`), `timeframe` uses the canonical short id from
`domain/timeframes.py` (already normalized on `StreamKey` construction), and
`open_time_ms` is the plain `int` already carried on `RealtimeIngestionOutcome`
(`application/realtime/outcomes.py:22`). No conversion or additional lookup
is required to build the payload; every field is already present on the
outcome plus its stream.

## Queue overflow behavior

`queue.put_nowait(...)` raising `asyncio.QueueFull` is caught at the
enqueue call site inside the worker's `enqueue(...)` method. On overflow:

- the notification is dropped (not retried, not requeued elsewhere);
- one `ERROR` (or `CRITICAL`, matching existing severity conventions used
  elsewhere in `runtime/`) log line is emitted containing `instrument`,
  `timeframe`, `open_time_ms`, and the configured queue capacity;
- `RealtimeCandleHandler`/`IngestObservedCandle`/the SQLite commit are
  **never** touched by this failure — overflow is detected strictly after
  the candle is already durably committed, so there is nothing to roll back;
- processing of the *next* outcome (a different stream's commit, or a
  different classification) is unaffected — overflow on one notification
  does not block or fail the realtime pipeline in any way.

## HTTP success/failure semantics

The adapter calls exactly
`POST {strategy_runtime_base_url}/v1/webhooks/closed-bar` with the JSON
payload above and the configured timeout. "Success" is defined narrowly: an
HTTP `200` response whose body parses as JSON and matches
`{"status": "accepted"}` (the exact contract documented in
`strategy_runtime`'s `http-closed-bar` capability). Anything else —
connection refused, DNS failure, timeout, any non-200 status (including
Runtime's own `503` queue-full or not-ready response), a 200 with an
unparsable or unexpected body — is treated identically: log at `WARNING`
with `instrument`/`timeframe`/`open_time_ms`/error detail, and drop the
notification. There is no retry queue, no dead-letter store, and no
backoff — once the offloaded `asyncio.to_thread(self._notifier.send, item)`
call returns (or raises), the worker immediately proceeds to the next
queued item (or `queue.get()` blocks until one arrives).

A failed or timed-out delivery attempt SHALL NOT:

- mark the source stream not-ready or degraded (`RealtimeSupervisor`/
  `RuntimeLifecycleRecorder` state is untouched by notifier failures);
- roll back or re-classify the already-committed canonical candle;
- stop or slow processing of the next queued notification, nor of any other
  realtime pipeline work (candle ingestion, recovery, stale detection
  continue unaffected).

## Lifecycle

The worker is started and stopped exactly once, following the same
`stop_event`-cascading shape already used by `HistoricalReconciliationWorker`
and `RealtimeRecoveryWorker`:

```text
RuntimeService.run(stop_event)
  → _build_realtime(...) constructs CommittedBarNotificationWorker
    directly from RuntimeSettings/ValidatedMarketConfig already
    held by RuntimeService
  → added as a fourth task inside RuntimeRealtimeCoordinator.run's
    asyncio.TaskGroup (runtime/realtime.py:68), alongside
    _run_connector, _recovery_worker.run, _stale_worker
  → any task's exception or the group's own stop_event.set() unwinds
    every sibling task through the same TaskGroup/finally mechanics
    already in place today
```

Shutdown has two distinct cases, and only one of them is bounded by the
worker's idle-poll interval:

- **No send in flight**: the worker's `run(stop_event)` loop is blocked on
  `asyncio.wait_for(queue.get(), timeout=0.2)` (matching
  `RealtimeRecoveryWorker`'s polling idiom). It notices `stop_event` within
  that ~0.2s bound and exits without dequeuing anything further.
- **A send is currently in flight**: the worker is awaiting
  `asyncio.to_thread(self._notifier.send, item)` for the item it already
  dequeued. `stop_event` being set does **not** interrupt that call — a
  `threading.Event` (or `asyncio.Event`) cannot cancel a blocking
  `urllib`-based HTTP call already running on its own OS thread. Shutdown
  therefore waits for that offloaded call to return or raise, bounded by
  that notification's own configured `runtime_webhook_timeout_seconds`
  (the adapter's own timeout), not by the 0.2s polling interval. Once that
  call completes (success or failure, logged as normal), the worker does
  **not** start sending any further queued item — it proceeds directly to
  exiting the loop.

In both cases, every notification still sitting in the queue — never
dequeued, or dequeued-but-not-yet-started — is discarded at shutdown; the
worker never attempts to drain or flush the queue. This is a documented,
accepted Live V1 limitation, not a defect: it matches the "queue contents
are lost on crash/shutdown" property stated as fixed for this change, with
the added precision that a graceful shutdown's total bound is
`max(idle-poll interval, time for one in-flight send to finish or time out)`
— not a fixed small constant. Because `RuntimeRealtimeCoordinator.run`'s
`asyncio.TaskGroup` waits for every child task to finish before the group
itself completes, this bounded wait is a real (if bounded) contributor to
overall process shutdown time whenever a send happens to be in flight when
`stop_event` is set — it is bounded, not hanging, but it is not always
instantaneous either.

When `MDS_RUNTIME_WEBHOOK_ENABLED=false` (or unset, matching the documented
default), `RuntimeService._build_realtime` never constructs the notifier
adapter or worker, no queue is constructed, and no fourth task is added to
the `TaskGroup`. `on_outcome` still runs its admission/`COMMITTED` check but
the enqueue call is conditioned on the notifier worker reference being
non-`None` (the same `None`-gates-the-code-path pattern already used for
`process_committed_bar`/`process_first_fill` on the Runtime side). Production
MDS behavior for every other subsystem is byte-for-byte identical to before
this change.

## Fail-fast configuration

Configuration policy is fixed as follows:

- `runtime_webhook_enabled` defaults to `False`. When it is `False`, none of
  the other three fields are validated or read for construction purposes —
  the notifier is simply never built (see "Lifecycle" above).
- `strategy_runtime_base_url` has **no default** (`""`, which is not a valid
  URL). When `runtime_webhook_enabled` is `True`, it is required and
  validated as a non-empty absolute `http`/`https` URL; missing or invalid
  raises `ValueError`.
- `runtime_webhook_timeout_seconds` defaults to `2.0`. When
  `runtime_webhook_enabled` is `True`, whatever value is in effect — the
  default, or an explicit `MDS_RUNTIME_WEBHOOK_TIMEOUT_SECONDS` override —
  is validated as a finite positive number. The default always passes this
  check; only an explicit invalid override can fail it.
- `runtime_webhook_queue_capacity` defaults to `256`. When
  `runtime_webhook_enabled` is `True`, whatever value is in effect is
  validated as a positive integer by `RuntimeSettings.__post_init__`, exactly
  like the three checks above, in the same constructor pass as every other
  existing setting.

These four checks all live in `RuntimeSettings.__post_init__` and raise
`ValueError` before any component is constructed, the same startup path an
invalid `http_port` or `log_level` already fails today.

### A fifth check that cannot live in `RuntimeSettings`

`RuntimeSettings` has no visibility into `ValidatedMarketConfig` — it is a
separate object with a separate constructor
(`RuntimeWiring.build(database, config, ...)`, `wiring.py:44-51`), and
`RuntimeSettings.__post_init__` validates only its own fields. But a
`runtime_webhook_queue_capacity` smaller than the number of configured
enabled streams would let the very first shared-boundary burst (midnight
UTC, worst case: one `COMMITTED` outcome per enabled stream within a short
window) overflow the queue before the single sender has any realistic
chance to drain it — silently degrading the notifier to "usually drops most
of every boundary burst" rather than "handles the realistic worst case with
headroom."

This check is therefore performed once, at composition time, in
`RuntimeService` — the one place that already holds both `RuntimeSettings`
and `ValidatedMarketConfig` (`service.py:44-45`) — immediately before the
notifier worker is constructed in `_build_realtime`:

```text
if settings.runtime_webhook_enabled:
    if settings.runtime_webhook_queue_capacity < len(config.enabled_streams):
        raise ValueError(
            "MDS_RUNTIME_WEBHOOK_QUEUE_CAPACITY must be at least the "
            "number of enabled streams"
        )
```

A `ValueError` from this check fails startup exactly like any
`RuntimeSettings.__post_init__` violation — there is no "partially ready"
composition where the notifier is half-built and every other subsystem
starts anyway.

## Single-process limitation

Live V1 supports exactly one MDS process. The queue and its single consumer
task are process-local `asyncio` primitives with no cross-process
coordination; running multiple MDS replicas against the same Runtime would
each independently attempt delivery for their own observed commits with no
deduplication between processes. This mirrors the equivalent constraint
already documented on the Runtime side
(`strategy-instance-keyed-coordination`: "Live V1 coordination makes no
cross-process guarantee") and is treated as an out-of-scope, accepted
Live V1 boundary rather than a defect of this change.

## No-retry limitation

Exactly one HTTP attempt is made per notification. This is a deliberate V1
simplification, not an oversight: MDS has no way to know whether Runtime's
non-durable in-memory queue (see the companion Runtime change) would even
benefit from a retried delivery landing seconds later — by the time a retry
could plausibly succeed, Runtime may already be evaluating a subsequent
committed bar for the same strategy instance, so a stale retry would not be
strictly more useful than silence. Automatic retry is intentionally deferred
to a future change, if a real production gap is later observed to justify
it.

## Findings: cross-timeframe (HTF) consistency — confirmed, not a dependency

This change's scope is limited to *whether and how* MDS notifies Runtime
after a commit. From the MDS side, each configured `ticker × timeframe`
stream is ingested and classified fully independently (confirmed:
`RealtimeSupervisor` and `RuntimeLifecycleRecorder` track per-`StreamKey`
state with no cross-stream sequencing), so MDS has no mechanism — and this
change introduces none — to hold back or reorder a lower-timeframe
notification relative to a co-closing higher-timeframe commit.

The Strategy Engine repository has since been inspected directly (as part
of the companion `strategy_runtime` change's correction pass) to determine
whether that matters. `LoadLiveFeatureFrame` constructs exactly one
`MarketStream(ticker, base_timeframe)` and calls `EvaluateIndicatorRange`,
which performs exactly one `MarketDataPort.load_range(request.market,
requested_range)` call before handing the already-loaded `MarketFrame` to
the evaluator; the evaluator itself has no `MarketDataPort` dependency in
this use case. This confirms that live-entry evaluation reads exactly one
base-timeframe stream through one load call and does not read independently
committed 1h/4h/1d MDS streams at evaluation time.

The arrival order of independently committed higher-timeframe MDS streams
relative to a lower-timeframe commit is therefore **not** a correctness
dependency of the current Engine live-entry path. This change introduces no
MDS cross-timeframe barrier, fixed sleep, or boundary coordinator, and no
follow-up cross-timeframe-readiness change is proposed from either this
change or its Runtime companion — there is no confirmed gap for one to
close.
