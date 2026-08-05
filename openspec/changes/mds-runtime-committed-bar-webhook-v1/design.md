# Design: MDS → Strategy Runtime Committed-Bar Webhook v1

## Module decomposition

This is the module layout as actually implemented, not the earlier sketch —
see "Deviations from the original sketch" immediately below for what changed
and why.

```text
ports/
  committed_bar_notifier.py           CommittedBarNotification value object
                                       AND CommittedBarNotifier Protocol
                                       (both co-located here — see below)

adapters/http/
  committed_bar_notifier.py           HttpCommittedBarNotifier adapter
                                       (implements CommittedBarNotifier)

runtime/
  committed_bar_notification.py       CommittedBarNotificationWorker only —
                                       no value object lives here
  committed_bar_notification_factory.py
                                       build_committed_bar_notifier_worker(
                                       settings, config) -> Worker | None;
                                       owns the composition-time
                                       capacity-vs-enabled-streams check
  settings.py                         + 4 new validated fields, enabled
                                       -first environment parsing (existing
                                       file)
  service.py                          _build_realtime calls the factory
                                       function; no notifier construction
                                       logic lives in RuntimeService itself
                                       (existing file)
  realtime.py                         on_outcome gains the enqueue call;
                                       RuntimeRealtimeCoordinator receives
                                       an already-constructed optional
                                       worker reference — it does not
                                       construct one itself (existing file)
```

### Deviations from the original sketch, and why

**The `CommittedBarNotification` dataclass is co-located with
`CommittedBarNotifier` in `ports/committed_bar_notifier.py`**, not split
into `runtime/committed_bar_notification.py` as first sketched. That split
would have created a two-module import cycle: the port needs the dataclass
type for `send(self, notification: CommittedBarNotification) -> None`, and
the worker needs the Protocol type for its own `notifier: CommittedBarNotifier`
constructor parameter — whichever module the dataclass lived in, the other
module would need to import back from it. Co-locating both in `ports/`
(the lower layer `runtime/` already depends on one-directionally, exactly
like every other port in this codebase) resolves the cycle without weakening
either contract: `ports/committed_bar_notifier.py` has no dependency on
`runtime/` at all, and `runtime/committed_bar_notification.py` depends only
on `ports/`, never the reverse. This is the same one-directional dependency
shape `ports/market_data_source.py` → `domain/` already establishes
elsewhere in this codebase; it is not a new architectural pattern.

**Composition-time construction (the enabled check, the capacity-vs
-enabled-streams fail-fast check, and building the adapter + worker) lives
in a new `runtime/committed_bar_notification_factory.py` module** — a
`build_committed_bar_notifier_worker(settings, config) -> CommittedBarNotificationWorker | None`
function — rather than as a private method on `RuntimeService` as first
sketched. Reason: inlining this logic into `RuntimeService._build_realtime`
pushed `service.py` to 218 of the 220-line limit `tests/
test_architecture_baseline.py::test_python_modules_remain_laconic` enforces
across every module in this package; extracting one cohesive, independently
testable function (already this codebase's established response to a module
approaching that limit — see the realtime subsystem's own multi-module
split) was preferred over raising the limit or cramming the check into an
already-large composition method. `RuntimeService._build_realtime` now
contains exactly one line calling this function — it holds no composition
logic of its own, only the call site.

`ports/committed_bar_notifier.py` mirrors `ports/market_data_source.py`: a
plain `typing.Protocol` plus its value type, no framework dependency,
self-contained:

```python
@dataclass(frozen=True, slots=True)
class CommittedBarNotification:
    instrument: str
    timeframe: str
    open_time_ms: int


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
  process by `build_committed_bar_notifier_worker(settings, config)`
  (`runtime/committed_bar_notification_factory.py`), called from
  `RuntimeService._build_realtime` with the `RuntimeSettings` and
  `ValidatedMarketConfig` that `RuntimeService` already holds. This is not a
  `RuntimeWiring` factory method: `RuntimeWiring` does not currently own
  `RuntimeSettings` (it owns `database`, `config`, `rest_source`, `clock` —
  see `wiring.py:38-42`), and extending its constructor to also accept
  `RuntimeSettings` purely to support one optional component was considered
  and rejected — `RuntimeWiring` composes already-validated pieces from data
  it already has, and every one of its existing factory methods
  (`candle_handler()`, `recovery()`, etc.) needs no configuration beyond
  what it already holds. The worker is the single consumer of the queue and
  owns exactly one `HttpCommittedBarNotifier` instance for its process
  lifetime.
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
of the call. Instead, the worker offloads each send to a dedicated
`asyncio.Task`, shielded so that cancelling the *awaiting* coroutine cannot
abandon the underlying OS thread:

```python
async def run(self, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        try:
            notification = await asyncio.wait_for(self._queue.get(), timeout=0.2)
        except TimeoutError:
            continue
        # Atomic re-check: no await between this line and creating
        # send_task below, so a stop_event set concurrently on this same
        # loop is either seen here (item discarded, unstarted) or not yet
        # visible at all (item proceeds) — never half-observed.
        if stop_event.is_set():
            self._queue.task_done()
            return
        await self._send(notification)

async def _send(self, notification: CommittedBarNotification) -> None:
    send_task = asyncio.ensure_future(
        asyncio.to_thread(self._notifier.send, notification)
    )
    try:
        try:
            await asyncio.shield(send_task)
        except asyncio.CancelledError:
            # This coroutine was cancelled, not send_task: shield() kept
            # send_task running. Wait for the in-flight thread rather than
            # abandoning it, then re-raise so the worker still stops.
            await self._await_send_completion(notification, send_task)
            raise
        except Exception as exc:
            self._log_delivery_failure(notification, exc)
    finally:
        self._queue.task_done()
```

`await asyncio.to_thread(...)` yields control of the event loop back to the
scheduler for the duration of the blocking call, so other tasks (the
connector's receive loop, the recovery worker, the stale worker) continue to
run normally while one notification's HTTP send is in progress on its own
thread. The worker still awaits that call's completion before dequeuing the
next item — offloading to a thread changes *where* the blocking happens, not
*whether* sends remain strictly sequential (see "Concurrency limits" below).

### Why the send is shielded, not awaited directly

If `_send` awaited `send_task` directly (no `shield`), cancelling the
worker's `run(stop_event)` task — which happens whenever a sibling task
inside `RuntimeRealtimeCoordinator.run`'s `asyncio.TaskGroup` raises, not
only on ordinary shutdown — would propagate that cancellation straight into
`send_task` too, and asyncio would consider the underlying `to_thread` call
"done" from the coroutine's perspective the moment the `CancelledError` is
raised, even though the OS thread executing `self._notifier.send(...)`
keeps running to completion in the background, unobserved. Any exception
that call raises would be lost, `task_done()` could be skipped or
double-counted depending on exactly where the cancellation landed, and the
TaskGroup could proceed to close resources (in the companion Runtime
change's shutdown ordering) while this orphaned thread was still mid-flight.
`asyncio.shield(send_task)` prevents the *external* cancellation from
touching `send_task` itself: only the `await` in `_send` raises
`CancelledError`, `send_task` keeps running, and `_send` explicitly awaits
it to completion (logging any error, exactly like the non-cancelled failure
path) before re-raising — so the worker still stops promptly, but never
abandons an in-flight HTTP thread. `finally: self._queue.task_done()`
guarantees exactly one `task_done()` call per dequeued item regardless of
which of the three outcomes (success, logged failure, cancellation) occurs.

## Concurrency limits

```text
max concurrent outbound committed-bar HTTP calls from MDS = 1
```

This is a direct consequence of there being exactly one
`CommittedBarNotificationWorker.run(stop_event)` task, which processes its
queue with the sequential loop shown above (dequeue, atomic stop-check,
`await self._send(notification)`), never `asyncio.gather` or a task pool.
The next queued item is not dequeued until `_send` has returned — which,
per the shielded design above, only happens after the offloaded
`asyncio.to_thread(...)` call has itself fully completed, whether that
completion arrives via ordinary return, a logged failure, or the
cancellation-safe wait — this is what makes "one MDS HTTP sender" true even
though enqueue can be called concurrently from multiple in-flight
`on_outcome` invocations, and even though the send itself runs on a worker
thread rather than the event-loop thread (offloading to a thread bounds
*where* the blocking I/O happens, not *how many* sends can be in flight).

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
  → _build_realtime(...) calls
    build_committed_bar_notifier_worker(settings, config)
    (runtime/committed_bar_notification_factory.py) to construct the
    optional CommittedBarNotificationWorker
  → added as a fourth task inside RuntimeRealtimeCoordinator.run's
    asyncio.TaskGroup (runtime/realtime.py:68), alongside
    _run_connector, _recovery_worker.run, _stale_worker
  → any task's exception (including this worker's own, and including any
    sibling task's) or the group's own stop_event.set() unwinds every
    sibling task through the same TaskGroup/finally mechanics already in
    place today
```

The worker's task can stop for two different reasons, and they are handled
by two different mechanisms:

- **Ordinary shutdown** (`stop_event.set()`, no exception): the worker's own
  loop notices this cooperatively — see the three cases below.
- **A sibling task in the same `TaskGroup` raises** (e.g. the recovery
  worker's `ExplodingRecovery`-style failure): `asyncio.TaskGroup` cancels
  every other task in the group directly, including the notifier worker's
  task, regardless of `stop_event`. This is why the cancellation-safety
  described under "Non-blocking event loop" exists as a distinct mechanism
  from the `stop_event` check — a `Task.cancel()` can arrive at any `await`
  point, not only at the top of the loop.

Within the worker's own loop, there are three distinct shutdown-adjacent
cases:

- **Idle, no item dequeued**: the loop is blocked on
  `asyncio.wait_for(queue.get(), timeout=0.2)` (matching
  `RealtimeRecoveryWorker`'s polling idiom). It notices `stop_event` within
  that ~0.2s bound on the next loop iteration and exits without dequeuing
  anything further.
- **An item was just dequeued but its send has not started**: the atomic
  `if stop_event.is_set(): self._queue.task_done(); return` check (see
  "Non-blocking event loop") fires before `_send` is ever called. The item
  is discarded — `notifier.send(...)` is never invoked for it — and the
  worker exits immediately, without waiting on anything.
- **A send is already in flight** (`_send` already past that check,
  awaiting the shielded `send_task`): whether triggered by `stop_event`
  being observed on the *next* loop iteration after this send completes, or
  by a direct `Task.cancel()` from a failing sibling, the in-flight
  `asyncio.to_thread(self._notifier.send, item)` call is never abandoned.
  For the `stop_event` case, the worker simply finishes this send normally
  (success or logged failure) and then exits on the next iteration without
  starting another. For the `Task.cancel()` case, `_send`'s shielded
  `await` raises `CancelledError`, the worker awaits `send_task`'s actual
  completion (logging any error), and only then re-raises — so the group's
  unwind is delayed exactly as long as this one HTTP thread takes, never
  longer, and never abandoned mid-flight.

In every case, any notification still sitting in the queue — never
dequeued, or dequeued-but-discarded-before-send — is lost at shutdown; the
worker never attempts to drain or flush the queue. This is a documented,
accepted Live V1 limitation, not a defect: it matches the "queue contents
are lost on crash/shutdown" property stated as fixed for this change. The
graceful-shutdown wait is bounded by whichever one send happened to be
in-flight (its own configured `runtime_webhook_timeout_seconds` bounds the
network portion) — not by a fixed small constant, and not indefinitely,
since exactly one item is ever waited on, never a queue's worth.

When `MDS_RUNTIME_WEBHOOK_ENABLED=false` (or unset, matching the documented
default), `build_committed_bar_notifier_worker` returns `None` without
constructing the notifier adapter or worker, no queue is constructed, and
no fourth task is added to the `TaskGroup`. `on_outcome` still runs its
admission/`COMMITTED` check but the enqueue call is conditioned on the
notifier worker reference being non-`None` (the same `None`-gates-the-code
-path pattern already used for `process_committed_bar`/`process_first_fill`
on the Runtime side). Production MDS behavior for every other subsystem is
byte-for-byte identical to before this change.

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

### `from_environment()` reads `enabled` first and skips the rest when disabled

`__post_init__`'s checks above only run against whatever values
`RuntimeSettings` was actually constructed with — they say nothing about
*how* `from_environment()` gets those values out of `os.environ`. The
naive approach — parse all four `MDS_RUNTIME_WEBHOOK_*`/`MDS_STRATEGY_
RUNTIME_*` variables unconditionally, then let `__post_init__` decide
whether to validate them — has a real bug: `float(env.get(
"MDS_RUNTIME_WEBHOOK_TIMEOUT_SECONDS", "2.0"))` raises immediately if that
variable holds a non-numeric string, *even when the feature is disabled and
the field is irrelevant*. A stale or malformed leftover value for a
disabled feature must not fail startup.

`from_environment()` therefore determines `MDS_RUNTIME_WEBHOOK_ENABLED`
first, via a small `_parse_committed_bar_webhook_environment(env)` helper.
When it resolves to `False`, the other three variables
(`MDS_STRATEGY_RUNTIME_BASE_URL`, `MDS_RUNTIME_WEBHOOK_TIMEOUT_SECONDS`,
`MDS_RUNTIME_WEBHOOK_QUEUE_CAPACITY`) are **never read or parsed** — the
helper returns the fixed internal defaults (`""`, `2.0`, `256`) directly,
regardless of what (if anything) those variables hold. Only when `enabled`
resolves to `True` are the other three actually read from the environment
and parsed (`float(...)`/`int(...)`, which can still raise `ValueError` for
a malformed *enabled* configuration — that failure mode is unchanged and
still correct: a present, enabled, invalid value must fail startup). This
mirrors, at the environment-parsing layer, the same "absent uses default,
present-and-invalid fails" distinction `__post_init__` already enforces at
the validation layer.

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

This check is therefore performed once, at composition time, inside
`build_committed_bar_notifier_worker(settings, config)`
(`runtime/committed_bar_notification_factory.py`) — the one function that
receives both `RuntimeSettings` and `ValidatedMarketConfig` — immediately
before constructing the adapter and worker, and before
`RuntimeService._build_realtime` (which calls this function) ever sees a
return value:

```text
if not settings.runtime_webhook_enabled:
    return None
if settings.runtime_webhook_queue_capacity < len(config.enabled_streams):
    raise ValueError(
        "MDS_RUNTIME_WEBHOOK_QUEUE_CAPACITY must be at least the "
        "number of enabled streams"
    )
```

A `ValueError` from this check fails startup exactly like any
`RuntimeSettings.__post_init__` violation — there is no "partially ready"
composition where the notifier is half-built and every other subsystem
starts anyway. This function is unit-tested directly (constructing
`RuntimeSettings`/`ValidatedMarketConfig` pairs and asserting `None`,
a constructed worker, or a raised `ValueError`), independent of the full
`RuntimeService` composition.

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
