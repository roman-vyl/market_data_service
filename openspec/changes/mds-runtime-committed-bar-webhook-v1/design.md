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
  wiring.py                      + notifier() factory (existing file)
  service.py                     + worker construction and TaskGroup wiring
                                  (existing file)
  realtime.py                    on_outcome gains the enqueue call
                                  (existing file)
```

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
  process by `RuntimeWiring.notifier()` (mirroring `wiring.py`'s existing
  `candle_handler()`/`recovery()` factories) and is the single consumer of
  the queue. It owns exactly one `HttpCommittedBarNotifier` instance for its
  process lifetime.
- **Producer**: `RuntimeRealtimeCoordinator.on_outcome` calls
  `await self._notifier_worker.enqueue(outcome)` — a non-blocking
  `queue.put_nowait(...)` wrapped with `QueueFull` handling, never
  `queue.put(...)` (which would block the caller and, transitively, the
  connector's receive loop).
- **HTTP client**: one client object (a small `urllib`-based POST helper, or
  a single reused connection if the implementation later adopts an async
  HTTP library) constructed once by the adapter and reused for every
  outbound call — never per-notification. This mirrors `BybitRestCandleSource`
  reusing its transport rather than reconnecting per request.

## Concurrency limits

```text
max concurrent outbound committed-bar HTTP calls from MDS = 1
```

This is a direct consequence of there being exactly one
`CommittedBarNotificationWorker.run(stop_event)` task, which processes its
queue with a plain sequential loop (`while not stop_event.is_set(): item =
await queue.get(); await self._send(item)`), never `asyncio.gather` or a
task pool. The next queued item is not dequeued until the previous `send`
call has returned (success or failure) — this is what makes "one MDS HTTP
sender" true even though enqueue can be called concurrently from multiple
in-flight `on_outcome` invocations.

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

Admission gating (`self._admission.allows(outcome.stream)`) is applied
before the notification check for consistency with every other `on_outcome`
side effect, but it is not what makes the historical exclusion correct — the
call-graph separation above is.

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
backoff — the worker immediately proceeds to the next queued item (or
`queue.get()` blocks until one arrives).

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
  → added as a fourth task inside RuntimeRealtimeCoordinator.run's
    asyncio.TaskGroup (runtime/realtime.py:68), alongside
    _run_connector, _recovery_worker.run, _stale_worker
  → any task's exception or the group's own stop_event.set() unwinds
    every sibling task through the same TaskGroup/finally mechanics
    already in place today
```

On shutdown, the worker's `run(stop_event)` loop exits its
`while not stop_event.is_set()` loop promptly (bounded `asyncio.wait_for`
polling on `queue.get()`, matching `RealtimeRecoveryWorker`'s
`timeout=0.2` idiom) — it does not attempt to drain or flush remaining
queued notifications before exiting. Any notification still sitting in the
queue at shutdown is discarded. This is a documented, accepted Live V1
limitation, not a defect: it matches the "queue contents are lost on
crash/shutdown" property stated as fixed for this change.

When `MDS_RUNTIME_WEBHOOK_ENABLED=false` (or unset, matching the documented
default), `RuntimeWiring.notifier()` is never called, no queue is
constructed, no `HttpCommittedBarNotifier` is constructed, and no fourth
task is added to the `TaskGroup`. `on_outcome` still runs its `COMMITTED`
check but calls into a no-op (the enqueue call is conditioned on the
notifier component's presence, exactly like `process_committed_bar`/
`process_first_fill` being `None` gates the equivalent code path on the
Runtime side). Production MDS behavior for every other subsystem is
byte-for-byte identical to before this change.

## Fail-fast configuration

When `MDS_RUNTIME_WEBHOOK_ENABLED=true`, `RuntimeSettings.__post_init__`
SHALL validate (in the same constructor pass as every other setting, so a
misconfiguration raises `ValueError` before any component is constructed):

- `strategy_runtime_base_url` is non-empty and looks like an absolute
  `http`/`https` URL;
- `runtime_webhook_timeout_seconds` is a finite positive number;
- `runtime_webhook_queue_capacity` is a positive integer.

A `ValueError` here fails the same startup path that an invalid
`http_port` or `log_level` already fails today — there is no
"partially ready" composition where the notifier is half-built and every
other subsystem starts anyway.

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

## Findings: cross-timeframe (HTF) consistency

This change's scope is limited to *whether and how* MDS notifies Runtime
after a commit — it makes no claim about what Runtime does with multiple
near-simultaneous notifications for the same instrument at different
timeframes (e.g. `BTCUSDT.P:5m` and `BTCUSDT.P:1h` both closing within the
same second at an hour boundary). From the MDS side, each configured
`ticker × timeframe` stream is ingested and classified fully independently
(confirmed: `RealtimeSupervisor` and `RuntimeLifecycleRecorder` track
per-`StreamKey` state with no cross-stream sequencing), so MDS has no
mechanism — and this change introduces none — to hold back or reorder a
lower-timeframe notification relative to a co-closing higher-timeframe
commit.

Whether that matters depends entirely on whether the Strategy Engine (a
separate service, outside both `market_data_service` and `strategy_runtime`)
derives higher-timeframe features internally from a single base-timeframe
candle stream, or instead depends at evaluation time on independently
-committed HTF MDS streams. `strategy_runtime`'s own exploration for the
companion change found that `strategy_runtime` itself cannot answer this —
it is a pure HTTP client of the Engine and carries only one
`base_timeframe`/`target_bar_open_time_ms` pair per request. This change
does **not** add any cross-timeframe barrier, fixed sleep, or boundary
coordinator to work around the unknown — doing so here would be
speculative and would encode a correctness assumption about a system this
change cannot see. If a real correctness gap is later confirmed in the
Engine repository, it should be addressed as its own explicitly scoped
change (proposed name: `engine-cross-timeframe-readiness-v1` or similar),
not folded into either the MDS notifier or the Runtime intake queue.
