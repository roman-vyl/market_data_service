# Design: MDS → Strategy Runtime Committed-Bar Webhook v1

## Module layout

```text
ports/committed_bar_notifier.py           CommittedBarNotification (frozen
                                           dataclass) + CommittedBarNotifier
                                           (Protocol), co-located
adapters/http/committed_bar_notifier.py   HttpCommittedBarNotifier
runtime/committed_bar_notification.py     CommittedBarNotificationWorker
runtime/committed_bar_notification_factory.py
                                           build_committed_bar_notifier_worker(
                                           settings, config) -> Worker | None
runtime/settings.py                       + 4 new validated fields (existing)
runtime/service.py                        _build_realtime calls the factory
                                           (existing)
runtime/realtime.py                       on_outcome enqueues; run() adds the
                                           worker's task (existing)
```

The notification dataclass and its Protocol are co-located in `ports/`
rather than split, to avoid a two-module import cycle between the port and
the worker. Composition-time construction (enabled check, the
capacity-vs-enabled-streams check, adapter + worker construction) lives in
its own factory module rather than inline in `RuntimeService`, keeping
`service.py` under the architecture's 220-line-per-module limit.

## Ownership and lifetime

- **Queue**: one bounded `asyncio.Queue[CommittedBarNotification]`
  (`maxsize=runtime_webhook_queue_capacity`), owned exclusively by the
  worker.
- **Worker**: constructed once per process by
  `build_committed_bar_notifier_worker`, called from
  `RuntimeService._build_realtime`. Not owned by `RuntimeWiring`, which has
  no `RuntimeSettings` dependency and does not gain one for this.
- **Producer**: `RuntimeRealtimeCoordinator.on_outcome` calls `enqueue(...)`
  (non-blocking `put_nowait`, `QueueFull` handled locally) — never a
  blocking `put(...)`.
- **HTTP client**: one adapter instance, constructed once and reused for
  every call. Built on stdlib `urllib.request`; does not guarantee
  connection reuse across calls, only that the same configured
  `base_url`/`timeout_seconds` is reused.

## Non-blocking event loop

`CommittedBarNotifier.send(...)` is a plain synchronous, blocking call. The
worker never calls it directly on the event-loop thread — every send is
offloaded via `asyncio.to_thread(...)` and shielded from external
cancellation, so a cancelled `run()` task (e.g. a sibling `TaskGroup` member
failing) can never abandon an in-flight OS thread mid-send. The shielding
survives any number of repeated cancellations arriving while the worker is
already waiting on one in-flight send: an external cancellation is caught,
remembered, and re-raised only after the send has genuinely finished and its
outcome (success or logged failure) has been observed. This guarantees
`task_done()` is called exactly once per dequeued item, no delivery outcome
is ever silently dropped, and exactly one outbound call is in flight at any
instant.

## Enqueue gate

`on_outcome` enqueues if and only if the outcome's stream is currently
admitted (`RealtimeAdmissionGate.allows(...)`) **and**
`classification is COMMITTED`. This is the only place a notification is
ever produced: `RealtimeIngestionOutcome` is constructed exclusively by
`RealtimeCandleHandler.handle`, called exclusively from the live WebSocket
receive loop — historical bootstrap, backfill, gap repair, import, and
realtime recovery all write through `IngestObservedCandle` directly and
never construct an outcome or call `on_outcome`. The notifier hook SHALL
live only at `on_outcome`, never duplicated elsewhere.

## Payload

Exactly `{"instrument": <canonical id>, "timeframe": <canonical short id>,
"open_time_ms": <int>}`, taken directly from the outcome and its stream —
no conversion or additional lookup required.

## Queue overflow and delivery failure

Overflow (`QueueFull`) drops the notification, logs one `ERROR` line with
`instrument`/`timeframe`/`open_time_ms`/capacity, and never touches the
already-committed candle or the next queued item. A delivery attempt
succeeds only on HTTP `200` with body `{"status": "accepted"}`; anything
else (timeout, transport failure, non-200, malformed body) is logged at
`WARNING` and dropped — no retry, no dead-letter store, no backoff, and no
effect on stream readiness or canonical history.

## Lifecycle

The worker starts and stops exactly once, as a fourth task inside
`RuntimeRealtimeCoordinator.run`'s existing `asyncio.TaskGroup`. It stops
for two distinct reasons: ordinary shutdown (`stop_event.set()`, noticed
cooperatively within its idle-poll bound) and a sibling task's failure
(`TaskGroup` cancels every task directly, regardless of `stop_event`). An
in-flight send is never abandoned in either case — shutdown while idle
exits within the idle-poll bound; shutdown or cancellation while a send is
in flight waits for that one send, bounded by its own configured timeout,
and no further queued item is started. Any notification still sitting in
the queue at shutdown is discarded, never drained. When disabled
(`MDS_RUNTIME_WEBHOOK_ENABLED=false`, the default), no queue, worker,
adapter, or task is constructed — every other MDS subsystem is unaffected.

## Configuration

`runtime_webhook_enabled` defaults `False`; when `False`, none of the other
three fields are read or validated. `strategy_runtime_base_url` has no
default — required and validated as a non-empty absolute `http`/`https` URL
only when enabled. `runtime_webhook_timeout_seconds` (default `2.0`) and
`runtime_webhook_queue_capacity` (default `256`) are validated only when
present and the feature is enabled; absence never fails startup, an
invalid explicit value always does. `from_environment()` reads `enabled`
first and skips parsing the other three variables entirely when it
resolves `False`, so a stale or malformed leftover value for a disabled
feature cannot fail startup. A fifth check — queue capacity at least the
number of enabled configured streams — cannot live in `RuntimeSettings` (no
visibility into `ValidatedMarketConfig`) and instead runs once, at
composition time, inside `build_committed_bar_notifier_worker`. Every one of
these checks raises `ValueError` before any component is constructed; there
is no partially-built composition.

## Single-process, no-retry limitations

Live V1 supports exactly one MDS process; the queue and its consumer are
process-local with no cross-process coordination or deduplication (mirrors
the equivalent Runtime-side constraint). Exactly one HTTP attempt is made
per notification — by the time a retry could land, Runtime may already be
evaluating a subsequent bar for the same instance, so a stale retry is not
obviously more useful than silence. Both are deliberate V1 simplifications,
deferred to a future change if a real production gap justifies revisiting
them.

## Cross-timeframe ordering: confirmed not a dependency

Each configured stream is ingested and classified independently, with no
cross-stream sequencing; this change introduces no mechanism to hold back
or reorder a lower-timeframe notification relative to a co-closing
higher-timeframe commit. Direct inspection of the Strategy Engine's
live-entry path (`LoadLiveFeatureFrame` → `EvaluateIndicatorRange`)
confirmed it reads exactly one base-timeframe stream through one load call
and does not read independently-committed higher-timeframe MDS streams at
evaluation time — so this arrival-order question is not a correctness
dependency of the current Engine path, and no cross-timeframe barrier or
follow-up change is introduced or proposed.
