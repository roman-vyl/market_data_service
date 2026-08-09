# mds-runtime-committed-bar-webhook-v1 Specification

## Purpose
Defines optional best-effort delivery of notifications for newly committed admitted realtime candles from MDS to Strategy Runtime, without coupling notification delivery to canonical ingestion or readiness.
## Requirements
### Requirement: Only an admitted stream's genuine new normal realtime commit enqueues a notification

Market Data Service SHALL enqueue exactly one `CommittedBarNotification` for
a `RealtimeIngestionOutcome` if and only if **both**: the outcome's stream is
currently admitted (`RealtimeAdmissionGate.allows(outcome.stream)` is
`True`), and the outcome's classification is `COMMITTED` and was produced by
the live WebSocket realtime path delivered to
`RuntimeRealtimeCoordinator.on_outcome`. Neither condition alone is
sufficient.

#### Scenario: Normal confirmed candle commit on an admitted stream enqueues once

- **WHEN** a confirmed realtime candle close on a currently admitted stream
  is ingested and classified `COMMITTED`
- **THEN** exactly one `CommittedBarNotification` is enqueued
- **AND** it carries the exact `instrument`, `timeframe`, and `open_time_ms`
  of the committed candle

#### Scenario: A not-yet-admitted stream's outcome enqueues nothing

- **WHEN** `RealtimeAdmissionGate.allows(outcome.stream)` is `False` for the
  outcome's stream (for example, before that stream has completed startup
  admission)
- **THEN** zero notifications are enqueued for that outcome, regardless of
  its classification

#### Scenario: Non-committed realtime classifications enqueue nothing

- **WHEN** a realtime outcome for an admitted stream is classified
  `DUPLICATE`, `CORRECTED`, `REJECTED`, or `FAILED`
- **THEN** zero notifications are enqueued for that outcome

#### Scenario: Unconfirmed candle updates never reach the notification boundary

- **WHEN** a WebSocket candle update is not exchange-confirmed
- **THEN** it produces no `RealtimeIngestionOutcome`
- **AND** enqueues zero notifications

#### Scenario: Historical and recovery writes never enqueue a notification

- **WHEN** a canonical candle is written through historical bootstrap,
  backfill, gap repair, REST import, realtime recovery, or startup
  reconciliation
- **THEN** zero notifications are enqueued for that write
- **AND** this holds regardless of how many candles the operation writes or
  whether any of them would classify as `COMMITTED` if observed through the
  live realtime path

#### Scenario: Notification never precedes the canonical commit

- **WHEN** MDS enqueues a `CommittedBarNotification`
- **THEN** the corresponding canonical candle's unit-of-work has already
  committed
- **AND** no outbound network call for that notification occurs before that
  commit

### Requirement: Notification payload is limited to instrument, timeframe, and open time

A `CommittedBarNotification` sent to Strategy Runtime SHALL contain exactly
`instrument`, `timeframe`, and `open_time_ms`, and no other field.

#### Scenario: Payload shape

- **WHEN** MDS sends a committed-bar notification
- **THEN** the JSON body contains exactly the keys `instrument` (non-empty
  string), `timeframe` (non-empty string), and `open_time_ms` (non-negative
  JSON integer)
- **AND** it does not contain a strategy ID, deployment ID, OHLCV payload,
  indicator value, market-data hash, or trade-cycle ID

### Requirement: Ingestion does not wait for notification delivery

Enqueueing and sending a committed-bar notification SHALL NOT block or delay
realtime candle ingestion.

#### Scenario: Enqueue is non-blocking

- **WHEN** `on_outcome` enqueues a notification
- **THEN** it does not perform network I/O directly
- **AND** it returns without waiting for the notification to be delivered

#### Scenario: A blocking HTTP send does not stall the event loop

- **WHEN** the worker sends a queued notification
- **THEN** it offloads the notifier's synchronous `send(...)` call to a
  separate thread (`asyncio.to_thread(...)` or equivalent) rather than
  calling it directly on the event-loop thread
- **AND** while that offloaded call is in progress, other event-loop work
  (the WebSocket receive loop, other scheduled tasks) continues to run
  without waiting for it

#### Scenario: A full queue does not block or fail ingestion

- **WHEN** the notification queue is at its configured capacity when a new
  `COMMITTED` outcome arrives
- **THEN** the new notification is dropped
- **AND** the already-committed canonical candle is not rolled back,
  re-classified, or otherwise affected
- **AND** MDS emits one `ERROR`-level diagnostic containing the dropped
  notification's `instrument`, `timeframe`, `open_time_ms`, and the
  configured queue capacity
- **AND** no retry of the dropped notification occurs

### Requirement: At most one outbound committed-bar HTTP call is in flight

MDS SHALL maintain exactly one consumer of the notification queue, and that
consumer SHALL send notifications one at a time.

#### Scenario: Sequential delivery

- **WHEN** two or more notifications are queued
- **THEN** the second notification's HTTP attempt does not begin until the
  first notification's HTTP attempt has completed (success or failure)

#### Scenario: FIFO order under rapid multi-stream commits

- **WHEN** multiple independently configured streams commit candles in rapid
  succession, each successfully enqueuing a notification
- **THEN** notifications are delivered in the exact order they were
  successfully enqueued
- **AND** this requirement covers only the queue's own enqueue-to-delivery
  ordering — it makes no claim about, and does not require, any particular
  ordering of the underlying SQLite commits across independent streams

### Requirement: Delivery is best-effort with no retry

A committed-bar notification SHALL be attempted at most once over HTTP.

#### Scenario: Successful delivery

- **WHEN** the configured Strategy Runtime endpoint responds `200` with a
  body equal to `{"status": "accepted"}`
- **THEN** MDS treats the notification as delivered and proceeds to the next
  queued item

#### Scenario: Any non-success outcome is logged and dropped, not retried

- **WHEN** the HTTP attempt times out, fails at the transport level, returns
  a non-200 status, or returns a 200 response whose body does not parse as
  `{"status": "accepted"}`
- **THEN** MDS logs the failure with `instrument`, `timeframe`,
  `open_time_ms`, and error detail
- **AND** does not retry that notification
- **AND** proceeds to process the next queued notification without delay

#### Scenario: A delivery failure has no effect on stream state

- **WHEN** a committed-bar notification delivery fails
- **THEN** the affected stream's realtime readiness, lifecycle state, and
  canonical candle history remain unchanged

### Requirement: Notifier is disabled by default and fails closed when misconfigured

Market Data Service SHALL support the committed-bar notifier as an
explicitly enabled, validated component of `RuntimeSettings`.

#### Scenario: Disabled by default

- **WHEN** `MDS_RUNTIME_WEBHOOK_ENABLED` is unset or `false`
- **THEN** MDS constructs no notification queue, no worker, and no outbound
  HTTP client
- **AND** every other MDS subsystem behaves exactly as it did before this
  capability existed

#### Scenario: Enabled with valid configuration

- **WHEN** `MDS_RUNTIME_WEBHOOK_ENABLED=true`, `MDS_STRATEGY_RUNTIME_BASE_URL`
  is present and a valid absolute `http`/`https` URL,
  `MDS_RUNTIME_WEBHOOK_TIMEOUT_SECONDS` is either absent (the `2.0` default
  applies) or present with a finite positive value, and
  `MDS_RUNTIME_WEBHOOK_QUEUE_CAPACITY` is either absent (the `256` default
  applies) or present with a positive integer value that is, in either case,
  at least the number of enabled configured streams
- **THEN** MDS constructs the queue, worker, and HTTP adapter as part of the
  same production composition as every other runtime component

#### Scenario: An absent numeric field uses its documented default, not a startup failure

- **WHEN** `MDS_RUNTIME_WEBHOOK_ENABLED=true`, `MDS_STRATEGY_RUNTIME_BASE_URL`
  is present and valid, and `MDS_RUNTIME_WEBHOOK_TIMEOUT_SECONDS` and/or
  `MDS_RUNTIME_WEBHOOK_QUEUE_CAPACITY` are absent from the environment
- **THEN** `RuntimeSettings` construction does **not** fail on account of
  either absent field
- **AND** the absent field takes its documented default (`2.0` seconds for
  timeout, `256` for queue capacity) exactly as if it had been supplied with
  that value
- **AND** this is distinct from a *present but invalid* value for the same
  field, which does fail construction (see below)

#### Scenario: Enabled with a queue capacity smaller than the enabled-stream count fails startup

- **WHEN** `MDS_RUNTIME_WEBHOOK_ENABLED=true`, every field otherwise passes
  its own individual validation, and the queue capacity in effect (whether
  the `256` default or an explicit `MDS_RUNTIME_WEBHOOK_QUEUE_CAPACITY`) is
  smaller than the number of currently enabled configured streams
- **THEN** composition raises `ValueError` before the notifier worker is
  constructed
- **AND** no partially constructed composition is returned

#### Scenario: A missing or invalid base URL fails startup

- **WHEN** `MDS_RUNTIME_WEBHOOK_ENABLED=true` and
  `MDS_STRATEGY_RUNTIME_BASE_URL` is missing, empty, or not a valid absolute
  `http`/`https` URL
- **THEN** `RuntimeSettings` construction raises `ValueError` before any
  component is built
- **AND** no partially constructed composition (queue without worker, worker
  without HTTP adapter, or any other partial state) is ever returned
- **AND** this is the only field whose *absence* fails construction — the
  timeout and queue-capacity fields never fail construction merely by being
  absent

#### Scenario: An explicit invalid numeric override fails startup

- **WHEN** `MDS_RUNTIME_WEBHOOK_ENABLED=true` and
  `MDS_RUNTIME_WEBHOOK_TIMEOUT_SECONDS` is present but not finite and
  positive, or `MDS_RUNTIME_WEBHOOK_QUEUE_CAPACITY` is present but not a
  positive integer
- **THEN** `RuntimeSettings` construction raises `ValueError` before any
  component is built
- **AND** no partially constructed composition is ever returned

### Requirement: Worker has single-owner start/stop lifecycle; the adapter has none of its own

The notification worker SHALL be constructed, started, and stopped exactly
once per process, by the production runtime composition. The
`HttpCommittedBarNotifier` adapter SHALL be constructed exactly once per
process; it has no independent start, stop, or close operation of its own —
it is a plain object holding its configured `base_url`/`timeout_seconds`,
not a component with its own lifecycle.

#### Scenario: Start once

- **WHEN** the notifier is enabled and the runtime process starts
- **THEN** exactly one `CommittedBarNotificationWorker` and exactly one
  `HttpCommittedBarNotifier` instance are constructed
- **AND** the worker's run loop starts exactly once, as one task inside the
  existing realtime `TaskGroup`
- **AND** the adapter itself is never separately "started" — it has no
  start operation to call

#### Scenario: The adapter has no close operation

- **WHEN** the runtime process shuts down and the worker stops
- **THEN** no `close()` or equivalent shutdown call is made on the adapter
- **AND** the adapter becomes unreachable together with the worker that
  owned it, simply by going out of scope — this is sufficient, since the
  adapter holds no socket, file handle, or other resource that requires an
  explicit release beyond what `urllib.request.urlopen(...)` already
  releases per call

#### Scenario: Stop while idle exits within the idle-poll bound

- **WHEN** the runtime process shuts down (`stop_event` is set) while no
  notification send is currently in flight
- **THEN** the worker's run loop exits within its idle-poll bound (matching
  the existing recovery worker's polling interval)
- **AND** it does not attempt to drain or flush any notification still
  sitting in the queue

#### Scenario: Stop while a send is in flight waits for that send, not a fixed timeout

- **WHEN** the runtime process shuts down while one notification's HTTP send
  is already in flight (offloaded to a thread)
- **THEN** shutdown waits for that specific send to complete or reach its
  own configured `runtime_webhook_timeout_seconds` — not the worker's
  idle-poll interval, which does not bound an in-flight blocking call
- **AND** the worker does not begin sending any further queued notification
  once `stop_event` has been observed
- **AND** every notification still sitting in the queue at that point is
  discarded, not drained
- **AND** this bounded wait is a real, bounded contributor to overall
  process shutdown time when a send happens to be in flight — it is not
  claimed to be instantaneous

#### Scenario: No orphaned background worker

- **WHEN** the runtime process is not running (before start, or after clean
  shutdown)
- **THEN** no notification worker task is alive

