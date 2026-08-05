## ADDED Requirements

### Requirement: Only a genuine new normal realtime commit enqueues a notification

Market Data Service SHALL enqueue exactly one `CommittedBarNotification` for
a `RealtimeIngestionOutcome` if and only if that outcome's classification is
`COMMITTED` and the outcome was produced by the live WebSocket realtime path
delivered to `RuntimeRealtimeCoordinator.on_outcome`.

#### Scenario: Normal confirmed candle commit enqueues once

- **WHEN** a confirmed realtime candle close is ingested and classified
  `COMMITTED`
- **THEN** exactly one `CommittedBarNotification` is enqueued
- **AND** it carries the exact `instrument`, `timeframe`, and `open_time_ms`
  of the committed candle

#### Scenario: Non-committed realtime classifications enqueue nothing

- **WHEN** a realtime outcome is classified `DUPLICATE`, `CORRECTED`,
  `REJECTED`, or `FAILED`
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
  succession
- **THEN** notifications are enqueued and delivered in the same order their
  underlying commits completed

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

- **WHEN** `MDS_RUNTIME_WEBHOOK_ENABLED=true` and
  `MDS_STRATEGY_RUNTIME_BASE_URL`, `MDS_RUNTIME_WEBHOOK_TIMEOUT_SECONDS`, and
  `MDS_RUNTIME_WEBHOOK_QUEUE_CAPACITY` are all present and valid
- **THEN** MDS constructs the queue, worker, and HTTP adapter as part of the
  same production composition as every other runtime component

#### Scenario: Enabled with invalid configuration fails startup

- **WHEN** `MDS_RUNTIME_WEBHOOK_ENABLED=true` and any of
  `MDS_STRATEGY_RUNTIME_BASE_URL`, `MDS_RUNTIME_WEBHOOK_TIMEOUT_SECONDS`, or
  `MDS_RUNTIME_WEBHOOK_QUEUE_CAPACITY` is missing, empty, non-positive
  (for the numeric fields), or not a valid absolute `http`/`https` URL
  (for the base URL)
- **THEN** `RuntimeSettings` construction raises `ValueError` before any
  component is built
- **AND** no partially constructed composition (queue without worker, worker
  without HTTP adapter, or any other partial state) is ever returned

### Requirement: Worker and HTTP client have single-owner lifecycle

The notification worker and its HTTP client SHALL be constructed and started
exactly once per process and stopped exactly once per process, by the
production runtime composition.

#### Scenario: Start once

- **WHEN** the notifier is enabled and the runtime process starts
- **THEN** exactly one `CommittedBarNotificationWorker` and exactly one HTTP
  adapter instance are constructed
- **AND** the worker's run loop starts exactly once, as one task inside the
  existing realtime `TaskGroup`

#### Scenario: Stop once, without hanging

- **WHEN** the runtime process shuts down (`stop_event` is set)
- **THEN** the worker's run loop exits within a bounded time budget
- **AND** it does not attempt to drain or flush any notification still
  sitting in the queue
- **AND** shutdown of the worker does not delay or block shutdown of any
  other runtime component

#### Scenario: No orphaned background worker

- **WHEN** the runtime process is not running (before start, or after clean
  shutdown)
- **THEN** no notification worker task is alive
