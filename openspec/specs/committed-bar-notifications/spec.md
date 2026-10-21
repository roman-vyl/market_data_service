# Committed Bar Notifications Specification

## Purpose
Defines optional best-effort notification delivery from MDS to Strategy Runtime after genuine admitted realtime candle commits.
## Requirements
### Requirement: Notification integration is optional and disabled by default
MDS SHALL operate without constructing a committed-bar notifier worker when the feature is disabled. Disabled notification configuration SHALL NOT be required to contain a valid Runtime URL/timeout/queue configuration.

#### Scenario: Feature is disabled
- **WHEN** committed-bar notifications are disabled
- **THEN** MDS constructs no notification worker/client and realtime ingestion continues without notification delivery.

### Requirement: Only genuine admitted realtime commits notify
MDS SHALL enqueue a notification only from one of two sources, both after the stream has passed realtime admission: (1) a realtime ingestion outcome classified `committed`; (2) the end of a realtime recovery episode that ended `restored`, under the episode requirement below. Duplicate, corrected, rejected, failed, unconfirmed, or admission-blocked observations, `incomplete` or failed recovery attempts, and the historical reconciler SHALL NOT enqueue a committed-bar notification.

#### Scenario: Duplicate realtime candle produces no notification
- **WHEN** a confirmed admitted realtime candle is classified `duplicate`
- **THEN** MDS does not enqueue a committed-bar notification for it.

#### Scenario: Restored recovery episode is the second source
- **WHEN** a realtime recovery episode ends `restored` and the episode requirement says a notification is needed
- **THEN** MDS enqueues one notification for the episode's latest restored bar.

### Requirement: Notification payload is minimal
Each notification SHALL contain exactly the canonical `instrument`, canonical `timeframe`, and committed candle `open_time_ms` needed by Strategy Runtime's closed-bar webhook.

#### Scenario: Committed candle is represented canonically
- **WHEN** `BTCUSDT.P:5m` commits at an open time
- **THEN** the notification carries `instrument=BTCUSDT.P`, `timeframe=5m`, and that `open_time_ms`.

### Requirement: Delivery is bounded best-effort and non-durable
Notification delivery SHALL use a finite in-memory queue and a single consumer. Queue overflow MAY drop a notification with diagnostics. MDS SHALL NOT require an outbox, broker, durable notification log, or delivery retry loop.

#### Scenario: Queue capacity is exhausted
- **WHEN** a new notification cannot be enqueued because the bounded queue is full
- **THEN** MDS drops/logs that notification without rolling back canonical candle ingestion.

### Requirement: At most one notification send is in flight
The notification worker SHALL preserve FIFO processing and SHALL NOT start a second delivery while the current delivery remains in flight.

#### Scenario: Two notifications are queued
- **WHEN** the first HTTP send has not completed
- **THEN** the second queued notification does not begin sending yet.

### Requirement: HTTP delivery is one attempt
The HTTP notifier SHALL perform one POST to Strategy Runtime `/v1/webhooks/closed-bar`, require HTTP `200` with body `{"status":"accepted"}`, and SHALL NOT retry within the notifier.

#### Scenario: Runtime rejects or times out
- **WHEN** the one delivery attempt fails, times out, returns non-JSON, or returns an unexpected status/body
- **THEN** the notifier reports delivery failure without retrying.

### Requirement: Notification failure does not change canonical truth
Delivery failure, queue overflow, or dropped queued work during shutdown SHALL NOT undo an already committed candle and SHALL NOT directly change stream readiness/canonical ingestion classification.

#### Scenario: Delivery fails after candle commit
- **WHEN** the notification POST fails after a candle has been committed
- **THEN** the canonical candle remains committed and realtime processing continues.

### Requirement: Shutdown does not orphan an in-flight send
If notification delivery has already begun when shutdown/cancellation occurs, the worker SHALL observe the in-flight send to completion before exiting; it SHALL NOT start further queued sends after shutdown wins before their send begins.

#### Scenario: Shutdown arrives during HTTP send
- **WHEN** a send is already in flight and shutdown is requested
- **THEN** the worker waits for that send's outcome and then stops without draining later queued items.

### Requirement: A restored realtime recovery episode notifies once for its latest bar
MDS SHALL track, per recovery episode of a stream, whether a retry notification is needed. It SHALL be needed when any REST attempt of the episode committed at least one bar, or when a WebSocket `COMMITTED` outcome marked the episode. A WebSocket `COMMITTED` outcome SHALL mark the recovery episode that was active when the outcome was processed, or the recovery episode created as a direct consequence of processing that same outcome (a sequence gap it revealed), and SHALL NOT mark an unrelated or superseded episode. The need SHALL persist through `incomplete`, non-fatal failed attempts and changes during an attempt, and SHALL end with the episode; a fatal failure ends the episode without a notification and discards the need. After the episode ends `restored` and the stream's durable state has been synchronised, MDS SHALL enqueue exactly one committed-bar notification for `restored_through_open_time_ms` if the need exists. The notification SHALL use the same payload, queue, worker and delivery contract as WebSocket notifications. MDS SHALL NOT enqueue notifications for other restored bars, for episodes without the need, for fatal failures, or from the historical reconciler.

#### Scenario: Lost bar, next bar already delivered
- **WHEN** T1 is lost, T2 arrives over WebSocket and recovery restores T1 through T2
- **THEN** after the stream is READY again one notification for T2 is enqueued

#### Scenario: Silent WebSocket
- **WHEN** REST restores the missing bar T1 and commits it
- **THEN** one notification for T1 is enqueued after the stream is READY

#### Scenario: Several bars restored
- **WHEN** a recovery commits T1..T10 and ends restored
- **THEN** exactly one notification, for T10, is enqueued

#### Scenario: Commit in an earlier attempt of the episode
- **WHEN** attempt 1 commits T1 and ends incomplete because the target advanced, and attempt 2 commits nothing and ends restored
- **THEN** one notification is enqueued

#### Scenario: New WebSocket bar closes during recovery
- **WHEN** a recovery episode is active, a new bar closes and is committed over WebSocket, and the episode ends restored with no REST commit
- **THEN** one notification for the latest restored bar is enqueued after the stream is READY

#### Scenario: WebSocket bar reveals a sequence gap
- **WHEN** a WebSocket `COMMITTED` outcome makes the supervisor open a new recovery episode because of a sequence gap
- **THEN** that new episode is marked, and one notification is enqueued when it ends restored

#### Scenario: Unrelated or superseded episode is not marked
- **WHEN** a WebSocket `COMMITTED` outcome neither found an active episode nor produced a recovery signal, or the episode it could have marked has already ended
- **THEN** no episode is marked and no notification results from that outcome

#### Scenario: Fatal failure
- **WHEN** an episode ends with a fatal failure
- **THEN** no notification is enqueued and the episode's need is discarded

#### Scenario: Nothing committed and no live bar
- **WHEN** an episode ends restored with no REST commit and no WebSocket commit during it
- **THEN** no notification is enqueued

#### Scenario: Not restored
- **WHEN** attempts end incomplete or fail
- **THEN** no notification is enqueued yet and the need is kept

#### Scenario: Ordering
- **WHEN** a restored episode enqueues its notification
- **THEN** the stream lifecycle synchronisation has already run for that episode

#### Scenario: Historical reconciliation
- **WHEN** the historical reconciler repairs bars
- **THEN** no notification is enqueued

