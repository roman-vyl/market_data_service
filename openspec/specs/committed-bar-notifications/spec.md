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
MDS SHALL enqueue a notification only for a realtime ingestion outcome classified `committed` after the stream has passed realtime admission. Duplicate, corrected, rejected, failed, unconfirmed, or admission-blocked observations SHALL NOT enqueue a committed-bar notification.

#### Scenario: Duplicate realtime candle produces no notification
- **WHEN** a confirmed admitted realtime candle is classified `duplicate`
- **THEN** MDS does not enqueue a committed-bar notification for it.

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
