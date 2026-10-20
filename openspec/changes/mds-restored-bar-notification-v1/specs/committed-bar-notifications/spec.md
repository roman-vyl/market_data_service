## ADDED Requirements

### Requirement: A restored realtime recovery notifies once for its latest bar
After a realtime recovery ends `restored` and the stream's durable state has been synchronised, MDS SHALL enqueue exactly one committed-bar notification for `restored_through_open_time_ms`, provided the recovery committed at least one bar. The notification SHALL use the same payload, queue, worker and delivery contract as WebSocket notifications. MDS SHALL NOT enqueue notifications for the other restored bars, for `incomplete` or failed recoveries, for recoveries that committed no bar, or from the historical reconciler.

#### Scenario: Lost bar, next bar already delivered
- **WHEN** T1 is lost, T2 arrives over WebSocket, and recovery restores T1 through T2
- **THEN** after the stream is READY again one notification for T2 is enqueued

#### Scenario: Silent WebSocket
- **WHEN** REST recovery restores the missing bar T1 and commits it
- **THEN** one notification for T1 is enqueued after the stream is READY

#### Scenario: Several bars restored
- **WHEN** a recovery commits T1..T10 and ends restored
- **THEN** exactly one notification, for T10, is enqueued

#### Scenario: Nothing committed
- **WHEN** a recovery ends restored with no committed bar
- **THEN** no notification is enqueued

#### Scenario: Not restored
- **WHEN** a recovery ends incomplete or fails
- **THEN** no notification is enqueued

#### Scenario: Ordering
- **WHEN** a restored recovery enqueues its notification
- **THEN** the stream lifecycle synchronisation has already run for that recovery

#### Scenario: Historical reconciliation
- **WHEN** the historical reconciler repairs bars
- **THEN** no notification is enqueued
