## ADDED Requirements

### Requirement: A bar of an open-interest stream is sent only with continuous open interest
For a stream whose instrument has `open_interest = true`, the notification worker SHALL send the notification for bar T only after a publication check confirms that open interest is stored for every bar of the required range ending at T, each value for exactly the same `open_time_ms` as its bar. MDS SHALL NOT use the previous, next, nearest or any synthetic value. Streams without the flag SHALL be sent exactly as before.

#### Scenario: Open interest already stored
- **WHEN** the worker takes the notification for bar T of a flagged stream and open interest is continuous up to T
- **THEN** it sends the notification without any REST request

#### Scenario: Open interest fetched before send
- **WHEN** open interest at T is not stored and Bybit returns it
- **THEN** MDS stores it and sends the ordinary notification for T

#### Scenario: Partial REST answer
- **WHEN** Bybit returns the points after L with one point missing before T
- **THEN** only the points before the missing one are stored and the notification is not sent

#### Scenario: Stream without the flag
- **WHEN** the worker takes a notification of an unflagged stream
- **THEN** it is sent exactly as before this change

### Requirement: Open interest continues from the last stored point
For bar T the publication check SHALL continue open interest from the stream's latest stored point L: if L is at or after T it SHALL send without a REST request; otherwise it SHALL fetch the range from L plus one step to T through the existing open interest importer and SHALL store only the continuous run of points that starts right after L. A stream without stored open interest SHALL be held without fetching.

#### Scenario: Restart after an outage
- **WHEN** the latest stored open interest point is far behind T
- **THEN** the check continues from that point, and T is sent only after every point up to T is stored

#### Scenario: History not prepared
- **WHEN** a flagged stream has no stored open interest
- **THEN** its notification is held and no open interest request is made

### Requirement: A failed open interest check holds only that notification
When the publication check cannot confirm the required range, MDS SHALL not send that notification, SHALL log the stream, `open_time_ms` and reason, and SHALL NOT change the committed candle, its `COMMITTED` classification, the stream lifecycle or readiness. MDS SHALL NOT retry it on its own schedule: the next notification of the same stream SHALL check a range that also covers the points missed before.

#### Scenario: Bybit open interest is down for one bar
- **WHEN** the check for T fails and the next bar T+1 is committed after Bybit recovers
- **THEN** T is not notified, open interest for T and T+1 is stored, and one notification for T+1 is sent

#### Scenario: Restored episode on a flagged stream
- **WHEN** recovery restores T1..T10 of a flagged stream and enqueues its one notification for T10
- **THEN** open interest for T1..T10 is stored before that notification is sent, and no other notification is added

### Requirement: The open interest check never blocks WebSocket and is bounded
The publication check SHALL run off the event loop inside the notification worker, SHALL NOT take the shared REST operation gate, and SHALL use a REST client with one attempt and a configured short timeout and at most a configured number of REST windows per check, stopping at the first REST failure.

#### Scenario: Bybit REST hangs
- **WHEN** an open interest request does not answer
- **THEN** WebSocket ingestion continues, the check ends after one timeout, and the worker moves on to the next notification

#### Scenario: Long history pass in progress
- **WHEN** a history pass holds the operation gate and a flagged stream's notification is checked
- **THEN** the check runs without waiting for that pass
