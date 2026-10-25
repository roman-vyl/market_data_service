## ADDED Requirements

### Requirement: An open-interest stream publishes only data-ready bars
For a stream whose instrument has `open_interest = true`, MDS SHALL send the committed-bar notification for bar T only when the stream is data-ready and both its candle and open interest continuity frontiers are at or after T. Otherwise it SHALL NOT be sent; the restoring recovery episode publishes it with its existing single notification. Streams without the flag SHALL be notified as before.

#### Scenario: Normal live bar
- **WHEN** a ready flagged stream commits bar T and its open interest at T is stored within the grace
- **THEN** one ordinary notification for T is sent

#### Scenario: Live bar while a hole is being recovered
- **WHEN** a flagged stream commits T2 over WebSocket while T1 is missing in either track
- **THEN** no notification for T2 is sent until recovery restores the stream, which then sends one notification for its last restored bar

#### Scenario: Stream without the flag
- **WHEN** an unflagged stream commits bar T
- **THEN** the notification for T is sent exactly as before this change
