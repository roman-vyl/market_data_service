## ADDED Requirements

### Requirement: Open-interest streams publish a bar only with its open interest
For a stream whose instrument has `open_interest = true`, MDS SHALL enqueue a committed-bar notification for bar T only after open interest for exactly the same `open_time_ms` is stored. It SHALL NOT use the previous, next or nearest open interest value instead. Notification sources, payload, queue, worker and HTTP delivery SHALL stay as for every other stream. Streams without the flag SHALL be notified exactly as before.

#### Scenario: Open interest already available at bar close
- **WHEN** a flagged stream commits bar T over WebSocket and open interest at T is available from Bybit
- **THEN** MDS stores it and enqueues one ordinary notification for T

#### Scenario: Open interest is late
- **WHEN** a flagged stream commits bar T and Bybit has no open interest point at T yet
- **THEN** no notification for T is enqueued until that point is stored, and WebSocket ingestion continues meanwhile

#### Scenario: Stream without the flag
- **WHEN** an unflagged stream commits bar T
- **THEN** the notification for T is enqueued exactly as before this change

### Requirement: Open interest completion does not wait for history work
Fetching open interest for a live, restored or startup-tail bar SHALL NOT wait for the shared REST operation gate held by candle or open interest history passes, and SHALL NOT block the WebSocket receive path.

#### Scenario: Long history pass in progress
- **WHEN** an open interest history pass holds the operation gate and a flagged stream commits a live bar
- **THEN** open interest for that bar is fetched without waiting for the history pass to finish

### Requirement: Interrupted publication is finished after restart or recovery
After a restored recovery episode, and after a flagged stream is admitted at startup, MDS SHALL store open interest for every committed bar after the stream's latest open interest and then enqueue exactly one notification, for the latest committed bar, when that bar had not been published.

#### Scenario: Crash between candle commit and open interest
- **WHEN** MDS stops after committing bar T of a flagged stream but before storing open interest at T
- **THEN** after restart MDS stores open interest at T and enqueues one notification for T

#### Scenario: Restored episode on a flagged stream
- **WHEN** recovery restores T1..T10 of a flagged stream
- **THEN** open interest is stored for T1..T10 and exactly one notification, for T10, is enqueued
