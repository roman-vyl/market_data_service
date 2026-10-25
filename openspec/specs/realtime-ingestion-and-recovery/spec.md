# Realtime Ingestion and Recovery Specification

## Purpose
Defines WebSocket subscription/ingestion, per-stream supervision, and REST-authoritative recovery for admitted runtime streams.

## Requirements

### Requirement: Deterministic configured subscriptions
MDS SHALL derive one deterministic Bybit kline topic for every enabled configured stream from its exact exchange symbol and canonical timeframe mapping. Unknown or duplicate topic mappings SHALL be rejected.

#### Scenario: Multiple instruments and timeframes are subscribed independently
- **WHEN** configuration enables multiple canonical streams
- **THEN** MDS builds a unique topic mapping for every enabled stream.

### Requirement: WebSocket transport is separated from canonical storage
The WebSocket protocol/transport layer SHALL parse exchange events and manage connection/subscription lifecycle without writing SQLite or running historical repair/readiness logic directly.

#### Scenario: WebSocket message is parsed without storage mutation
- **WHEN** the protocol adapter receives a Bybit heartbeat, subscription acknowledgement, or kline message
- **THEN** it emits transport-neutral events without directly mutating canonical storage.

### Requirement: Reconnect is bounded and cancellable
The realtime connector SHALL support clean cancellation and SHALL use a finite reconnect policy. Reconnect exhaustion SHALL be observable rather than retrying forever.

#### Scenario: Reconnect attempts are exhausted
- **WHEN** all configured reconnect attempts fail
- **THEN** the connector emits reconnect-exhausted/stopped facts and terminates the attempt cycle.

### Requirement: Only admitted streams reach canonical realtime ingestion
Realtime candle events for streams not yet admitted by historical reconciliation SHALL NOT be passed to canonical ingestion.

#### Scenario: Historically incomplete stream receives a candle
- **WHEN** a realtime candle arrives for a stream whose admission gate is closed
- **THEN** the candle is ignored by canonical realtime ingestion.

### Requirement: Only confirmed realtime closes are ingested
Unconfirmed WebSocket candle updates SHALL NOT enter canonical candle ingestion. Confirmed updates SHALL use the shared canonical ingestion path.

#### Scenario: In-progress update is ignored
- **WHEN** Bybit sends an unconfirmed kline update
- **THEN** no realtime ingestion outcome/canonical write is produced for that update.

### Requirement: Realtime outcomes are supervised per stream
Confirmed realtime ingestion SHALL expose one of `committed`, `duplicate`, `corrected`, `rejected`, or `failed` for the affected stream/open time. One stream's operational state SHALL remain isolated from other streams.

#### Scenario: Rejected observation requests verification
- **WHEN** canonical ingestion rejects a confirmed realtime observation
- **THEN** supervision marks recovery required for that stream without mutating another stream's state.

### Requirement: Supervisor signals symptoms, not continuity proof
Realtime supervision MAY detect disconnect restoration, staleness, sequence discontinuity, and rejected observations and emit recovery-required signals with a suspected start hint. Such signals SHALL NOT be treated as proof of historical gaps.

#### Scenario: Sequence discontinuity is only a recovery hint
- **WHEN** a confirmed successful candle skips the expected next timeframe open
- **THEN** supervision requests recovery from the suspected missing point without itself declaring the historical range continuous or repaired.

### Requirement: REST recovery is authoritative and bounded
Realtime recovery SHALL derive a bounded window from durable canonical progress plus the latest closed boundary, invoke existing audit/repair workflows, and serialize recovery per affected stream.

#### Scenario: Recovery has no durable anchor
- **WHEN** a stream has no durable latest committed candle from which to derive recovery
- **THEN** recovery returns `incomplete` rather than inventing a recovery window.

### Requirement: Recovery classifications have explicit ownership
Realtime recovery SHALL distinguish `restored`, `incomplete`, `recoverable_failure`, and `fatal_failure`. Incomplete and recoverable outcomes MAY be requeued with bounded backoff; fatal outcomes SHALL NOT be retried by the realtime recovery worker.

#### Scenario: Recoverable failure does not block another stream
- **WHEN** one stream enters realtime recovery backoff
- **THEN** another due stream remains eligible to run.

### Requirement: Restored requires post-recovery continuity
A stream SHALL be classified `restored` only when post-recovery continuity is proven through the current closed boundary covered by that recovery cycle. If the closed boundary advances during recovery, the next bounded tail window SHALL remain pending.

#### Scenario: Market advances while recovery runs
- **WHEN** post-audit proves the original recovery window but a newer closed boundary now exists
- **THEN** recovery returns `incomplete` with the next finite tail window instead of prematurely declaring restoration.

### Requirement: A missing expected bar is recovered once per bar
For each admitted stream, supervision SHALL compute the latest expected closed bar on the stream's own time grid: the latest bar whose close plus the grace period (`MDS_STALE_GRACE_MS`, default 15 seconds) has passed. When that bar is not committed, supervision SHALL emit exactly one stale signal for it, unless the stream is recovery-pending or failed. A `restored` recovery SHALL mark the bar that triggered it as checked; `incomplete` or failed recovery SHALL NOT, and follows the retry schedule below.

#### Scenario: WebSocket delivers the bar within the grace period
- **WHEN** the confirmed candle of an expected bar is committed before its grace period ends
- **THEN** no stale signal is emitted for that bar and nothing is enqueued for that stream in the recovery worker

#### Scenario: Silent WebSocket
- **WHEN** no candle arrives for an expected bar and its grace period has passed
- **THEN** exactly one stale signal is emitted for the stream, and none again on later stale ticks until the next expected bar is overdue

#### Scenario: Restored recovery is not repeated for the same bar
- **WHEN** a stale recovery completes `restored` (including one that attempted and committed nothing) and the WebSocket stays silent
- **THEN** no further stale recovery runs for that stream until a later expected bar is overdue

#### Scenario: Incomplete recovery is not a check
- **WHEN** a stale recovery ends `incomplete` or fails
- **THEN** the bar is not marked checked, the stream stays recovery-pending, and only the retry schedule schedules the next attempt

#### Scenario: The recovery marks the bar that triggered it
- **WHEN** a stale recovery is started for expected bar E1 and the next expected bar E2 becomes due before the recovery completes `restored`
- **THEN** E1, the bar stored when the signal was emitted, is marked checked, and E2 is judged independently afterwards: no signal if the restored recovery already covered E2, otherwise exactly one signal for E2

#### Scenario: Retry keeps the original expected bar
- **WHEN** the recovery for expected bar E1 ends `incomplete` or fails and is retried after one or more later bars have become due
- **THEN** the pending expected bar stays E1 until a retry completes `restored`, and then E1 is marked checked

#### Scenario: Long idle produces one signal
- **WHEN** MDS resumes after many missed bars
- **THEN** one stale signal is emitted for the current expected bar and the recovery window covers all missed bars through the existing bounded recovery

#### Scenario: Late WebSocket candle after recovery
- **WHEN** the WebSocket delivers a bar that REST recovery already restored
- **THEN** it is classified as a duplicate and does not trigger another stale signal

#### Scenario: Each timeframe uses its own grid
- **WHEN** streams of 5m, 1h, 4h and 1d are admitted
- **THEN** each stream's expected bar and grace deadline are computed from its own timeframe grid

#### Scenario: Stream without a durable anchor
- **WHEN** a stream has no last successful bar
- **THEN** no stale signal is emitted for it

### Requirement: Stale recovery does not change readiness or live semantics
A restored stale recovery SHALL leave stream readiness to the existing readiness rules and SHALL NOT force a stream ready. `realtime_live` SHALL remain true only after a confirmed WebSocket candle has been observed after the stream's last restored recovery. Stale detection SHALL NOT depend on heartbeat or other transport activity.

#### Scenario: REST recovery does not make a stream live
- **WHEN** a stream is restored by stale recovery and no WebSocket candle follows
- **THEN** `realtime_live` stays false and `ready` is whatever the existing readiness rules give, unchanged by this change

### Requirement: Failed stale recovery is retried on a fixed capped schedule
After an unsuccessful realtime recovery with reason `STALE` the worker SHALL retry the stream not earlier than 60 seconds later, and every further retry not earlier than 360 seconds after the previous attempt (`MDS_STALE_RECOVERY_FIRST_RETRY_SECONDS`, `MDS_STALE_RECOVERY_NEXT_RETRY_SECONDS`). A newly expected bar SHALL NOT reset, shorten or duplicate this schedule. A `restored` result SHALL reset it. The worker SHALL keep a single recovery intent per stream into which all signals merge; reconnect, sequence-gap and other urgent reasons SHALL keep the existing backoff, settings and defaults.

#### Scenario: REST works, WebSocket silent
- **WHEN** REST restores the missing bar at 12:05:15 and the WebSocket stays silent
- **THEN** the next stale check happens at 12:10:15 when the next bar becomes expected, not later than that

#### Scenario: REST and WebSocket both fail
- **WHEN** the attempt at 12:05:15 fails, the retry at 12:06:15 fails and the bar of 12:10 becomes due
- **THEN** no second independent recovery is created, the next attempt is not before 12:12:15, then not before 12:18:15, and the first success covers all missed bars

#### Scenario: Empty recovery is not a failure
- **WHEN** a recovery ends `restored` with `attempted=0 committed=0`
- **THEN** it resets the schedule and is not retried

#### Scenario: Progress is not a failure
- **WHEN** a stale recovery ends `incomplete` because a bar closed during it or `max_windows` was reached, and the recovery window or progress marker advanced
- **THEN** it continues immediately and the failure counter is reset

#### Scenario: Same window without advance is rate-limited
- **WHEN** a stale recovery ends `incomplete` with an unchanged window and progress marker, even after a successful REST response
- **THEN** it counts as unsuccessful and the next attempt follows the 60 s / 360 s schedule

#### Scenario: Reconnect and sequence gap keep the existing backoff
- **WHEN** a recovery with reason reconnect or sequence gap fails with a recoverable error or ends `incomplete` without progress
- **THEN** the retry delay is the existing 1 s base doubling to 60 s, with the existing settings and defaults, and the stale schedule is not applied

#### Scenario: Success resets the schedule
- **WHEN** a recovery after one or more failures ends `restored`
- **THEN** the next unsuccessful recovery of that stream starts again with the 60 s delay

#### Scenario: What counts as unsuccessful
- **WHEN** a recovery ends with a recoverable failure or with `incomplete` and no progress
- **THEN** it counts toward the schedule, while `incomplete` with progress continues immediately and resets the counter

#### Scenario: Reconnect is not delayed by a waiting stale retry
- **WHEN** a stream waits for a stale retry and a reconnect or sequence-gap signal arrives for it
- **THEN** the intent becomes urgent and due now, with cursor and backoff reset

#### Scenario: Duplicate signals change nothing
- **WHEN** a signal arrives that adds no earlier start and no escalation
- **THEN** the intent revision, next attempt time and backoff are unchanged

#### Scenario: A requirement that changes during an attempt is not lost
- **WHEN** a signal that changes the intent arrives while an attempt runs and the attempt ends `RESTORED`
- **THEN** the intent is not cleared and another pass runs from the database with the earliest required start; an unchanged intent keeps its `INCOMPLETE` continuation window

### Requirement: Websocket connection diagnostics
The service SHALL log every websocket connect, disconnect (with close code and reason), transport failure and reconnect exhaustion, and SHALL warn when the event loop was blocked longer than `MDS_EVENT_LOOP_LAG_WARN_SECONDS`.

#### Scenario: Websocket closes
- **WHEN** the websocket closes
- **THEN** a warning containing the close code and reason is logged

### Requirement: Configurable keepalive timeout
The websocket keepalive pong timeout SHALL be configurable through `MDS_WS_PING_TIMEOUT_SECONDS` and default to 60 seconds.

#### Scenario: Default
- **WHEN** the variable is not set
- **THEN** the timeout is 60 seconds

### Requirement: Candle commit is independent of open interest
Realtime and REST candle ingestion, validation and the `COMMITTED` classification SHALL NOT depend on open interest, and the WebSocket receive path SHALL NOT wait for an open interest request. A confirmed valid candle of a flagged stream SHALL be committed whether or not its open interest is available.

#### Scenario: Bybit open interest endpoint is down
- **WHEN** open interest requests fail for a flagged stream
- **THEN** its candles keep being committed as `COMMITTED`, and only the stream's readiness and publication reflect the missing open interest

### Requirement: Each data track has a proven continuity frontier
For a flagged stream the realtime supervisor SHALL keep an open interest continuity frontier next to the candle frontier. A frontier SHALL move only to the end of an audited continuous range or by exactly one step on a live append; a larger jump SHALL raise a sequence discontinuity. The latest stored point SHALL NOT prove continuity. Stale detection SHALL use the lower frontier.

#### Scenario: Partial open interest answer
- **WHEN** open interest at T+1 is stored but T is missing
- **THEN** the open interest frontier stays before T and the stream goes to recovery

### Requirement: Live open interest is fetched when its candle is committed
For a flagged stream, after candle T is committed MDS SHALL fetch open interest for exactly T by REST off the event loop, without the shared operation gate, with one attempt and a short timeout, store it through the existing open interest importer and report the result to the supervisor. A failed fetch SHALL NOT be retried by its own schedule; the expected-bar rule SHALL send the stream to the existing recovery.

#### Scenario: Open interest fetched in time
- **WHEN** candle T of a ready flagged stream is committed and Bybit returns open interest at T
- **THEN** the open interest frontier advances to T and the stream stays ready

### Requirement: Realtime recovery repairs both tracks over one window
For a flagged stream a realtime recovery episode SHALL repair candles and open interest with the existing gap repair over the same window, starting no later than the lower frontier plus one step, and SHALL end `restored` only when both repairs complete with continuous post-audits. Open interest points committed by the episode SHALL count as committed bars for the episode's single notification.

#### Scenario: Restart after a crash between candle and open interest
- **WHEN** MDS stops after committing candle T and before storing open interest at T
- **THEN** after restart the recovery window includes T, open interest at T is stored, the stream becomes ready and one notification is sent
