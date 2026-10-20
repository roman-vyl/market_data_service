## ADDED Requirements

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
After an unsuccessful realtime recovery with reason `STALE` the worker SHALL retry the stream not earlier than 60 seconds later, and every further retry not earlier than 360 seconds after the previous attempt (`MDS_STALE_RECOVERY_FIRST_RETRY_SECONDS`, `MDS_STALE_RECOVERY_NEXT_RETRY_SECONDS`). A newly expected bar SHALL NOT reset, shorten or duplicate this schedule. A `restored` result SHALL reset it. The schedule SHALL be selected inside the existing recovery worker by recovery reason; reconnect, sequence-gap and other reasons SHALL keep the existing backoff, settings and defaults.

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
