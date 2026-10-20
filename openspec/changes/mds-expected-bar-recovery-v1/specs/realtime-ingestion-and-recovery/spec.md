## ADDED Requirements

### Requirement: A missing expected bar is recovered once per bar
For each admitted stream, realtime supervision SHALL compute the latest expected closed bar on the stream's own time grid, defined as the latest bar whose close plus the configured grace period has passed. When that bar has not been committed by WebSocket or by recovery, supervision SHALL emit exactly one stale recovery-required signal for it, unless the stream is already recovery-pending or failed. A recovery classified `restored` SHALL mark that expected bar as checked, and no further stale signal SHALL be emitted for the stream until a later expected bar becomes due. A recovery that is `incomplete` or fails SHALL NOT mark the bar checked and SHALL be governed only by the existing recovery backoff. The grace period SHALL default to 15 seconds and be configured by `MDS_STALE_GRACE_MS`.

#### Scenario: WebSocket delivers the bar within the grace period
- **WHEN** the confirmed candle of an expected bar is committed before its grace period ends
- **THEN** no stale signal is emitted for that bar

#### Scenario: Silent WebSocket
- **WHEN** no candle arrives for an expected bar and its grace period has passed
- **THEN** exactly one stale signal is emitted for the stream, and none again on later stale ticks until the next expected bar is overdue

#### Scenario: Restored recovery is not repeated for the same bar
- **WHEN** a stale recovery completes `restored` (including one that attempted and committed nothing) and the WebSocket stays silent
- **THEN** no further stale recovery runs for that stream until a later expected bar is overdue

#### Scenario: Incomplete recovery is not a check
- **WHEN** a stale recovery ends `incomplete` or fails
- **THEN** the bar is not marked checked, the stream stays recovery-pending, and only the existing backoff schedules the next attempt

#### Scenario: Late WebSocket candle after recovery
- **WHEN** the WebSocket delivers a bar that REST recovery already restored
- **THEN** it is classified as a duplicate and does not trigger another stale signal

#### Scenario: Each timeframe uses its own grid
- **WHEN** streams of 5m, 1h, 4h and 1d are admitted
- **THEN** each stream's expected bar and grace deadline are computed from its own timeframe grid

#### Scenario: Stream without a durable anchor
- **WHEN** a stream has no last successful bar
- **THEN** no stale signal is emitted for it

### Requirement: Live readiness still requires a confirmed WebSocket candle
`realtime_live` SHALL remain true only after a confirmed WebSocket candle has been observed after the stream's last restored recovery. Stale detection SHALL NOT depend on heartbeat or other transport activity.

#### Scenario: REST recovery alone does not make a stream live
- **WHEN** a stream is restored by recovery and no WebSocket candle follows
- **THEN** the stream is ready but `realtime_live` stays false
