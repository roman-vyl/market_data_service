## ADDED Requirements

### Requirement: Open interest is an independent historical track
For every stream with the open interest track enabled, MDS SHALL reconcile open interest history from Bybit `/v5/market/open-interest` (`intervalTime=5min`) with the same rules as candle history: observed lower bound, bounded and resumable discovery, bounded window import atomic per window, explicit grid audit, audit-driven bounded repair, restart reconstruction from stored data and progress, and classified source failures. Open interest windows SHALL hold at most 200 points.

#### Scenario: Open interest history is loaded and proven
- **WHEN** the track runs for a flagged stream until its fixed target
- **THEN** its lower bound is resolved and an explicit audit of `[open interest lower bound, target]` reports no gaps

#### Scenario: Restart in the middle of open interest history
- **WHEN** the process restarts with part of the open interest history stored
- **THEN** the next pass re-audits the full window and repairs only the remaining gaps, without a persisted job queue

### Requirement: Open interest lower bound is independent of the candle lower bound
The open interest lower bound SHALL be the earliest valid open interest point actually observed for the stream, discovered and persisted separately from the candle lower bound. Bars before the open interest lower bound SHALL NOT be open interest gaps, and candles before it SHALL be kept.

#### Scenario: Candles start before open interest
- **WHEN** a stream has candles from 2020-03-25 and its first open interest point is 2020-07-20
- **THEN** the open interest audit starts at 2020-07-20, the earlier months are not reported as open interest gaps, and no candle is removed

#### Scenario: Open interest starts before candles
- **WHEN** a stream's first open interest point is earlier than its first stored candle
- **THEN** the open interest track is complete on its own range and the candle track is unaffected

### Requirement: Open interest history does not touch the live candle pipeline
Open interest history SHALL run in the background after candle startup, and SHALL NOT change stream lifecycle, stream readiness, runtime status blocking reasons, realtime admission, realtime supervision or recovery, committed-bar notifications, read contracts or candle rows. Any failure of the open interest track SHALL be contained in that track: a recoverable failure SHALL back off, a fatal failure SHALL stop the track for that stream until restart, and neither SHALL stop the service.

#### Scenario: Open interest source fails while candles are live
- **WHEN** open interest requests fail for a `ready` stream
- **THEN** the stream stays `ready`, its candles and notifications continue, and only the open interest track backs off or stops

#### Scenario: Candle startup does not wait for open interest
- **WHEN** the service starts with flagged streams whose open interest history is incomplete
- **THEN** candle startup, admission and readiness proceed as without the flag

### Requirement: Open interest work shares REST serialization and keeps its tail
Every open interest pass SHALL run under the same runtime operation gate as candle historical reconciliation and realtime REST recovery, with a finite per-pass window budget. After a completed cycle the track SHALL start a new cycle with a fresh target after a configured refresh period, so the stored open interest follows the closed bars.

#### Scenario: Realtime recovery becomes due during open interest history
- **WHEN** a realtime REST recovery becomes due while an open interest pass holds the operation gate
- **THEN** the recovery runs after that one bounded pass, not after the whole open interest history

#### Scenario: Tail after completion
- **WHEN** the open interest history of a stream is complete and new bars close
- **THEN** a later cycle extends the stored open interest to the new last closed bar
