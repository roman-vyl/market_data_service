## MODIFIED Requirements

### Requirement: Stream bounds are read-only and readiness-independent
`GET /v1/streams/{ticker}/{timeframe}/bounds` SHALL expose the current available history bounds and durable lifecycle state for an enabled configured stream without requiring runtime readiness or mutating lifecycle/history.

For a stream without an open interest track (any stream other than the 5m stream of an instrument with `open_interest = true`), the bounds SHALL be the committed minimum/maximum candle open times.

For a stream with an open interest track (the 5m stream of an instrument with `open_interest = true`), the bounds SHALL be the intersection of its two data tracks: `earliest_committed_open_time_ms` SHALL be the later of the earliest committed candle open time and the resolved open interest lower bound, and `latest_committed_open_time_ms` SHALL be the earlier of the latest committed candle open time and the latest committed open interest open time. When either track has no bounds, or the intersection is empty, both fields SHALL be `null`; the bounds of one track SHALL NOT be returned alone.

The bounds SHALL report boundaries only and SHALL NOT be treated as proof that every bar between them is present; continuity remains proven by continuity audits, readiness and ready consumer reads.

#### Scenario: Degraded stream bounds remain inspectable
- **WHEN** a configured stream is degraded but has committed candles (and, for a stream with an open interest track, committed open interest)
- **THEN** its bounds endpoint returns those persisted bounds and current state.

#### Scenario: Unflagged stream bounds come from candles
- **WHEN** a stream without an open interest track is read, including a non-5m stream of an instrument with `open_interest = true`
- **THEN** the bounds are the committed minimum and maximum candle open times, exactly as before.

#### Scenario: Open interest starts later than candles
- **WHEN** a stream with an open interest track has candles from `A` and a resolved open interest lower bound `B > A`
- **THEN** `earliest_committed_open_time_ms` is `B`.

#### Scenario: Candle committed before its open interest
- **WHEN** a stream with an open interest track has its latest candle at `T` and its latest committed open interest at `T - step`
- **THEN** `latest_committed_open_time_ms` is `T - step`.

#### Scenario: One track has no bounds
- **WHEN** a stream with an open interest track has committed candles but no resolved open interest lower bound or no committed open interest
- **THEN** both `earliest_committed_open_time_ms` and `latest_committed_open_time_ms` are `null` and `state` is returned as persisted.

#### Scenario: Bounds read has no side effects
- **WHEN** bounds are read for any configured stream
- **THEN** no readiness is required, no lifecycle transition happens and no REST fetch, backfill or repair is started.
