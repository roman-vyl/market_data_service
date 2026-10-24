## ADDED Requirements

### Requirement: Configured market series per instrument
An enabled instrument MAY declare `market_series` with values from the supported registry `open_interest_15m` and
`premium_index_5m`. Each declared series SHALL become one series stream with its own lifecycle state. An instrument that
declares none SHALL behave exactly as before.

#### Scenario: Instrument without series
- **WHEN** an instrument declares only `canonical_timeframes`
- **THEN** MDS creates no series stream for it and its candle behaviour is unchanged.

### Requirement: Series history is complete and authoritative from Bybit REST
MDS SHALL import the full available history of each series stream from Bybit REST in bounded resumable windows, audit it for
gaps on the series grid, and repair gaps from REST. REST values SHALL replace earlier differing values.

#### Scenario: Restart during history import
- **WHEN** MDS stops in the middle of a series history import
- **THEN** after restart it resumes from the last committed point without re-importing committed windows.

### Requirement: Series points commit atomically with their stream state
Accepted series points and the corresponding series stream state SHALL commit in one transaction.

#### Scenario: Commit failure
- **WHEN** the transaction that stores a series window fails
- **THEN** neither the points nor the stream progress are visible.

### Requirement: Live series follow bucket closes
For every ready series stream MDS SHALL fetch the newly closed bucket(s) from REST after each bucket close plus a grace period
and ingest them through the same path as history.

#### Scenario: OI bucket closes
- **WHEN** the 15-minute OI bucket starting at 10:00 closes at 10:15
- **THEN** MDS commits the 10:00 point shortly after 10:15 plus grace.

### Requirement: Ready-only series range read
MDS SHALL serve `GET /v1/series` for a ready series stream as a complete half-open grid of exact decimal values with a
provenance hash, and SHALL fail closed with the standard error envelope when the stream is not ready or the range has gaps.

#### Scenario: Range with a missing point
- **WHEN** a requested series range has a missing point
- **THEN** MDS returns an error and no partial series.
