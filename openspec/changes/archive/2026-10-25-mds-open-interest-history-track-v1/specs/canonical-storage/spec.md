## MODIFIED Requirements

### Requirement: One canonical SQLite schema
MDS SHALL use one versioned SQLite schema containing instrument catalog, configured streams, canonical candles, per-stream lifecycle state, quarantine, open interest values and per-stream track progress. Unsupported or incomplete schema versions SHALL fail validation rather than being read through a compatibility fallback. Migration from schema v2 to v3 SHALL only add the open interest and track progress tables and SHALL NOT change existing tables or rows.

#### Scenario: Unsupported schema fails closed
- **WHEN** MDS opens a database whose schema version is neither current nor a supported forward-migration source
- **THEN** initialization fails instead of serving or mutating canonical data through an unknown schema.

#### Scenario: Migration keeps candles and stream state
- **WHEN** a v2 database is migrated to v3
- **THEN** every candle, stream state and quarantine row is unchanged and the two new tables are empty

## ADDED Requirements

### Requirement: Open interest values are stored separately from candles
Open interest SHALL be stored in its own table, unique by stream plus `open_time_ms`, as canonical decimal text normalized by the domain. Storing, correcting or removing open interest SHALL NOT create, change or remove any candle row.

#### Scenario: Duplicate open interest identity
- **WHEN** storage already holds open interest for one stream and open time
- **THEN** a second row with the same identity cannot coexist with it

#### Scenario: Open interest round-trips exactly
- **WHEN** an open interest value is written and later read
- **THEN** it reconstructs the same exact decimal value

### Requirement: Track progress is independent of stream lifecycle
Durable progress of the open interest track (resolved lower bound, discovery cursor, latest committed point, last audit, last success, latest error) SHALL be stored per stream and track, separately from `stream_state`. It SHALL NOT carry a lifecycle state, and writing it SHALL NOT change `stream_state`. A resolved track lower bound SHALL NOT coexist with an unresolved track discovery cursor.

#### Scenario: Open interest progress does not move the stream
- **WHEN** the open interest track of a `ready` stream records discovery, repair progress or a failure
- **THEN** the stream's durable lifecycle state and its candle progress facts are unchanged
