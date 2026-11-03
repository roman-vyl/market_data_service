# Canonical Storage Specification

## Purpose
Defines durable SQLite facts and transactional boundaries that form MDS canonical storage.
## Requirements
### Requirement: One canonical SQLite schema
MDS SHALL use one versioned SQLite schema containing instrument catalog, configured streams, canonical candles, per-stream lifecycle state, quarantine, open interest values and per-stream track progress. Unsupported or incomplete schema versions SHALL fail validation rather than being read through a compatibility fallback. Migration from schema v2 to v3 SHALL only add the open interest and track progress tables and SHALL NOT change existing tables or rows.

#### Scenario: Unsupported schema fails closed
- **WHEN** MDS opens a database whose schema version is neither current nor a supported forward-migration source
- **THEN** initialization fails instead of serving or mutating canonical data through an unknown schema.

#### Scenario: Migration keeps candles and stream state
- **WHEN** a v2 database is migrated to v3
- **THEN** every candle, stream state and quarantine row is unchanged and the two new tables are empty

### Requirement: Unique catalog and candle identities
Canonical instrument tickers and exact exchange-symbol mappings SHALL be unique. A stream SHALL be unique by instrument plus timeframe. A canonical candle SHALL be unique by stream plus `open_time_ms`.

#### Scenario: Duplicate candle identity cannot create two canonical rows
- **WHEN** storage already contains a canonical candle for one stream/open time
- **THEN** a second canonical row with the same identity cannot coexist with it.

### Requirement: Canonical OHLCV storage is decimal text
SQLite candle OHLCV columns SHALL store canonical decimal text; decimal normalization SHALL remain a domain responsibility rather than relying on SQLite numeric coercion.

#### Scenario: Stored OHLCV round-trips exactly
- **WHEN** a canonical candle is written and later read
- **THEN** its OHLCV values reconstruct the same exact decimal values.

### Requirement: Per-stream durable lifecycle state
MDS SHALL persist lifecycle state independently for each configured stream. The durable lifecycle SHALL use `uninitialized`, `bootstrapping`, `auditing`, `repairing`, `connecting`, `ready`, `degraded`, and `failed`, together with the resolved earliest available candle, lower-bound discovery cursor, latest committed candle, operational timestamps, and latest error facts needed for restart recovery.

Lifecycle transitions SHALL be validated by the domain state machine; illegal direct transitions SHALL be rejected. A resolved historical lower bound SHALL NOT coexist with an unresolved discovery cursor.

#### Scenario: Restart reconstructs stream progress from SQLite
- **WHEN** the process restarts after bounded historical work
- **THEN** MDS can resume from persisted per-stream state and canonical candles without a separate orchestration queue.

#### Scenario: Illegal lifecycle transition is rejected
- **WHEN** application code attempts a transition not allowed by the current per-stream lifecycle state machine
- **THEN** the transition fails instead of silently persisting an invalid lifecycle state.

### Requirement: Transactional unit of work
Canonical mutation workflows SHALL execute inside explicit SQLite transactions and SHALL commit or roll back as one unit. Mutations SHALL retain the existing immediate write-transaction boundary. A lifecycle transition following a standalone snapshot SHALL re-read and validate current state inside its mutation transaction, preserving unrelated current progress fields rather than saving the preliminary snapshot.

#### Scenario: Exception before commit rolls back
- **WHEN** an exception leaves a storage unit of work before commit
- **THEN** its uncommitted catalog/candle/state/quarantine mutations are rolled back.

#### Scenario: Preliminary snapshot cannot overwrite newer progress
- **GIVEN** a preliminary lifecycle snapshot has been read
- **AND** another transaction subsequently commits updated stream progress
- **WHEN** a lifecycle transition is attempted
- **THEN** it re-reads current state inside the write transaction and validates the transition against that state
- **AND** a successful transition preserves the newer progress fields instead of restoring their preliminary values.

### Requirement: Read snapshots are internally consistent
Consumer candle reads SHALL obtain stream state and the requested candle range from one SQLite read transaction/snapshot.

#### Scenario: Consumer read does not mix different storage moments
- **WHEN** MDS reads state and candles for one consumer request
- **THEN** both are read from the same SQLite snapshot.

### Requirement: SQLite connection safety baseline
Every ordinary SQLite connection SHALL enable foreign-key enforcement and a finite busy timeout. Database creation SHALL establish WAL journal mode.

#### Scenario: Ordinary connection enables safety pragmas
- **WHEN** MDS opens an existing SQLite database through its shared connection factory
- **THEN** foreign keys are enabled and the configured finite busy timeout is applied.

### Requirement: No durable realtime orchestration log
MDS SHALL NOT require persisted WebSocket event logs, connection/subscription event tables, recovery-job tables, or server-owned consumer cursors for v1 runtime recovery.

#### Scenario: Runtime facts are rebuilt without event-log persistence
- **WHEN** MDS restarts
- **THEN** transient realtime connection/supervision facts are reconstructed while durable market-data/lifecycle truth comes from canonical SQLite state.

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

### Requirement: Lifecycle snapshots use read-only committed state
Reading a standalone per-stream lifecycle snapshot SHALL use read-only database access and SHALL NOT reserve SQLite's writer slot or mutate canonical data. Each call SHALL return an internally consistent committed snapshot for the requested stream, without changing the existing snapshot shape or missing-stream error behavior. Read-only access SHALL NOT create a missing database or initialize or migrate storage.

#### Scenario: Snapshot completes while a writer holds uncommitted state
- **GIVEN** an initialized WAL database with committed state for a stream
- **AND** another connection holds an active write transaction with an uncommitted change to that state
- **WHEN** runtime reads a standalone lifecycle snapshot for the stream
- **THEN** the read completes while that writer transaction is still held
- **AND** returns the previously committed state without observing the uncommitted change.

#### Scenario: A fresh snapshot observes a completed commit
- **WHEN** a writer commits a stream-state change and a new lifecycle snapshot call begins afterward
- **THEN** the new call returns the newly committed state rather than a cached earlier snapshot.

#### Scenario: A rolled-back change remains invisible
- **WHEN** a writer rolls back its stream-state change
- **THEN** a subsequent lifecycle snapshot returns the unchanged committed state.

#### Scenario: Streams remain isolated
- **WHEN** lifecycle snapshots are read for two distinct stream identities
- **THEN** each result contains only the durable state associated with its requested identity.

#### Scenario: Unknown stream preserves the existing error
- **WHEN** a lifecycle snapshot is requested for a stream without a persisted state
- **THEN** the existing missing-stream error is returned without creating catalog or state records.

#### Scenario: Read-only access has no database-creation side effect
- **WHEN** standalone lifecycle snapshot access targets a missing database
- **THEN** it fails without creating or initializing a database.

#### Scenario: Snapshot resources are released
- **WHEN** a standalone lifecycle snapshot call succeeds or fails
- **THEN** its database connection and read transaction are closed before control returns to the caller.
