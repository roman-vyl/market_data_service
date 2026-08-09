# Canonical Storage Specification

## Purpose
Defines durable SQLite facts and transactional boundaries that form MDS canonical storage.

## Requirements

### Requirement: One canonical SQLite schema
MDS SHALL use one versioned SQLite schema containing instrument catalog, configured streams, canonical candles, per-stream lifecycle state, and quarantine. Unsupported or incomplete schema versions SHALL fail validation rather than being read through a compatibility fallback.

#### Scenario: Unsupported schema fails closed
- **WHEN** MDS opens a database whose schema version is neither current nor a supported forward-migration source
- **THEN** initialization fails instead of serving or mutating canonical data through an unknown schema.

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
Canonical mutation workflows SHALL execute inside explicit SQLite transactions and SHALL commit or roll back as one unit.

#### Scenario: Exception before commit rolls back
- **WHEN** an exception leaves a storage unit of work before commit
- **THEN** its uncommitted catalog/candle/state/quarantine mutations are rolled back.

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
