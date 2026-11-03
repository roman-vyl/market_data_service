## ADDED Requirements

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

## MODIFIED Requirements

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
