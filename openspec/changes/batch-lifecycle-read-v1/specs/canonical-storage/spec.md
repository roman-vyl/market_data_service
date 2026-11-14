## MODIFIED Requirements

### Requirement: Lifecycle snapshots use read-only committed state
Reading a standalone per-stream lifecycle snapshot SHALL use read-only database access and SHALL NOT reserve SQLite's writer slot or mutate canonical data. Each call SHALL return an internally consistent committed snapshot for the requested stream, without changing the existing snapshot shape or missing-stream error behavior. A complete multi-stream runtime lifecycle synchronization SHALL obtain its preliminary lifecycle snapshots through one read-only database connection, one read transaction and one batch SELECT, and SHALL associate the results with requested streams in their configured processing order. Conditional lifecycle transitions remain separate mutations and SHALL re-read current durable state inside their write transactions. Read-only access SHALL NOT create a missing database or initialize or migrate storage.

#### Scenario: Full lifecycle synchronization uses one batch read
- **GIVEN** multiple configured streams require a complete runtime lifecycle synchronization
- **WHEN** runtime obtains the preliminary durable snapshots for that pass
- **THEN** it uses one read-only SQLite connection and one batch SELECT in one read transaction
- **AND** processes the streams in their existing configured order.

#### Scenario: Batch snapshot completes while a writer holds uncommitted state
- **GIVEN** an initialized WAL database with committed state for multiple streams
- **AND** another connection holds an active write transaction with an uncommitted change to that state
- **WHEN** runtime batch-reads the lifecycle snapshots
- **THEN** the read completes while that writer transaction is still held
- **AND** returns the previously committed states without observing the uncommitted change.

#### Scenario: Concurrent recovery progress is preserved before a lifecycle write
- **GIVEN** runtime has obtained a preliminary batch snapshot for a stream
- **AND** recovery commits newer durable progress for that stream before runtime applies a lifecycle transition
- **WHEN** runtime applies the transition
- **THEN** it re-reads current state inside the write transaction
- **AND** validates the transition against that current state
- **AND** preserves the concurrently committed progress fields rather than replacing them with values from the preliminary batch snapshot.

#### Scenario: READY DEGRADED and FAILED projection remains unchanged
- **WHEN** the same realtime facts and durable lifecycle states are synchronized through the batch-read path
- **THEN** the READY, DEGRADED or FAILED decisions and status projections are the same as before the batch-read change.

#### Scenario: Time-boundary projection remains unchanged
- **WHEN** a complete synchronization runs immediately before, exactly at, or immediately after an existing readiness time boundary
- **THEN** each stream receives the same lifecycle decision and status projection as the existing per-stream read path.

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
- **WHEN** a standalone or batch lifecycle snapshot call succeeds or fails
- **THEN** its database connection and read transaction are closed before control returns to the caller.
