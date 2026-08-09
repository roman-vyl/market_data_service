# Realtime Ingestion and Recovery Specification

## Purpose
Defines WebSocket subscription/ingestion, per-stream supervision, and REST-authoritative recovery for admitted runtime streams.

## Requirements

### Requirement: Deterministic configured subscriptions
MDS SHALL derive one deterministic Bybit kline topic for every enabled configured stream from its exact exchange symbol and canonical timeframe mapping. Unknown or duplicate topic mappings SHALL be rejected.

#### Scenario: Multiple instruments and timeframes are subscribed independently
- **WHEN** configuration enables multiple canonical streams
- **THEN** MDS builds a unique topic mapping for every enabled stream.

### Requirement: WebSocket transport is separated from canonical storage
The WebSocket protocol/transport layer SHALL parse exchange events and manage connection/subscription lifecycle without writing SQLite or running historical repair/readiness logic directly.

#### Scenario: WebSocket message is parsed without storage mutation
- **WHEN** the protocol adapter receives a Bybit heartbeat, subscription acknowledgement, or kline message
- **THEN** it emits transport-neutral events without directly mutating canonical storage.

### Requirement: Reconnect is bounded and cancellable
The realtime connector SHALL support clean cancellation and SHALL use a finite reconnect policy. Reconnect exhaustion SHALL be observable rather than retrying forever.

#### Scenario: Reconnect attempts are exhausted
- **WHEN** all configured reconnect attempts fail
- **THEN** the connector emits reconnect-exhausted/stopped facts and terminates the attempt cycle.

### Requirement: Only admitted streams reach canonical realtime ingestion
Realtime candle events for streams not yet admitted by historical reconciliation SHALL NOT be passed to canonical ingestion.

#### Scenario: Historically incomplete stream receives a candle
- **WHEN** a realtime candle arrives for a stream whose admission gate is closed
- **THEN** the candle is ignored by canonical realtime ingestion.

### Requirement: Only confirmed realtime closes are ingested
Unconfirmed WebSocket candle updates SHALL NOT enter canonical candle ingestion. Confirmed updates SHALL use the shared canonical ingestion path.

#### Scenario: In-progress update is ignored
- **WHEN** Bybit sends an unconfirmed kline update
- **THEN** no realtime ingestion outcome/canonical write is produced for that update.

### Requirement: Realtime outcomes are supervised per stream
Confirmed realtime ingestion SHALL expose one of `committed`, `duplicate`, `corrected`, `rejected`, or `failed` for the affected stream/open time. One stream's operational state SHALL remain isolated from other streams.

#### Scenario: Rejected observation requests verification
- **WHEN** canonical ingestion rejects a confirmed realtime observation
- **THEN** supervision marks recovery required for that stream without mutating another stream's state.

### Requirement: Supervisor signals symptoms, not continuity proof
Realtime supervision MAY detect disconnect restoration, staleness, sequence discontinuity, and rejected observations and emit recovery-required signals with a suspected start hint. Such signals SHALL NOT be treated as proof of historical gaps.

#### Scenario: Sequence discontinuity is only a recovery hint
- **WHEN** a confirmed successful candle skips the expected next timeframe open
- **THEN** supervision requests recovery from the suspected missing point without itself declaring the historical range continuous or repaired.

### Requirement: REST recovery is authoritative and bounded
Realtime recovery SHALL derive a bounded window from durable canonical progress plus the latest closed boundary, invoke existing audit/repair workflows, and serialize recovery per affected stream.

#### Scenario: Recovery has no durable anchor
- **WHEN** a stream has no durable latest committed candle from which to derive recovery
- **THEN** recovery returns `incomplete` rather than inventing a recovery window.

### Requirement: Recovery classifications have explicit ownership
Realtime recovery SHALL distinguish `restored`, `incomplete`, `recoverable_failure`, and `fatal_failure`. Incomplete and recoverable outcomes MAY be requeued with bounded backoff; fatal outcomes SHALL NOT be retried by the realtime recovery worker.

#### Scenario: Recoverable failure does not block another stream
- **WHEN** one stream enters realtime recovery backoff
- **THEN** another due stream remains eligible to run.

### Requirement: Restored requires post-recovery continuity
A stream SHALL be classified `restored` only when post-recovery continuity is proven through the current closed boundary covered by that recovery cycle. If the closed boundary advances during recovery, the next bounded tail window SHALL remain pending.

#### Scenario: Market advances while recovery runs
- **WHEN** post-audit proves the original recovery window but a newer closed boundary now exists
- **THEN** recovery returns `incomplete` with the next finite tail window instead of prematurely declaring restoration.
