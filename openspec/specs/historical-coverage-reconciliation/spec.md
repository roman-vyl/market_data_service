# Historical Coverage Reconciliation Specification

## Purpose
Defines bounded, restart-safe discovery, backfill, continuity audit, and gap repair from Bybit REST into canonical storage.

## Requirements

### Requirement: Observed lower bound is authoritative
Bybit instrument launch time SHALL seed historical search but SHALL NOT itself become the canonical earliest available candle. The persisted lower bound SHALL be the earliest valid closed candle actually observed for that stream.

#### Scenario: Launch time precedes real candle history
- **WHEN** initial probe windows after launch contain no valid candles
- **THEN** MDS advances durable discovery progress until a valid candle is observed rather than treating launch time as available data.

### Requirement: Lower-bound discovery is bounded and resumable
One historical reconciliation pass SHALL consume a finite window budget. Unresolved lower-bound discovery SHALL persist an aligned next-search cursor so later passes/restarts continue from durable progress.

#### Scenario: Discovery budget is exhausted
- **WHEN** a pass uses its discovery-window budget without finding a valid candle
- **THEN** the stream remains incomplete and a later pass resumes from the persisted discovery cursor.

### Requirement: Historical import is bounded and atomic per window
REST history SHALL be fetched in aligned finite windows and imported through canonical ingestion inside one transaction per fetched window. Rows outside the requested stream/window SHALL NOT be inserted as canonical candles.

#### Scenario: One import window fails during persistence
- **WHEN** a window import encounters a storage failure before commit
- **THEN** that window is rolled back while previously committed windows remain durable.

### Requirement: Backfill resumes from durable committed progress
Bounded backfill MAY stop before the target range is complete. A resumed/restarted backfill SHALL continue after the latest committed candle for that stream and SHALL NOT erase prior progress.

#### Scenario: Process restarts during backfill
- **WHEN** previously committed candles exist below the target
- **THEN** the next bounded pass resumes after durable `latest_committed_open_time_ms`.

### Requirement: Backfill does not prove continuity
Completing sequential backfill SHALL NOT by itself prove the requested range continuous. Continuity SHALL be determined by an explicit aligned grid audit over canonical storage.

#### Scenario: Internal gap survives later committed candles
- **WHEN** canonical storage contains later candles but an internal timeframe-grid point is missing
- **THEN** continuity audit reports the gap despite the later progress marker.

### Requirement: Runtime reconciliation uses a fixed target and per-pass budget
Once a runtime historical reconciliation cycle has resolved its lower bound, it SHALL fix one finite target end boundary for that cycle. A repair-window budget SHALL limit one pass, not terminate runtime ownership of an incomplete cycle. Every later pass for that cycle SHALL start with a fresh full-window preflight so already committed candles do not consume the next pass budget and prefix/internal/suffix gaps remain visible.

#### Scenario: One pass exhausts its repair budget
- **WHEN** a fixed reconciliation window still contains gaps after one bounded pass uses its window budget
- **THEN** the stream remains incomplete and a later pass re-audits the same fixed window before repairing only remaining gaps.

### Requirement: Restart reconstructs unfinished reconciliation from canonical state
MDS SHALL reconstruct unfinished historical work after restart from validated configuration, canonical candles, persisted lower-bound/progress facts, and a fresh continuity preflight. It SHALL NOT require a persisted reconciliation-job queue, and a persisted `ready` value SHALL NOT by itself prove historical completion after restart.

#### Scenario: Restart occurs with an earlier internal gap
- **WHEN** canonical storage contains a high latest committed candle but an earlier gap remains
- **THEN** restart preflight rediscovers that gap instead of resuming solely from the latest committed timestamp.

### Requirement: Gap repair is audit-driven and bounded
Repair SHALL first audit the explicit range, fetch only missing aligned gap windows through REST/canonical ingestion, and audit again afterward. One invocation SHALL obey a finite repair-window budget.

#### Scenario: Repair budget cannot close all gaps
- **WHEN** missing windows remain after the bounded repair budget is consumed
- **THEN** repair returns `incomplete` with post-repair gap evidence and preserves committed progress.

### Requirement: Historical source failures are classified
Transient source failures SHALL be distinguishable from fatal failures so runtime orchestration can retry recoverable work later without converting fatal configuration/payload/storage failures into an endless retry loop.

#### Scenario: Recoverable failure preserves previous progress
- **WHEN** a recoverable source failure interrupts a later historical window
- **THEN** previously committed windows remain durable and the stream remains eligible for later reconciliation.

### Requirement: Administrative historical backfill is finite and deterministic
Administrative backfill SHALL operate on one selected configured stream or all enabled configured streams in deterministic configuration order. Each run SHALL require a positive finite window budget per stream. For full-history bootstrap, the configured candle-window budget SHALL be shared by lower-bound discovery and backfill work.

#### Scenario: All-stream administrative backfill is bounded
- **WHEN** an operator requests full backfill for all enabled streams with a finite per-stream budget
- **THEN** MDS processes streams sequentially in deterministic configuration order and no stream consumes more than its allowed candle-window budget.

### Requirement: Runtime historical convergence is fair
Startup SHALL perform bounded reconciliation per configured stream and background historical ownership SHALL continue incomplete/recoverable streams fairly. A stream in backoff SHALL NOT prevent another due stream from making progress.

#### Scenario: One stream backs off while another advances
- **WHEN** one stream has a recoverable or no-progress outcome
- **THEN** its retry is delayed without blocking another due configured stream.

### Requirement: Historical and realtime REST-authoritative work is serialized
Continuous historical reconciliation and realtime REST recovery SHALL share runtime orchestration that prevents them from performing overlapping REST-authoritative repair/backfill work concurrently. This coordination SHALL NOT replace canonical storage transactions or per-stream recovery semantics.

#### Scenario: Historical pass overlaps a realtime recovery request
- **WHEN** historical reconciliation is actively performing REST-authoritative work and realtime recovery also becomes due
- **THEN** runtime serializes those operations rather than running both repair/backfill workflows concurrently.

### Requirement: Successful historical reconciliation hands off to realtime
A stream SHALL enter the runtime connecting phase only after its current historical reconciliation window has a successful continuity proof.

#### Scenario: Continuous repaired range becomes connectable
- **WHEN** bounded repair completes and post-repair audit proves the target historical window continuous
- **THEN** the durable stream lifecycle advances to `connecting`.
