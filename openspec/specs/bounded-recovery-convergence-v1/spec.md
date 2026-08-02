# bounded-recovery-convergence-v1 Specification

## Purpose
Guarantee that bounded historical discovery and realtime continuity recovery advance durably,
remain restart-safe, and cannot trap a stream in an unchanged REST retry loop.
## Requirements
### Requirement: Every bounded pass has observable progress semantics

Every non-terminal historical discovery, historical repair, and realtime recovery pass SHALL expose
a stable progress marker sufficient for its runtime owner to distinguish strictly advanced work
from an unchanged attempt.

An `INCOMPLETE` classification alone SHALL NOT be treated as evidence of progress. Runtime SHALL
NOT continuously retry an unchanged bounded prefix with zero delay.

#### Scenario: Incomplete pass advances

- **WHEN** a bounded pass returns `INCOMPLETE` and its progress marker strictly advances
- **THEN** runtime retains ownership and schedules subsequent fair bounded work from that progress

#### Scenario: Incomplete pass does not advance

- **WHEN** a bounded pass returns `INCOMPLETE` with the same progress marker as its preceding attempt
- **THEN** runtime classifies the attempt as no-progress and does not immediately repeat it

### Requirement: Durable per-stream lower-bound discovery cursor

When historical lower-bound discovery completes a valid empty probe window, the system SHALL
durably persist the next aligned probe start for that exact stream before the window is treated as
completed work.

A later bounded pass and a restarted process SHALL resume from that durable probe cursor rather
than from instrument `launchTime`. Discovery progress for one symbol or timeframe SHALL NOT advance,
reset, or overwrite another stream's progress.

The discovery cursor SHALL NOT be used as an observed earliest candle, continuity lower bound,
readiness fact, or consumer-visible candle bound.

#### Scenario: Discovery budget is exhausted after empty probes

- **WHEN** the first available candle lies beyond the current discovery-pass budget
- **THEN** the next pass begins at the first unprobed aligned window instead of instrument launch
  time

#### Scenario: Process restarts before discovery resolves

- **WHEN** a process restarts after one or more empty probe windows were durably completed
- **THEN** discovery resumes from the stored per-stream probe cursor

#### Scenario: Another stream discovers history

- **WHEN** discovery advances for one configured symbol and timeframe
- **THEN** every other stream's discovery cursor remains unchanged

### Requirement: Lower-bound cursor failure and resolution semantics

A source, validation, transaction, or storage failure SHALL NOT advance the lower-bound discovery
cursor for an unproven probe window.

When the first valid available candle is resolved, the system SHALL persist its open time as the
observed earliest available candle and clear the discovery cursor in one transaction. A resolved
lower bound and a non-null discovery cursor SHALL be rejected as invalid durable state.

If the cursor reaches the current latest-closed boundary without finding a candle, the stream SHALL
remain unready and retry only after bounded delay or after a later searchable boundary exists. It
SHALL NOT rescan previously proven empty windows.

#### Scenario: Probe request fails

- **WHEN** a probe window fails before a valid empty result is established
- **THEN** the stored next-probe cursor remains at the failed window start

#### Scenario: Earliest candle is found

- **WHEN** a probe returns the first valid candle for the stream
- **THEN** the observed earliest candle and cleared discovery cursor commit atomically

#### Scenario: No closed candle is yet available

- **WHEN** discovery reaches the latest searchable boundary without finding a valid candle
- **THEN** the stream remains unready and waits without rescanning completed empty windows

### Requirement: Additive schema migration

The service-owned SQLite schema SHALL persist the lower-bound discovery cursor in the existing
per-stream operational state without adding a job table, event log, or second state store.

The forward migration SHALL preserve existing instrument, stream, candle, lifecycle, and
quarantine data, initialize the new field safely, and advance the schema version atomically. A
migration failure SHALL preserve the original database and prevent readiness.

The implementation SHALL NOT retain dual-read or legacy-schema compatibility branches after the
migration.

#### Scenario: Existing service database is upgraded

- **WHEN** a valid database from the preceding schema version is opened by the upgraded service
- **THEN** the new per-stream discovery field is added without changing existing canonical rows

#### Scenario: Migration cannot complete

- **WHEN** the schema migration transaction fails
- **THEN** the original database remains preserved and the service does not report readiness

### Requirement: Durable cursor synchronization before realtime admission

Immediately before a historically completed stream is admitted to canonical realtime ingestion,
runtime SHALL read its current durable latest committed candle and synchronize the supervisor's
per-stream successful-open cursor.

The synchronization SHALL occur before the admission gate opens, SHALL never rewind a newer
supervisor cursor, and SHALL prevent an admitted candle from being compared against a stale cursor
captured before historical reconciliation completed.

A stream without a durable latest committed anchor SHALL remain unadmitted and unready.

#### Scenario: Historical reconciliation completes after supervisor startup

- **WHEN** a stream's durable tail advances after supervisor construction and the stream is ready
  for admission
- **THEN** supervisor progress is refreshed from the durable tail before the gate opens

#### Scenario: Durable handoff anchor is missing

- **WHEN** historical completion attempts to admit a stream without a durable latest candle
- **THEN** the gate remains closed and the stream remains unready

### Requirement: Fixed realtime recovery scope

The first attempt for one realtime recovery cycle SHALL derive an aligned half-open recovery window
from durable stream state, any suspected-start hint, the resolved lower bound, and a captured
latest-closed boundary.

That recovery window, including its original suspected start, SHALL remain fixed across bounded
`INCOMPLETE` attempts. Cursor advancement SHALL NOT shrink the interval that must pass final
continuity audit.

#### Scenario: Recovery exceeds one pass budget

- **WHEN** one suspected recovery interval requires more REST windows than the configured per-pass
  budget
- **THEN** every subsequent pass retains the original start and captured end until that interval is
  proven continuous

### Requirement: Audit-authoritative realtime recovery

Realtime recovery SHALL reconcile its fixed window through the existing canonical continuity
preflight, bounded gap import, and post-repair audit workflow.

Each repeated bounded pass SHALL begin with a fresh audit of the complete fixed window so canonical
candles remove completed work and only actual remaining prefix, internal, or suffix gaps consume
the next budget.

Realtime recovery SHALL NOT sequentially refetch an unaudited old prefix merely because a signal's
suspected start predates the durable latest candle. It SHALL NOT add a second gap detector, fetch
window splitter, ingestion path, or direct SQLite mutation path.

#### Scenario: Old hint precedes a current durable tail

- **WHEN** a recovery hint points far behind the latest committed candle and the interval contains
  one internal gap
- **THEN** recovery fetches the actual audited gap rather than repeatedly importing the interval's
  already canonical prefix

#### Scenario: Repair remains incomplete

- **WHEN** a bounded repair pass leaves gaps in the fixed recovery window
- **THEN** the next pass audits that same complete window and plans only gaps still present

### Requirement: Finite moving-tail cycles

A restored recovery result SHALL require successful continuity proof through the fixed cycle end.
Before the stream becomes ready, runtime SHALL compare that end with the current latest-closed
boundary.

If the boundary advanced, runtime SHALL keep the stream unready and start another finite tail
recovery cycle. One moving tail SHALL NOT mutate the already proven start of the completed cycle.

#### Scenario: Tail advances during recovery

- **WHEN** the latest-closed boundary advances before a fixed recovery cycle completes
- **THEN** runtime preserves the completed proof and starts a new finite cycle for the additional
  tail before readiness

### Requirement: No persisted realtime recovery job

Realtime recovery signals, fixed windows, queues, retry counters, and backoff deadlines SHALL
remain process-local.

After restart, normal historical startup reconciliation SHALL reconstruct unfinished realtime work
by auditing canonical candles from the durable lower bound through a newly fixed target. No
recovery-job table, replay queue, or WebSocket event log SHALL be introduced.

#### Scenario: Process stops during realtime recovery

- **WHEN** the process restarts after committing only part of a realtime recovery interval
- **THEN** startup audit derives remaining gaps from canonical candles without a persisted recovery
  job

### Requirement: Progress-aware retry and no-progress backoff

A bounded `INCOMPLETE` result with a strictly advanced progress marker SHALL remain under fair
runtime ownership and SHALL reset its consecutive no-progress counter.

A bounded `INCOMPLETE` result with an unchanged marker SHALL increment a per-stream no-progress
counter and SHALL be retried only after capped exponential backoff. A stream in no-progress or
recoverable-failure backoff SHALL NOT prevent another due stream from running.

A fatal result SHALL stop automatic retry only for the affected stream. Backoff SHALL NOT be used
as a substitute for durable cursor advancement or full-window audit semantics.

#### Scenario: Repeated attempt makes no progress

- **WHEN** the same stream returns an unchanged progress marker on consecutive incomplete attempts
- **THEN** its retry delay increases up to the configured cap

#### Scenario: Another stream is due during backoff

- **WHEN** one stream is waiting after no progress and another stream is due
- **THEN** the due stream receives its bounded turn without waiting for the first stream's delay

#### Scenario: Progress resumes

- **WHEN** a later attempt strictly advances the marker
- **THEN** the no-progress counter resets

### Requirement: Independent recovery budgets

Startup lower-bound discovery and realtime recovery SHALL have independent positive per-pass window
budgets. Changing the startup discovery budget SHALL NOT implicitly change realtime recovery REST
load.

The realtime recovery budget SHALL be explicitly configurable and documented. Historical and
realtime REST-authoritative work SHALL remain serialized by runtime orchestration.

#### Scenario: Operator changes startup discovery budget

- **WHEN** the startup lower-bound discovery window budget is changed
- **THEN** the realtime recovery window budget remains unchanged

### Requirement: Actionable and bounded recovery diagnostics

Every incomplete, recoverable, no-progress, or fatal bounded pass SHALL emit structured diagnostics
containing the stream, fixed interval, before/after progress, applicable next cursor, window and
ingestion counts, result or error code, scheduled delay, and consecutive no-progress count.

Repeated identical unresolved-gap quarantine diagnostics SHALL be bounded within one unchanged
recovery cycle. Quarantine SHALL remain diagnostic evidence and SHALL NOT become retry state or
continuity authority.

#### Scenario: Recovery pass remains incomplete

- **WHEN** a bounded pass cannot complete its fixed interval
- **THEN** its diagnostic record contains enough progress and scheduling data to identify whether
  the next attempt will advance or back off

#### Scenario: Same gap survives repeated passes

- **WHEN** an identical unresolved gap fingerprint persists across one recovery cycle
- **THEN** the cycle does not append an unbounded number of identical quarantine records

### Requirement: Incident regression acceptance

Version-controlled acceptance tests SHALL cover lower-bound discovery beyond one pass budget,
old-hint realtime recovery beyond one pass budget, late admission, moving-tail handoff,
no-progress protection, multi-stream isolation, and restart reconstruction.

Empty-volume and restart runtime smokes SHALL converge without mandatory administrative backfill.

#### Scenario: Lower-bound discovery spans passes and restart

- **WHEN** the first available candle lies beyond one discovery quantum and the process restarts
  between passes
- **THEN** the stream eventually resolves its lower bound without repeating proven empty probes

#### Scenario: Old recovery hint spans multiple passes

- **WHEN** an old hint covers more work than one realtime recovery quantum and contains an internal
  gap
- **THEN** bounded attempts eventually repair the gap and audit the original interval before
  readiness

#### Scenario: Empty multi-stream runtime starts

- **WHEN** the service starts on an empty volume with multiple configured streams
- **THEN** every stream converges independently and aggregate readiness appears only after all
  required streams complete recovery
