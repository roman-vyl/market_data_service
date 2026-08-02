# Design: Bounded Recovery Convergence v1

## Incident invariant

Every non-terminal bounded pass must have one observable disposition:

```text
progressed | waiting_after_progress | no_progress_backoff | recoverable_failure | fatal_failure
```

`INCOMPLETE` is not itself evidence of progress. The caller must compare an explicit progress
marker and must never schedule an unchanged attempt as a zero-delay hot loop.

## Decision 1: Persist unresolved lower-bound discovery progress

### State model

Add the nullable field below to the per-stream durable snapshot:

```text
lower_bound_discovery_next_open_time_ms: int | None
```

Its invariants are:

- it is aligned to the stream's canonical timeframe;
- it is scoped by full `StreamKey` through the owning `stream_state` row;
- while `earliest_available_open_time_ms` is unresolved, it identifies the next probe start;
- after the lower bound is resolved, it must be `NULL`;
- it is never substituted for `earliest_available_open_time_ms` in audit, readiness, or API
  projections.

### Probe transaction rules

Discovery chooses its initial cursor in this order:

1. resolved `earliest_available_open_time_ms` returns the cached result and requires no probe;
2. persisted `lower_bound_discovery_next_open_time_ms` resumes unresolved discovery;
3. otherwise, aligned instrument `launchTime` seeds the first probe.

After a successful probe with no valid candle, the use case persists `window.end_ms` as the next
probe cursor before returning or fetching another window. A source/transport/storage failure does
not advance the cursor because the window has not been proven empty.

When a valid earliest candle is found, one SQLite transaction:

- writes `earliest_available_open_time_ms`;
- clears `lower_bound_discovery_next_open_time_ms`;
- clears the applicable transient error detail;
- updates the snapshot timestamps.

If discovery reaches the current latest-closed boundary without finding a candle, the cursor
remains at that boundary. The worker waits with bounded backoff until a later closed boundary is
available; it does not rescan old empty history.

An explicit lower-bound revalidation resets both the resolved lower bound and discovery cursor
according to its separately authorized operator contract. Ordinary retry and restart do not reset
either field.

### Schema migration

The existing six-table ownership boundary remains unchanged. The migration adds one nullable
checked integer column to `stream_state` and advances `schema_meta.schema_version` in the same
schema migration transaction.

Migration requirements:

- existing rows initialize the new field to `NULL`;
- existing candle and lifecycle data are preserved;
- fresh databases are created directly at the new schema version;
- migration failure preserves the original database and prevents readiness;
- older binaries are not supported against the newer schema;
- no dual schema reads or compatibility branches remain after migration.

## Decision 2: Synchronize durable progress before realtime admission

The supervisor may be constructed before a stream completes historical reconciliation, but its
cursor must not remain frozen at construction time.

Admission ordering for one stream is:

```text
historical post-audit COMPLETE
→ read current durable stream_state
→ require a durable latest committed candle
→ synchronize supervisor last_successful_open_time_ms
→ open realtime admission gate
→ enqueue startup/tail recovery
```

The synchronization and gate opening execute in this order on the runtime coordinator's event
loop. No admitted candle outcome may be processed between the durable refresh and gate opening.
Synchronization only moves supervisor progress forward; it never rewinds a newer successfully
observed value.

Missing durable progress at this handoff is an invariant failure. The stream remains unadmitted
and unready rather than accepting realtime data without a recovery anchor.

## Decision 3: Realtime recovery uses a fixed audit/repair window

### Recovery cycle

The first attempt for a recovery signal derives one aligned half-open recovery window. It includes
the earliest applicable suspected point, is clamped to the resolved historical lower bound, and
ends at a latest-closed boundary captured for that cycle.

The resulting window is immutable across `INCOMPLETE` attempts:

```text
fixed recovery window
→ RepairStreamGaps(window, max_windows)
   → full preflight audit
   → bounded import of actual prefix/internal/suffix gaps
   → full post-repair audit
→ COMPLETE | INCOMPLETE | FAILED
```

This replaces the current recovery ordering that sequentially backfills from the old hint before
performing an audit. Existing canonical candles are the durable work record. On every new pass,
preflight removes already repaired regions and plans only the remaining gaps.

The original suspected start is retained for the whole cycle. It is not overwritten with a fetch
cursor, because doing so would shrink the final audit interval and could hide an earlier internal
gap.

### Moving tail

Completing the fixed cycle proves only its captured end boundary. Before marking the stream ready,
runtime compares that boundary with the current latest-closed boundary. If the tail moved, runtime
starts a new bounded tail cycle and keeps the stream unready. Each individual cycle remains finite.

Realtime observations may continue through the admission gate while recovery is pending, but they
do not replace REST-authoritative continuity proof.

### Restart reconstruction

Realtime recovery signals, windows, and queues remain in memory. They are not persisted.

After restart, normal historical startup reconciliation audits the full required interval through a
new fixed target. Canonical candles and durable lower-bound state reconstruct every remaining
prefix, internal, and suffix gap before realtime admission. A recovery-job table is therefore not
needed.

## Decision 4: Progress-aware scheduling and backoff

Every bounded historical or realtime result exposes enough information to derive a stable progress
marker. For repair-driven work the marker includes, at minimum:

- fixed requested window;
- remaining missing-candle count or equivalent gap extent;
- earliest remaining gap start;
- attempted and completed window counts;
- committed, duplicate, corrected, rejected, and unexpected observation counts.

For lower-bound discovery, the durable next probe cursor is the progress marker.

Scheduling rules:

- `INCOMPLETE` with a strictly advanced marker remains in fair scheduling and resets the
  no-progress counter;
- `INCOMPLETE` with an unchanged marker increments a per-stream no-progress counter and receives
  capped exponential backoff;
- recoverable source failure uses the existing capped per-stream failure backoff and does not
  claim progress;
- fatal failure removes only the affected stream from automatic scheduling;
- another due stream may run while one stream is in either backoff class;
- shutdown does not bypass an unexpired delay or start new bounded work.

No-progress backoff protects Bybit and operators, but it is not used as a substitute for correct
cursor or audit semantics.

## Decision 5: Separate budget ownership

The startup lower-bound discovery quantum and realtime recovery quantum have different operational
purposes and load profiles.

`MDS_STARTUP_BACKFILL_WINDOWS_PER_STREAM` remains the bounded startup/discovery quantum for the
existing startup workflow. Runtime SHALL stop passing it into realtime recovery.

Realtime recovery receives a separately documented positive setting:

```text
MDS_REALTIME_RECOVERY_WINDOWS_PER_STREAM
```

Historical full-window repair continues to use its explicit repair-window setting. Increasing a
discovery workaround therefore cannot silently increase realtime REST load.

## Decision 6: Diagnostics remain useful and bounded

Each incomplete or failed pass logs one structured record containing:

- stream identity and recovery reason;
- fixed start and end;
- progress marker before and after the pass;
- next lower-bound probe when applicable;
- attempted/completed windows and ingestion classification counts;
- result classification, error code, delay, and consecutive no-progress count.

Repeated identical `repair_incomplete_gap` diagnostics are emitted at most once per unchanged gap
fingerprint within one recovery cycle. A changed range, new recovery cycle, or new process may emit
a new diagnostic. Quarantine remains diagnostic storage, not a retry queue or continuity source.

## Module boundaries

- `domain` owns validation of the new stream-state field and pure progress comparison values;
- lower-bound application code owns discovery cursor decisions;
- realtime application code owns fixed recovery planning and full-window recovery orchestration;
- runtime workers own fair scheduling, counters, delay, admission ordering, and structured logs;
- the SQLite adapter owns migration and persistence only;
- HTTP, CLI, REST, and WebSocket adapters do not decide convergence.

No universal lifecycle or recovery manager is introduced.

## Implementation slices

### Slice 1: durable lower-bound progress

- add the schema migration and stream-state field;
- resume discovery from the durable probe cursor;
- persist empty probes and atomically resolve/clear the cursor;
- add multi-pass and restart parity tests.

### Slice 2: realtime handoff and recovery convergence

- synchronize supervisor state before admission;
- freeze one recovery window per cycle;
- route bounded recovery through full-window `RepairStreamGaps`;
- add moving-tail continuation and restart reconstruction tests.

### Slice 3: scheduling and operations

- add explicit progress markers and no-progress backoff;
- separate realtime recovery budget configuration;
- add structured progress logs and bounded duplicate quarantine diagnostics.

### Slice 4: incident acceptance

- reproduce both incident loops with deterministic fakes before the fix;
- prove convergence after the fix with budgets smaller than the required work;
- run empty-volume and restart Docker smokes;
- update normative documentation and the acceptance matrix.
