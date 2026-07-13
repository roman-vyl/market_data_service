# Design: Runtime Continuous Reconciliation v1

## Core invariant

For every enabled configured stream, runtime owns the expected historical window until continuity is proven or a fatal failure is reached.

The source of truth is always a continuity audit of the complete expected half-open window. `latest_committed_open_time_ms` alone is not proof because an earlier internal gap may exist.

## Existing BBB-compatible repair flow

The existing production repair workflow already implements the required data path:

```text
RepairStreamGaps(requested_window, max_windows)
  → preflight AuditStreamContinuity(requested_window)
  → empty database becomes one full-window gap
  → prefix/internal/suffix gaps are split into REST fetch windows
  → at most max_windows are imported through canonical ingestion
  → post-repair AuditStreamContinuity(requested_window)
  → COMPLETE | INCOMPLETE | FAILED
```

Runtime continuous reconciliation SHALL orchestrate this workflow. It SHALL NOT add another gap detector, cursor-only catch-up algorithm, or direct storage path.

## Reconciliation cycle

For one stream, a reconciliation cycle fixes:

```text
start = earliest_available_open_time_ms
end   = latest fully closed boundary observed when the cycle starts
window = [start, end)
```

The same fixed window SHALL be used by every bounded pass in that cycle. This guarantees that preflight, repair, and post-audit refer to one finite target.

One bounded pass invokes existing `RepairStreamGaps` for that full window.

Result handling:

- `COMPLETE`: post-audit is continuous; mark the stream historically reconciled and admit it to realtime.
- `INCOMPLETE`: keep the same stream and fixed window scheduled for another bounded pass.
- `FAILED` with recoverable disposition: retain committed progress, apply per-stream backoff, then retry the same fixed window.
- `FAILED` with fatal disposition: mark only that stream failed and stop automatic retries for it in the current process.

Each new pass begins with the existing full-window preflight. Already committed candles therefore disappear from the gap report automatically; only remaining gaps consume the next pass budget.

## Budget semantics

`max_windows` is a work quantum, not a total task limit.

Example with a budget of two fetch windows:

```text
ETH 5m pass 1 → repair two windows → INCOMPLETE
BTC 1h pass 1 → repair two windows → INCOMPLETE
ETH 1h pass 1 → repair two windows → COMPLETE
ETH 5m pass 2 → preflight sees only remaining gaps → repair next two windows
```

A pass may consume fewer than the configured budget when the discovered gaps need fewer windows.

## Runtime scheduling

A single sequential historical reconciliation worker SHALL manage streams that are incomplete or due after backoff.

The scheduling policy is deterministic round-robin over due streams:

- each due stream receives at most one bounded pass before the worker advances;
- no parallel historical REST calls are required in v1;
- a recoverable failure of one stream does not block other due streams;
- a completed or fatal stream leaves the queue;
- shutdown prevents new passes from starting.

The queue itself may remain in memory. On restart it is reconstructed by running fresh preflight against configured streams and canonical SQLite state.

## Startup control flow

```text
RuntimeService starts HTTP/status surfaces
→ initialize and validate config/SQLite/metadata
→ resolve lower bound and fixed target window per configured stream
→ run one bounded reconciliation pass per stream in deterministic order
→ admit streams that are already COMPLETE
→ schedule INCOMPLETE and recoverable streams in the continuous worker
→ mark process healthy
→ run historical worker and realtime runtime together until shutdown
```

Startup is bounded because each stream receives one pass before the long-running workers take ownership. Startup no longer abandons an unfinished stream.

## Realtime admission

The transport SHALL subscribe to all configured topics, while canonical realtime ingestion is gated per stream.

For a historically incomplete stream:

- transport and protocol events may be observed;
- confirmed candle observations SHALL NOT enter canonical realtime ingestion;
- the stream SHALL NOT be treated as realtime ready.

After a continuous post-audit:

- runtime opens the stream's admission gate;
- the existing supervisor/recovery path receives the stream;
- startup/tail recovery closes any interval between the fixed historical target and current closed boundary;
- successful tail recovery is sufficient for data readiness.

This permits a stream to enter realtime without restarting the process and prevents a current WebSocket candle from masking an older historical gap.

## Coordination with realtime recovery

Historical reconciliation and realtime REST recovery are both REST-authoritative workflows. Runtime SHALL serialize them through one process-level historical-operation gate so that they do not execute overlapping repair/backfill work concurrently.

The gate coordinates orchestration only. It does not replace existing per-stream transactions or storage ownership.

## Moving tail

A reconciliation cycle uses a fixed target. When it completes, the existing realtime recovery path SHALL reconcile the short tail from that target to the current closed boundary before the stream can become ready.

If the process restarts before admission, startup creates a new cycle with a newly observed target boundary.

## Restart semantics

No persisted gap-job table is required for v1.

After restart:

1. load validated configured streams;
2. distrust persisted `ready`;
3. resolve/reuse each historical lower bound;
4. choose a new fixed target boundary;
5. run full-window preflight through existing `RepairStreamGaps`;
6. schedule any stream whose result is incomplete or recoverable;
7. admit only streams proven continuous.

Canonical candle rows are the durable work record. A new audit reconstructs every remaining prefix, internal, and suffix gap.

## Readiness and status

Process health remains independent from aggregate readiness.

A stream SHALL remain not ready while any of these apply:

- historical reconciliation pending;
- historical bounded pass active;
- recoverable historical backoff active;
- historical fatal failure;
- realtime admission not yet opened;
- realtime recovery pending;
- fatal realtime failure.

The readiness projection SHALL expose a stable blocking reason sufficient to distinguish these states. Aggregate readiness remains true only when every enabled required stream is ready.

## Graceful shutdown

On SIGINT/SIGTERM:

- stop scheduling new historical passes;
- allow the active bounded pass to finish or stop only at an existing safe application boundary;
- preserve every committed SQLite transaction;
- stop realtime connector/recovery and HTTP resources;
- rely on startup preflight to reconstruct remaining work after restart.

## Module boundaries

The implementation should introduce focused orchestration rather than expand `RuntimeService` into a large worker:

- a reusable one-stream reconciliation-cycle coordinator;
- a sequential runtime reconciliation worker with fairness/backoff;
- a per-stream realtime admission gate;
- minimal wiring changes in runtime composition and status projection.

Application audit, repair, import, and storage modules retain their existing responsibilities.

## Implementation slices

### Slice 1 — exact one-stream reconciliation contract

- extract or add a reusable one-stream reconciliation-cycle coordinator;
- use the existing full-window `RepairStreamGaps` flow;
- prove empty, prefix, internal, suffix, and multiple-gap behavior.

### Slice 2 — continuous runtime ownership

- retain incomplete streams after the startup pass;
- add sequential fair bounded scheduling;
- add per-stream recoverable backoff and graceful stop.

### Slice 3 — realtime admission

- subscribe transport to configured topics;
- gate canonical realtime ingestion until historical completion;
- admit completed streams without process restart;
- serialize historical reconciliation with realtime REST recovery.

### Slice 4 — full acceptance

- add fake REST + fake WebSocket + temporary SQLite full-runtime convergence test;
- run Docker empty-volume and restart smoke;
- update runtime documentation and acceptance status.
