# Proposal: Runtime Continuous Reconciliation v1

## Why

`market-data-service serve` currently performs one bounded historical pass for each configured stream. When that pass returns `incomplete` or a recoverable source failure, the stream remains not ready but the running process does not schedule another pass. A service started on an empty or partially populated database therefore may never converge to readiness without manual backfill commands or repeated process restarts.

The existing historical pipeline already contains the required data logic:

```text
historical lower bound
→ continuity preflight over a half-open expected window
→ prefix/internal/suffix gaps
→ bounded REST repair through canonical ingestion
→ post-repair continuity audit
```

The missing behavior is runtime ownership of unfinished reconciliation.

## What changes

The runtime SHALL keep every configured stream under historical reconciliation until the existing audit/repair workflow proves the stream continuous through a fixed target boundary or returns a fatal failure.

For each stream:

```text
resolve/reuse lower bound
→ fix target_end for this reconciliation cycle
→ RepairStreamGaps over [lower_bound, target_end)
→ complete: admit to realtime
→ incomplete: schedule another bounded pass
→ recoverable failure: retry after per-stream backoff
→ fatal failure: stop automatic retries for that stream
```

A bounded window budget limits one pass only. It does not end the runtime's responsibility for the unfinished stream.

## Intended outcome

A clean Docker volume, a partially populated database, and a restarted service all converge without mandatory operator-driven backfill:

```text
serve starts
→ full-window preflight finds every missing prefix/internal/suffix range
→ existing bounded repair fills a fair quantum
→ unfinished streams remain scheduled
→ transient failures preserve committed progress and retry later
→ continuous post-audit admits each completed stream to realtime
→ aggregate readiness becomes true after all required streams complete tail recovery
```

Completed streams may operate in realtime while other configured streams continue historical reconciliation.

## Scope

This change includes:

- process-lifetime ownership of incomplete historical reconciliation;
- repeated bounded repair passes;
- deterministic fairness between due streams;
- per-stream recoverable backoff;
- realtime admission after continuous post-audit;
- restart and graceful-shutdown behavior;
- readiness reasons and lifecycle observability;
- offline and full-runtime acceptance tests.

## Existing behavior reused

This change reuses without semantic duplication:

- `ResolveHistoricalLowerBound`;
- `AuditStreamContinuity`;
- `RepairStreamGaps`;
- `ImportHistoricalWindow` and canonical ingestion;
- existing gap detection and fetch-window splitting;
- existing source-failure classification;
- existing realtime connector, supervisor, and recovery coordinator;
- canonical SQLite candles and stream state.

## Non-goals

- changing candle identity, validation, duplicate, or correction semantics;
- creating a second gap algorithm or repair implementation;
- changing Bybit protocol parsing;
- introducing config hot reload;
- introducing parallel REST workers;
- requiring a persisted reconciliation-job table when canonical SQLite plus a new preflight is sufficient;
- removing the administrative backfill CLI.
