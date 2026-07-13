# Runtime Continuous Reconciliation v1 Specification

## Requirement: Full-window preflight authority

For each enabled configured stream, runtime SHALL define an expected half-open historical window from the resolved historical lower bound through a fixed latest-closed boundary and SHALL use the existing continuity audit as the authority for every prefix, internal, and suffix gap inside that window.

An empty database SHALL be represented as one gap covering the complete expected window. A high latest committed timestamp SHALL NOT be accepted as proof that no earlier internal gap exists.

## Requirement: Existing bounded repair path

Runtime historical reconciliation SHALL use the existing `RepairStreamGaps` workflow, including its preflight audit, fetch-window splitting, canonical ingestion, and post-repair audit. Runtime SHALL NOT implement a second gap detector, repair algorithm, or direct candle-write path.

## Requirement: Fixed reconciliation target

A reconciliation cycle SHALL fix its target end boundary before bounded work begins. Every pass in that cycle SHALL reconcile the same half-open window. Completion SHALL require a continuous post-audit of that fixed window.

## Requirement: Per-pass budget semantics

The configured repair-window budget SHALL limit one bounded reconciliation pass only. An `INCOMPLETE` result caused by remaining gaps or exhausted pass budget SHALL keep the stream scheduled for another pass and SHALL NOT end runtime ownership of the task.

Each subsequent pass SHALL begin with a fresh full-window preflight so already committed data is not fetched again and only remaining gaps consume work budget.

## Requirement: Fair continuous ownership

Runtime SHALL maintain process-lifetime ownership of incomplete configured streams and SHALL execute due streams through one deterministic sequential fair scheduler. Each due stream SHALL receive at most one bounded pass before another due stream may receive work.

A large historical gap in one stream SHALL NOT starve other configured streams.

## Requirement: Recoverable failure continuation

A recoverable Bybit or network failure SHALL preserve committed progress, keep the affected stream not ready, and schedule another reconciliation pass after bounded per-stream backoff. Other due streams SHALL continue during that backoff.

A fatal failure SHALL stop automatic retries only for the affected stream.

## Requirement: Restart reconstruction

After process restart, runtime SHALL reconstruct unfinished reconciliation by running a new full-window preflight against canonical SQLite state. The service SHALL NOT require a persisted reconciliation-job queue when candles, lower-bound metadata, stream lifecycle, and fresh audit are sufficient to identify remaining work.

Persisted `ready` SHALL be distrusted until historical continuity and realtime freshness are re-proven.

## Requirement: Per-stream realtime admission

Historically incomplete streams SHALL NOT send confirmed WebSocket observations into canonical realtime ingestion. Runtime SHALL open realtime admission for a stream only after continuous post-audit of its fixed historical target.

A completed stream SHALL be admitted without restarting the process and SHALL be permitted to operate in realtime while other streams continue historical reconciliation.

## Requirement: Tail reconciliation and fresh close

After historical admission, the existing realtime recovery path SHALL reconcile any tail between the fixed historical target and the current latest-closed boundary. The stream SHALL remain not ready until recovery succeeds and a fresh confirmed WebSocket close is observed.

## Requirement: Serialized REST-authoritative work

Continuous historical reconciliation and realtime REST recovery SHALL be serialized by runtime orchestration so they do not perform overlapping REST-authoritative repair/backfill work concurrently. Existing canonical transactions and per-stream recovery semantics SHALL remain unchanged.

## Requirement: Readiness transparency

The readiness projection SHALL distinguish historical reconciliation, historical backoff, fatal historical failure, realtime admission pending, realtime recovery pending, and fresh-close waiting. Aggregate readiness SHALL remain false while any enabled required stream is not ready.

## Requirement: Graceful shutdown

Shutdown SHALL prevent new historical passes, preserve all committed progress, stop only at existing safe bounded-work boundaries, and permit restart preflight to reconstruct all unfinished work.

## Requirement: Autonomous convergence acceptance

The implementation SHALL prove with fake REST, fake WebSocket, and temporary SQLite that an empty multi-stream database converges without mandatory administrative backfill, that internal gaps are repaired, that recoverable failures do not lose progress or block other streams, that completed streams enter realtime early, and that aggregate readiness is reached only after all required streams complete historical and realtime proof.
