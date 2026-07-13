# Runtime Startup Orchestration v1 Delta

## Requirement: Continuous bounded startup reconciliation

Runtime startup SHALL execute one bounded historical reconciliation pass per configured stream in deterministic order. Streams returning `INCOMPLETE` or recoverable failure SHALL remain owned by the running process and SHALL be transferred to continuous bounded reconciliation instead of being abandoned after the startup pass.

Startup budgets SHALL limit each pass, not the lifetime amount of historical work.

## Requirement: Historical proof before realtime

A configured stream SHALL enter realtime admission only after the existing full-window audit/repair workflow proves continuity through a fixed latest-closed target. Prefix, internal, and suffix gaps SHALL all block admission.

## Requirement: Failure isolation

Incomplete or recoverably failed streams SHALL remain scheduled without blocking completed streams from entering realtime. Fatal failure of one stream SHALL not terminate historical or realtime work for other valid streams.

## Requirement: Process lifetime

After the initial deterministic startup pass, the process SHALL run continuous historical reconciliation together with realtime connector, stale checking, recovery, and HTTP serving. The runtime SHALL converge incomplete configured streams without requiring administrative backfill or repeated restarts.

## Requirement: Docker persistence and restart

A container started on an empty persistent database SHALL autonomously continue bounded reconciliation until configured streams are historically complete or fatally failed. Restart on the same database SHALL reconstruct and continue unfinished work from canonical SQLite state.
