## Why

During nine hours MDS performed 617,582 lifecycle sweeps and 24.7 million snapshot reads. Intermediate candle updates and transport activity trigger these sweeps even though their direct mutations only refresh transport timestamps.

## What Changes

- Intermediate unconfirmed candles, heartbeats and activity-only Connected events update supervisor transport facts and return before lifecycle synchronization.
- The existing periodic stale worker continues a full sweep of every configured stream at its configured interval, one second by default. Time-driven public projection changes become observable on that sweep.
- An OI publication step that changes its stream frontier or recovery requirement explicitly synchronizes that stream before evaluating publication.
- Confirmed candles keep both their pre-ingestion and post-outcome full sweeps. Recovery, admission and disconnect paths retain their current synchronization.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `runtime-health-and-readiness`: define synchronization triggers for transport-only activity, periodic time changes and OI frontier updates.

## Impact

Expected changes are limited to runtime event dispatch, the existing lifecycle synchronization method, OI publication wiring and regression tests. The same supervisor, readiness predicates and lifecycle recorder remain responsible. Estimated snapshot traffic falls from 24.7 million to about 1.4 million per nine hours under the observed workload. This is an estimate to validate after a separately approved deployment.
