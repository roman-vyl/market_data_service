# ADR-010: Sequential bounded backfill

**Status:** Accepted

## Context

Full minute history requires many REST windows. Parallel scheduling adds complexity without removing the fundamental cost of deep bootstrap.

## Decision

REST backfill is sequential by default and bounded by explicit command options. One REST response window is one storage transaction. Commands may target one ticker or all configured streams in deterministic order.

## Consequences

- Predictable rate-limit behavior and logs.
- Simple SQLite write pattern.
- Restart resumes from the latest committed candle in stream state.
- Resume progress does not prove continuity; audit remains responsible for gaps.
- Parallelism may be added only after measurements justify it.

## Rejected alternatives

- Mandatory concurrent workers.
- Distributed queues or Redis scheduler.
- Unbounded automatic deep bootstrap on daemon startup.
