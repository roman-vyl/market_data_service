# ADR-010: Sequential bounded backfill

**Status:** Accepted

## Context

Full minute history requires many REST windows. Parallel scheduling adds complexity without removing the fundamental cost of deep bootstrap.

## Decision

REST backfill is sequential by default and bounded by explicit command options. Commands may target one ticker or all configured streams in deterministic order.

## Consequences

- Predictable rate-limit behavior and logs.
- Simple SQLite write pattern.
- Restart resumes from persisted candles and stream state.
- Parallelism may be added only after measurements justify it.

## Rejected alternatives

- Mandatory concurrent workers.
- Distributed queues or Redis scheduler.
- Unbounded automatic deep bootstrap on daemon startup.
