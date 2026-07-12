# Proposal: Runtime Startup Orchestration v1

## Why

The service now has production historical bootstrap/audit/repair and production WebSocket realtime ingestion/recovery, but they still run as separate commands and use cases. A long-running service process is required to load validated configured streams, reconcile durable history, start realtime supervision, expose health/readiness, and shut down safely.

## What changes

Add the first complete service runtime:

- validated environment/CLI settings and configured-stream loading;
- deterministic per-stream historical startup reconciliation using existing bootstrap, audit, and repair use cases;
- one realtime connector/supervisor/recovery loop using the existing WebSocket subsystem;
- strict per-stream and aggregate readiness projection from durable and realtime facts;
- `/health` and `/readiness` HTTP endpoints;
- structured runtime logging and graceful shutdown;
- Docker service command and persistent-volume compose setup;
- restart and failure-isolation smoke coverage.

## What does not change

- no consumer candle API;
- no periodic REST polling scheduler;
- no unlimited deep bootstrap at startup;
- no event log or server-owned consumer cursor;
- no second ingestion, gap, repair, or readiness implementation.
