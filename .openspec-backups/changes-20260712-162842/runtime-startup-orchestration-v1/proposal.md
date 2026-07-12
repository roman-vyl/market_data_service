# Proposal: Runtime Startup Orchestration v1

## Why

The codebase has production historical use cases and persisted lifecycle semantics, but it does not yet have a long-running service process that loads configured streams, coordinates finite startup recovery, exposes health/readiness, or shuts down safely.

## What changes

Add the first service runtime:

- environment settings and validated configured-stream loading;
- per-stream startup orchestration using existing bootstrap, audit, and repair use cases;
- strict aggregate and per-stream readiness surfaces;
- health and readiness HTTP endpoints;
- structured logging and graceful shutdown;
- Docker runtime image and persistent-volume compose setup;
- restart smoke coverage.

## What does not change

- no WebSocket ingestion in this change;
- no consumer candle API;
- no unlimited deep bootstrap at startup;
- no background REST worker scheduler;
- no event log or server-owned consumer cursor.
