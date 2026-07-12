# Specification: Runtime Startup Orchestration v1

## Requirement: Thin startup coordinator

The service runtime SHALL coordinate existing application use cases and SHALL NOT reimplement bootstrap, continuity, repair, ingestion, or readiness rules.

## Requirement: Finite startup recovery

Startup SHALL process enabled streams in deterministic order and SHALL use explicit finite REST-window budgets. Startup SHALL NOT silently begin unlimited deep-history bootstrap.

Interrupted `bootstrapping`, `auditing`, `repairing`, or `connecting` states SHALL be recovered according to the existing persisted lifecycle contract before new work proceeds.

## Requirement: Failure isolation

A recoverable failure for one stream SHALL leave that stream degraded and SHALL NOT prevent independent configured streams from being initialized. Fatal configuration, schema, or process-invariant failures SHALL make the process unhealthy.

## Requirement: Health and readiness distinction

The runtime SHALL expose process health independently from market-data readiness.

Aggregate readiness SHALL be true only when every enabled required stream is ready. The readiness response SHALL expose every stream's state and blocking reason.

## Requirement: Graceful shutdown

The runtime SHALL stop new work, terminate background tasks, close network clients, and close the single SQLite owner cleanly. Already committed UoWs SHALL remain durable.

## Requirement: Docker persistence

The production container SHALL run one service-owner process and SHALL store SQLite data on a persistent mounted volume. Container restart SHALL preserve canonical candles and stream state.
