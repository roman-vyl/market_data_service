# Design: Runtime Startup Orchestration v1

## Runtime responsibility

The runtime is a thin process coordinator. Domain rules remain in domain modules; bootstrap, audit, repair, and readiness projection remain application capabilities. Adapters remain responsible for SQLite, Bybit, clock, and HTTP transport.

## Startup sequence

For each enabled required stream in deterministic configuration order:

1. load persisted state;
2. recover interrupted lifecycle according to the existing state-machine contract;
3. perform only the explicitly configured bounded startup catch-up budget;
4. run continuity audit;
5. run bounded repair when gaps exist;
6. project stream readiness from persisted facts;
7. continue to the next stream even when another stream is recoverably degraded.

Startup SHALL NOT silently initiate unlimited multi-year bootstrap. If full historical bootstrap remains incomplete, the stream stays not ready and the service remains healthy but not fully ready.

## Process surfaces

- `/health` reports process liveness and critical dependency initialization.
- `/readiness` reports aggregate readiness and per-stream readiness/reason.

Health does not imply readiness.

## Shutdown

Shutdown stops accepting new work, cancels runtime tasks, closes network clients, and releases the single SQLite owner cleanly. A committed UoW remains durable; no open network request may own a long SQLite transaction.

## Docker

The image runs one service-owner process. SQLite data is mounted on a persistent volume. Configuration and environment settings are explicit and validated at startup.
