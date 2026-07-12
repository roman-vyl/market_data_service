# Design: Runtime Startup Orchestration v1

## Runtime responsibility

The runtime is a thin process coordinator. It wires existing validated configuration, SQLite, historical use cases, realtime connector, supervisor, recovery coordinator, readiness projections, and process adapters. It SHALL NOT reimplement candle ingestion, bootstrap, continuity, repair, WebSocket parsing, supervision, or recovery rules.

## Module decomposition

Runtime concerns SHALL remain separate:

- `runtime/settings.py` owns environment/CLI parsing and validation;
- `runtime/startup.py` owns deterministic one-shot historical reconciliation;
- `runtime/realtime.py` owns event/outcome/recovery dispatch between existing realtime components;
- `runtime/status.py` owns thread-safe health/readiness snapshots;
- `adapters/http/runtime_server.py` owns `/health` and `/readiness` transport;
- `entrypoints/serve.py` owns construction, signal handling, and process lifetime.

No runtime module may become a replacement for the existing application use cases.

## Settings and precedence

Runtime settings use this precedence:

```text
explicit CLI option
→ environment variable
→ documented default
```

The v1 settings are:

- database path;
- markets config path;
- HTTP host and port;
- Bybit REST and WebSocket URLs;
- startup historical window budget per stream;
- startup repair window budget per stream;
- realtime reconnect attempts/delay;
- stale interval count and grace;
- log level.

All budgets and timeouts SHALL be validated before network or storage mutation.

## Startup sequence

The process SHALL:

1. validate settings and `markets.toml`;
2. initialize/validate SQLite and register every enabled `ticker × timeframe` stream;
3. verify configured exchange metadata;
4. process streams in deterministic configuration order;
5. for each stream, recover interrupted durable state and invoke bounded full bootstrap;
6. when the historical target is reached, audit the complete persisted range;
7. invoke bounded repair only when audit reports gaps;
8. require a successful post-repair audit;
9. persist `connecting` only for historically reconciled streams;
10. construct the realtime supervisor with each stream's durable latest open time;
11. start the existing realtime connector;
12. dispatch `RecoveryRequired` signals to the existing realtime recovery coordinator;
13. project readiness continuously from durable state plus realtime supervisor facts.

Startup SHALL NOT silently perform unlimited multi-year work. Each stream receives explicit startup backfill and repair budgets.

## Interrupted-state recovery

The startup coordinator SHALL use durable facts rather than resume transient plans:

- `uninitialized` and `bootstrapping`: continue bounded full bootstrap;
- `auditing`: repeat audit;
- `repairing`: repeat audit first, then repair only actual gaps;
- `connecting`, `ready`, and `degraded`: repeat historical reconciliation before realtime start;
- `failed`: do not silently retry unless the existing state contract and current validated inputs permit explicit audit recovery; otherwise report fatal startup outcome.

Persisted `ready` is never trusted across process restart.

## Realtime orchestration

The runtime owns dispatch only:

- connector events go to the supervisor;
- ingestion outcomes go to the supervisor;
- emitted `RecoveryRequired` values go to the recovery queue;
- recovery results go back to the supervisor and durable lifecycle recorder;
- stale checks run at a small bounded cadence using the supervisor's configured policy.

REST recovery SHALL NOT execute inside WebSocket transport callbacks. Recovery work is serialized per stream and duplicate pending recovery signals are coalesced.

## Readiness

A stream is ready only when both are true:

1. durable lifecycle is `ready`;
2. realtime supervisor facts report `realtime_ready=true`.

Historically reconciled streams enter `connecting`. After subscription, successful recovery reconciliation, and a fresh confirmed close, runtime persists `ready`. Disconnect, stale, rejected observations, recovery pending, or fatal ingestion failure makes the stream not ready and persists degraded/failed state according to the existing lifecycle contract.

Aggregate readiness is true only when every enabled required stream is ready.

## Process surfaces

- `/health` returns HTTP 200 only when configuration, SQLite initialization, startup coordinator, HTTP server, and runtime loop are operational. Fatal process errors return HTTP 503.
- `/readiness` returns HTTP 200 only when aggregate readiness is true; otherwise HTTP 503. Its JSON includes every configured stream, durable state, realtime status, ready flag, and blocking reason.

Health does not imply readiness.

## Process lifetime and shutdown

Historical startup reconciliation runs once. After it completes, the process runs the realtime connector, stale checker, recovery worker, and HTTP server. No periodic deep-history scheduler is introduced.

SIGINT/SIGTERM SHALL stop accepting HTTP work, signal realtime cancellation, allow bounded in-flight recovery to finish or cancel cleanly, close WebSocket and HTTP transports, and release process resources. Already committed SQLite UoWs remain durable.

## Docker

The image runs one `market-data-service serve` process. SQLite data is mounted on a persistent volume. Config and runtime settings are explicit. Restart preserves canonical candles and stream state and forces historical plus realtime reconciliation before readiness.
