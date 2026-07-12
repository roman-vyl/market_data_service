# Design: Market Data Service v1

## Context

The service is an independent source of canonical closed candles. It does not know about strategies, indicators, signals, orders, positions, or consumer topology.

## Layers

```text
domain
  ticker/stream identity, candle, timeframe, windows, gaps, validation

application
  ingest candle, discover history floor, backfill, audit, repair, readiness

ports
  market-data source, canonical storage unit of work, clock

adapters
  SQLite, Bybit REST, Bybit WebSocket, later HTTP read API

entrypoints
  service runtime and optional administrative CLI
```


## Implemented baseline and delegated future work

As of the current reconciliation, the production code implements the complete bounded historical core: canonical ingestion, SQLite atomicity, REST backfill, observed historical lower-bound discovery, shared full-bootstrap budget, continuity audit, gap repair, and post-repair audit.

The base change remains the architectural source of truth for identity, storage, ingestion, ranges, lifecycle states, and readiness semantics. Future process-level capabilities are specified in separate changes so they do not become implicit implementation details:

- `complete-phase2-operations-v1` completes configuration validation, source-failure classification, metadata verification, and sequential multi-stream administrative orchestration.
- `runtime-startup-orchestration-v1` owns process startup, configured-stream coordination, health/readiness surfaces, shutdown, logging, and Docker runtime.
- `websocket-realtime-recovery-v1` owns confirmed-close realtime ingestion, reconnect, stale detection, and REST catch-up.
- `consumer-read-api-v1` owns deterministic candle reads, OpenAPI, readiness gating, and consumer catch-up contracts.
- `hardening-operations-v1` owns failure injection, metrics, long-running validation, runbooks, and database maintenance policy.

No delegated change may introduce a second candle ingestion path, a second gap algorithm, server-owned consumer cursors, or an event-log requirement without a new approved architectural decision.

## Identity

Version 1 uses canonical perpetual tickers:

```text
BTCUSDT.P
ETHUSDT.P
```

The exact Bybit API symbols are explicit mappings:

```text
BTCUSDT.P <-> BTCUSDT
ETHUSDT.P <-> ETHUSDT
```

`InstrumentKey = ticker`. `StreamKey = ticker + timeframe`.

## Ingestion flow

```text
REST/WS payload
  -> adapter normalization
  -> ObservedCandle
  -> validation
  -> duplicate/correction classification
  -> atomic SQLite transaction
       - candle insert or approved correction action
       - quarantine diagnostic when required
       - stream_state update
```

REST and WebSocket adapters must not write directly to storage.

## SQLite schema v1

The approved schema is deliberately small:

```text
schema_meta
instruments
streams
candles
stream_state
quarantine
```

The normative DDL is `src/market_data_service/adapters/sqlite/schema_v1.sql`.

Version 1 intentionally has no event-log table, consumer cursor, bootstrap-window history, persisted gap history, candle-revision history, metadata-revision history, or feature storage.

## Duplicate and correction behavior

- missing `(stream_id, open_time_ms)`: insert;
- identical normalized OHLCV: no-op duplicate;
- conflicting OHLCV: never silently overwrite; persist quarantine diagnostics and apply the approved source-authority policy;
- REST is the repair authority;
- WebSocket may not overwrite a conflicting REST candle silently.

## Transaction boundaries

A realtime candle mutation and its `stream_state` update share one transaction.

One bounded Bybit REST response window is the historical transaction boundary. Restart resumes from the latest committed candle and is followed by continuity audit and repair. The resume point is durable progress, not proof that the historical grid is gap-free.

## Recovery

REST is used for initial population, startup catch-up, explicit backfill, gap repair, and reconnect repair. WebSocket is used for low-latency delivery of confirmed closes. Both use the same canonical ingestion path.

Gap repair is an application use case with this observable sequence:

```text
bounded continuity audit
  -> detected half-open gaps
  -> bounded REST windows
  -> canonical ingestion
  -> post-repair continuity audit
```

The repair result reports the preflight audit, every attempted bounded window,
canonical ingestion counts, unexpected-row diagnostics, the post-repair audit,
and a status of complete, incomplete, or failed. Empty or partial REST responses
are not success by themselves; the post-repair audit is the source of truth.
Rows outside the requested stream or half-open window are quarantined and are
not inserted into canonical storage. Repair may exhaust its explicit window
budget and return incomplete without inventing durable job or gap tables.

Repair owns lifecycle transitions only between `auditing` and `repairing`.
It may move a stream to `degraded` or `failed` on classified failure, but a
successful repair returns to `auditing` and never directly to `connecting` or
`ready`.

## Storage operation

SQLite has one service-owner process. The baseline pragmas are WAL, synchronous NORMAL, 30-second busy timeout, and foreign keys enabled.

Unknown schema versions fail closed. Existing databases are never silently recreated.

## Precision

OHLCV values are Python `Decimal` in the domain and canonical non-exponential decimal `TEXT` in SQLite and JSON APIs. Equivalent spellings such as `1.0`, `1.000`, and `1E+0` normalize to `1`. Non-finite values and binary float input are rejected. Duplicate/correction classification compares canonical OHLCV text.


## Persisted stream lifecycle

The lifecycle contract is implemented as small pure domain modules, not a universal manager. `domain/stream_state.py` owns state names and legal transitions; `domain/readiness.py` owns readiness projection. Application use cases justify transitions, while SQLite only persists validated snapshots.

Normal initialization is `uninitialized -> bootstrapping -> auditing -> connecting -> ready`, with `auditing -> repairing -> auditing` when gaps exist. Temporary REST failure during bootstrap uses `bootstrapping -> degraded -> bootstrapping`; fatal invariant or storage failures use `failed`. Runtime failure uses `ready -> degraded -> auditing|connecting -> ready`. Persisted ready is always re-proven after restart.

## Readiness-first consumer contract

Schema v1 has no event log or server-owned consumer cursor. Bootstrap, catch-up, and repair write canonical storage while the stream remains not ready. Consumers own their own last-processed candle cursor, pause decisions whenever readiness is false, and catch up through ordered candle range reads when readiness returns. Any future push transport is a latency hint only.

## Sequential backfill

Version 1 uses finite sequential REST runs rather than a parallel scheduler. One bounded window is the unit of fetch, ingestion, and atomic commit. Administrative commands may select one stream or all streams in deterministic configuration order and must enforce an explicit per-run window budget. For full-history bootstrap, the per-run `max_windows` budget is shared by lower-bound discovery and backfill historical-candle REST windows; instrument metadata requests do not count as candle windows. Normal service startup does not perform unlimited deep bootstrap.
