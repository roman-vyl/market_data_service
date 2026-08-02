# Database Schema v2

## Decision

Schema v2 preserves the deliberately small six-table SQLite model from v1 and adds one durable, per-stream lower-bound discovery cursor. The database remains canonical candle storage and current operational state; it is not a job queue or event log.

The normative DDL for a fresh database is `src/market_data_service/adapters/sqlite/schema_v2.sql`. Existing schema-v1 databases are migrated forward transactionally. Unknown versions fail closed and are not recreated.

## Tables

```text
schema_meta
instruments
streams
candles
stream_state
quarantine
```

Schema v2 does not add event logs, consumer cursors, bootstrap jobs, recovery queues, persisted gap journals, candle revision history, features, or indicators.

## Stream state addition

`stream_state.lower_bound_discovery_next_open_time_ms` is the aligned start of the next lower-bound probe for that exact `StreamKey`.

The field is:

- nullable when discovery has not advanced or is complete;
- non-negative and aligned to the stream timeframe when present;
- isolated by ticker and timeframe;
- mutually exclusive with `earliest_available_open_time_ms`.

After a successfully classified empty source window, the next probe cursor is committed in its own transaction. A source, classification, or storage failure does not advance it. When the first valid candle is observed, its open time becomes `earliest_available_open_time_ms` and the probe cursor is cleared in the same transaction.

The cursor is bounded progress, not a persisted work item. On restart, the ordinary lower-bound use case reads it and continues. Once the lower bound is resolved, normal continuity preflight reconstructs all remaining bootstrap and repair work from canonical candles.

## Migration from v1

The supported migration:

1. acquires an immediate transaction;
2. creates the v2 `stream_state` shape and constraints;
3. copies all v1 stream-state rows with a null discovery cursor;
4. replaces the v1 table;
5. advances `schema_meta.schema_version` to `2`;
6. commits atomically.

Failure rolls the transaction back, preserving the v1 version and canonical data. There are no dual reads or compatibility paths after migration.

## Preserved invariants

- Candle identity is `(stream_id, open_time_ms)`.
- OHLCV is canonical decimal text, never SQLite `REAL`.
- Candle mutation and corresponding stream progress commit atomically.
- One bounded REST response window is the historical transaction boundary.
- WAL, foreign keys, normal synchronous mode, and the configured busy timeout remain required.
- Persisted `ready` is never trusted after restart without reconciliation.

Lifecycle semantics are defined in `docs/stream-state-machine.md`; numeric semantics are defined in `docs/decimal-policy.md`.
