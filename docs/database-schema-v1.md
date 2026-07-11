# Database Schema v1

## Decision

The first Market Data Service database stays deliberately small. It is a local SQLite store for full available 1-minute history and realtime continuation, not an exchange-grade accounting platform.

## Tables

```text
schema_meta
instruments
streams
candles
stream_state
quarantine
```

## Canonical symbols

The service-facing ticker is the TradingView-style perpetual ticker:

```text
BTCUSDT.P
ETHUSDT.P
```

The exact Bybit API symbol is stored separately:

```text
BTCUSDT.P <-> BTCUSDT
ETHUSDT.P <-> ETHUSDT
```

`category=linear` is source configuration, not repeated in every database row.

## Relationships

```text
instrument 1 -> many streams
stream     1 -> many candles
stream     1 -> one current persisted stream_state
quarantine optionally references a stream
```

## Candle identity

One candle is uniquely identified by:

```text
stream_id + open_time_ms
```

This preserves the old BBB Data Engine semantics of `symbol + timeframe + open_time_ms`, while avoiding repeated ticker/timeframe strings across millions of rows.

## Numeric columns

OHLCV values are stored as canonical non-exponential decimal text. The domain uses Python `Decimal`; equivalent spellings normalize to one text; non-finite values and binary float inputs are rejected. SQLite `REAL` is intentionally avoided so duplicate/correction classification is exact.

## Write behavior

- Missing key: insert.
- Same key and identical normalized OHLCV: duplicate, no write.
- Same key and different OHLCV: quarantine as `candle_correction_detected`, then apply the approved authority policy.
- REST is the repair authority; WebSocket may not silently overwrite a conflicting REST candle.

Candle mutation and the corresponding `stream_state` advancement must be atomic. The persisted lifecycle states are `uninitialized`, `bootstrapping`, `auditing`, `repairing`, `connecting`, `ready`, `degraded`, and `failed`. `state_changed_at_ms` records when the current lifecycle state began.

## Historical transaction boundary

One bounded Bybit REST response window is one transaction. After restart, the service resumes from the latest committed candle and later performs a full continuity audit.

## Cold start

If the database file is absent, the service creates schema v1, registers configured instruments/streams, resolves launch time and earliest available minute candle, loads full history, audits gaps, repairs them, and then enters realtime mode.

If the database exists, the service validates `schema_version`, reconciles configuration, audits stored history, repairs gaps, catches up the trailing interval, and then enters realtime mode.

Unknown schema versions fail closed. Existing files are never silently recreated.

## SQLite baseline

```sql
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA busy_timeout=30000;
PRAGMA foreign_keys=ON;
```

## Explicitly deferred

Schema v1 does not contain:

- market event log;
- consumer cursors;
- bootstrap run/window history;
- persisted gap history;
- candle revision history;
- metadata revision history;
- feature or indicator storage;
- derived timeframe storage.


## Stream lifecycle

The normative lifecycle and restart rules are defined in `docs/stream-state-machine.md`. Persisted state is a recovery snapshot; actual candles are re-audited before readiness is restored after restart.
