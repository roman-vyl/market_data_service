# Market Data Service

Standalone market-data backend for canonical Bybit OHLCV candles.

The service is intentionally isolated from:

- BBB research and VectorBT backtesting;
- strategy feature calculation;
- signal generation;
- Abi Executor;
- order and position management.

Its responsibility is to obtain, validate, repair, persist, and expose canonical closed candles.

The backend is explicitly multi-instrument. The checked-in deployment example enables canonical tickers `BTCUSDT.P` and `ETHUSDT.P`, mapped to Bybit API symbols `BTCUSDT` and `ETHUSDT`, each with full available canonical `1m` history.

## Status

Phase 1 SQLite vertical slice is implemented. Canonical ingestion, duplicate/correction handling, atomic stream-state persistence, rollback, restart persistence, and multi-stream isolation are covered by integration tests. Bybit connectivity and HTTP API are not implemented yet.

## Planned first vertical slice

```text
Bybit REST candle
  -> normalize
  -> validate
  -> atomic SQLite commit
       - canonical candle
       - stream state
  -> repository readback
```

See:

- `docs/master-plan.md`
- `docs/source-reuse-audit.md`
- `docs/audits/old-bbb-data-engine-file-audit.md`
- `docs/ported-semantics.md`
- `docs/multi-symbol-model.md`
- `docs/instrument-stream-semantics.md`
- `docs/decimal-policy.md`
- `docs/sqlite-vertical-slice.md`
- `docs/bybit-rest-adapter.md`
- `config/markets.toml`
- `openspec/changes/market-data-service-v1/`

## Repository rules and operational contracts

- `AGENTS.md` — mandatory architecture and implementation rules for agents and contributors.
- `.cursor/rules/` — always-on Cursor rules mirroring the repository contract.
- `docs/operational-scenarios.md` — cold start, lower-bound discovery, gap repair, reconnect, crash recovery, and readiness scenarios.
- `docs/stream-state-machine.md` — persisted per-stream lifecycle, restart behavior, legal transitions, and strict readiness.
- `docs/consumer-readiness-contract.md` — readiness-first consumer recovery without an event log or replay broker.

## Core data policy

- Every configured symbol has a mandatory canonical `1m` stream.
- The default goal is the full minute history Bybit actually exposes.
- Instrument `launchTime` is a discovery floor, not proof of the first candle.
- Full bootstrap is resumable and followed by a complete continuity audit.
- Readiness is the consumer processing gate: when a stream is not `ready`, consumers must pause decisions and later catch up by range read from their own cursor.

## Architecture map

See `docs/architecture.md` for package boundaries and dependency rules.
See `docs/operational-scenarios.md` for cold-start, repair, reconnect, and readiness contracts.


## Audited contract skeleton

The skeleton now encodes the strongest preserved old-engine semantics in code:

- canonical `InstrumentKey` and `StreamKey`;
- mandatory `1m` timeframe registry;
- half-open `TimeWindow`;
- deterministic grid math;
- pure gap detection;
- bounded REST fetch-window planning;
- separate `ObservedCandle` and `CanonicalCandle`;
- explicit ingestion classifications;
- named application use-case and infrastructure port boundaries.

The contracts now have a first SQLite persistence implementation; network behavior remains deferred.

## Step 2 decision

Instrument and stream semantics are normative: `InstrumentKey = ticker`; `StreamKey = InstrumentKey + validated timeframe`. The Bybit API symbol is an explicit mapping, not part of identity. See `docs/instrument-stream-semantics.md`.

## Database baseline

The approved SQLite v1 design is documented in `docs/database-schema-v1.md`. Its canonical tickers are `BTCUSDT.P` and `ETHUSDT.P`, mapped explicitly to Bybit API symbols `BTCUSDT` and `ETHUSDT`.

## Step 4 decision

OHLCV values use exact `Decimal` semantics in the domain and normalized non-exponential decimal `TEXT` in SQLite and JSON APIs. Binary float input is rejected at the canonical ingestion boundary. See `docs/decimal-policy.md`.

## Step 6 decision

Consumer recovery is readiness-first. Bootstrap, catch-up, and repair do not require per-candle consumer events. A consumer owns its own per-stream `last_processed_open_time_ms`, pauses decisions while the stream is not `ready`, and catches up by ordered range read when readiness returns. Schema v1 has no event log or server-owned consumer cursor. See `docs/consumer-readiness-contract.md`.

## Sequential REST backfill

Version 1 performs historical REST work sequentially and in finite command-sized chunks. Deep bootstrap is resumable and does not require a parallel scheduler. See `docs/sequential-backfill.md`.

## Architecture decisions

See `docs/adr/README.md`.

## Step 10 implementation

The first network-free vertical slice now includes schema creation/version validation, small SQLite repositories, an atomic unit of work, canonical ingestion, duplicate detection, REST-authoritative correction handling with quarantine, rollback, restart persistence, and BTC/ETH stream isolation.

## Current implementation milestone

The project now contains a bounded Bybit V5 REST candle adapter and one-window import path:

```text
Bybit REST kline window
  -> transport-neutral ObservedCandle
  -> canonical ingestion
  -> SQLite
```

See `docs/bybit-rest-adapter.md`.
