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

The SQLite vertical slice, Bybit REST market-data adapter, REST smoke verification,
bounded historical backfill runner, continuity audit CLI, production bounded
gap repair workflow, and real REST repair smoke are implemented. Canonical
ingestion, duplicate/correction handling, atomic stream-state persistence,
rollback, restart persistence, continuity gaps, repair idempotency, and
multi-stream isolation are covered by integration tests.

Not implemented yet:

- Bybit WebSocket realtime ingestion;
- external HTTP API;
- live consumer runtime.

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

The contracts now have SQLite persistence plus bounded Bybit REST ingestion.
WebSocket realtime delivery and the external HTTP API remain deferred.

## Step 2 decision

Instrument and stream semantics are normative: `InstrumentKey = ticker`; `StreamKey = InstrumentKey + validated timeframe`. The Bybit API symbol is an explicit mapping, not part of identity. See `docs/instrument-stream-semantics.md`.

## Database baseline

The approved SQLite v1 design is documented in `docs/database-schema-v1.md`. Its canonical tickers are `BTCUSDT.P` and `ETHUSDT.P`, mapped explicitly to Bybit API symbols `BTCUSDT` and `ETHUSDT`.

## Step 4 decision

OHLCV values use exact `Decimal` semantics in the domain and normalized non-exponential decimal `TEXT` in SQLite and JSON APIs. Binary float input is rejected at the canonical ingestion boundary. See `docs/decimal-policy.md`.

## Step 6 decision

Consumer recovery is readiness-first. Bootstrap, catch-up, and repair do not require per-candle consumer events. A consumer owns its own per-stream `last_processed_open_time_ms`, pauses decisions while the stream is not `ready`, and catches up by ordered range read when readiness returns. Schema v1 has no event log or server-owned consumer cursor. See `docs/consumer-readiness-contract.md`.

## Sequential REST backfill

Version 1 performs historical REST work sequentially and in finite command-sized chunks. One REST response window is committed atomically. Deep bootstrap is resumable from the latest committed candle and does not require a parallel scheduler. Continuity is proven later by audit. See `docs/sequential-backfill.md`.

For a full available `1m` bootstrap of one configured stream, use the bounded
operator command:

```bash
market-data-service backfill --ticker BTCUSDT.P --full --max-windows 100
```

The command resolves and caches the observed earliest available candle before
backfill. `launchTime` remains exchange metadata and is not treated as proof
that a candle exists at that timestamp. For `--full`, `--max-windows` is the
total historical-candle REST-window budget shared by lower-bound discovery and
backfill; instrument metadata requests do not count against it. If the explicit
window budget is exhausted, committed candles remain durable, the stream stays
bootstrapping, and the next invocation resumes after the latest committed
candle once the lower bound has been cached.

## Local smoke commands

The bounded REST smoke commands use temporary SQLite databases by default and do
not touch production persistence:

```text
market-data-service smoke-rest
market-data-service smoke-backfill --minutes 120
market-data-service smoke-full-bootstrap --max-windows 20
market-data-service smoke-audit-continuity --minutes 120
market-data-service smoke-gap-repair --minutes 5
market-data-service audit-continuity --ticker BTCUSDT.P --start 0 --end 3600000
```

`smoke-backfill` fetches a small closed BTCUSDT 1m interval from Bybit REST,
runs `BackfillStreamHistory`, replays the same window to prove duplicate
classification, reopens SQLite for persistence checks, and performs a basic
1m continuity assertion. This assertion is smoke-only; full continuity proof is
performed by `AuditStreamContinuity`.

`smoke-full-bootstrap` uses real Bybit REST and a temporary SQLite database to
resolve the observed BTCUSDT.P `1m` lower bound, run full-history bootstrap
with a small shared window budget, reopen through a fresh workflow, run again,
and verify that every invocation stays within `max_windows`, cached discovery
uses zero candle windows, and backfill resumes from durable progress.

`audit-continuity` reads canonical candles for one explicit stream and
half-open range, reports bounded missing intervals, and does not change stream
state or attempt repair.

`smoke-audit-continuity` uses a temporary SQLite database, real Bybit REST
backfill, and `AuditStreamContinuity` over the same bounded range.

`smoke-gap-repair` uses a temporary SQLite database, real bounded Bybit REST
backfill, smoke-only deletion of one internal candle, production
`RepairStreamGaps`, and a post-repair audit. The post-repair audit determines
whether repair is complete or incomplete.

```bash
python3 -m market_data_service smoke-gap-repair --minutes 5
```

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
