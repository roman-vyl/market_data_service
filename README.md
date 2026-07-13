# Market Data Service

Standalone market-data backend for canonical Bybit OHLCV candles.

The service is intentionally isolated from:

- BBB research and VectorBT backtesting;
- strategy feature calculation;
- signal generation;
- Abi Executor;
- order and position management.

Its responsibility is to obtain, validate, repair, persist, and expose canonical closed candles.

The backend is explicitly multi-instrument. The checked-in deployment example enables canonical tickers `BTCUSDT.P` and `ETHUSDT.P`, mapped to Bybit API symbols `BTCUSDT` and `ETHUSDT`, with operator-declared canonical timeframes in `config/markets.toml`.

## Docker config behavior

The Docker image still contains a default checked-in `config/markets.toml`, but
the local `docker-compose.yml` mounts the host file over that path at runtime:

```text
./config/markets.toml -> /app/config/markets.toml (read-only)
```

This keeps the image self-contained while making local operator config changes
cheap:

- edit `config/markets.toml` on the host;
- restart the container with `docker compose restart` or `docker compose up -d`;
- do not rebuild the image unless application code or packaged defaults changed.

The SQLite database remains external to the image and, in local Docker Compose,
is mounted from the repository working tree:

```text
./data -> /data
./data/market.sqlite3 -> /data/market.sqlite3
```

That means the database file is visible directly in the project folder on the
host and survives ordinary image rebuilds or image deletion. For local Docker
Compose, the container reads and writes the same SQLite file that you can
inspect under `./data/`.

## Status

The SQLite vertical slice, Bybit REST market-data adapter, REST smoke verification,
validated market configuration, bounded single-stream and sequential multi-stream
historical backfill, continuity audit CLI, production bounded gap repair workflow,
and real REST smoke coverage are implemented. Canonical
ingestion, duplicate/correction handling, atomic stream-state persistence,
rollback, restart persistence, continuity gaps, repair idempotency, and
multi-stream isolation are covered by integration tests.

Implemented runtime capabilities now include:

- Bybit WebSocket realtime ingestion and recovery;
- `/health` and `/readiness`;
- autonomous bounded historical reconciliation for configured streams;
- per-stream realtime admission after continuous post-audit.

A consumer candle-read API is specified in `openspec/changes/consumer-read-api-v1/` but is not yet implemented. Its first implementation slice is a mandatory architecture/file-responsibility audit; production code must not begin until reuse paths and dependency direction are proven.


## Consumer Read API v1 planning

The approved design target is a backend-to-backend canonical range endpoint:

```text
GET /v1/candles?ticker=BTCUSDT.P&timeframe=5m&from_ms=<inclusive>&to_ms=<exclusive>
```

Version 1 is intentionally unpaginated: one aligned half-open range is returned in one JSON response. OHLCV values remain normalized decimal text. Candle reads are allowed only when the requested configured stream is `ready`, and out-of-bounds or invariant-breaking ranges are rejected rather than truncated.

BBB remains the Workbench BFF. A later BBB change will replace its direct legacy SQLite market reader with an HTTP client while keeping the existing Workbench `/api/market/candles-window` contract. See:

- `openspec/changes/consumer-read-api-v1/`
- `docs/integrations/bbb-consumer-api-current-state.docx`

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
WebSocket realtime delivery is implemented. The external consumer candle-read API is specified but remains unimplemented.

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

To advance every enabled canonical `1m` stream in deterministic configuration
order, use the same explicit budget per stream:

```bash
market-data-service backfill --all --full --max-windows 20
```

The command validates the complete versioned `markets.toml`, verifies each
configured mapping against Bybit linear perpetual metadata before database
mutation, then invokes the existing single-stream bootstrap sequentially. A
recoverable source failure is reported and later streams are still attempted; a
fatal configuration, payload, or storage failure stops the run.


The validated market config may declare multiple canonical timeframes per ticker. `backfill --all --full` expands the config into every enabled `ticker × timeframe` stream and processes them sequentially with an independent window budget and durable stream state.

Example configured streams in the checked-in local config: `BTCUSDT.P:5m`, `BTCUSDT.P:1h`, `BTCUSDT.P:4h`, `BTCUSDT.P:1d`, `ETHUSDT.P:5m`, `ETHUSDT.P:1h`, `ETHUSDT.P:4h`, and `ETHUSDT.P:1d`.

When the service runs through local Docker Compose, changing `markets.toml`
does not hot-reload into the running process. The mounted file is only
reconciled on the next container restart.

## Local smoke commands

The bounded REST smoke commands use temporary SQLite databases by default and do
not touch production persistence:

```text
market-data-service smoke-rest
market-data-service smoke-backfill --minutes 120
market-data-service smoke-all-backfill --max-windows 20
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


`smoke-all-backfill` validates BTCUSDT.P and ETHUSDT.P against real Bybit
metadata, advances both streams sequentially in a temporary SQLite database,
reopens the same database through a second bounded invocation, and confirms
durable independent resume for both streams.

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

## Long-running runtime

Run the complete historical + realtime service process with:

```bash
market-data-service serve
```

Runtime configuration uses CLI options over environment variables over defaults.
The main environment variables are:

```text
MDS_DATABASE_PATH
MDS_MARKETS_CONFIG_PATH
MDS_HTTP_HOST
MDS_HTTP_PORT
MDS_REST_BASE_URL
MDS_WEBSOCKET_URL
MDS_STARTUP_BACKFILL_WINDOWS_PER_STREAM
MDS_STARTUP_REPAIR_WINDOWS_PER_STREAM
MDS_HISTORICAL_RETRY_BASE_SECONDS
MDS_HISTORICAL_RETRY_MAX_SECONDS
MDS_RECONNECT_MAX_ATTEMPTS
MDS_RECONNECT_DELAY_SECONDS
MDS_STALE_INTERVALS
MDS_STALE_GRACE_MS
MDS_LOG_LEVEL
```

Startup processes every enabled `ticker × timeframe` stream in deterministic
configuration order and performs one bounded full-window reconciliation pass.
The existing continuity audit and `RepairStreamGaps` workflow find and repair
prefix, internal, and suffix gaps. A pass budget limits only one turn: incomplete
streams remain owned by the running process and receive later fair sequential
turns until post-audit proves continuity or a fatal failure occurs.

The WebSocket transport subscribes to all configured topics, while canonical
realtime ingestion is gated per stream. Each stream is admitted immediately after
historical continuity is proven, then uses existing realtime recovery to close the
moving tail. Successful tail recovery makes the stream data-ready; a later fresh
confirmed close advances realtime-live diagnostics but does not gate access to
already proven canonical history. Persisted `ready` is never trusted after restart.

Process endpoints:

```text
GET /health
GET /readiness
```

`/health` reports process operation independently from market-data readiness.
`/readiness` returns success only when every required stream has durable
`ready` state and realtime supervisor data-ready facts after recovery.

## Consumer candle read API

A ready canonical stream can be read through:

```bash
curl 'http://127.0.0.1:8080/v1/candles?ticker=BTCUSDT.P&timeframe=5m&from_ms=1710000000000&to_ms=1710000300000'
```

The range is aligned and half-open. OHLCV values are normalized decimal strings. Version 1 returns the complete requested range in one JSON response and intentionally has no pagination, cursor, or response chunking. Non-ready streams and out-of-bounds ranges never return partial candle data. The maintained schema is available at `/openapi.json`; see `docs/consumer-read-api-v1.md` for the full contract and BBB integration boundary.
