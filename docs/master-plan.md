# Market Data Service — Master Plan

## 1. Purpose

Build a standalone service that obtains canonical closed OHLCV candles from Bybit, validates and repairs the time series, durably persists accepted candles, and exposes canonical candle read APIs to independent consumers.

The service is a source of market facts. It is not a trading orchestrator.

## 2. Repository and deployment boundary

The service lives in its own repository beside the existing projects:

```text
BBB_project/
├── _bbb_new_gen/
├── abi_executor_bot/
└── market_data_service/
```

It has:

- its own Python package;
- its own database;
- its own Docker image;
- its own release lifecycle;
- no runtime import dependency on BBB;
- no runtime import dependency on Abi Executor.

The existing BBB Data Engine remains unchanged. BBB research continues to use its current historical-data pipeline during this phase.

## 3. Responsibilities

The service owns:

1. Bybit public market-data connectivity.
2. REST candle fetches for bootstrap, backfill, audit, and repair.
3. WebSocket subscriptions for realtime delivery.
4. Canonical instrument, stream, timeframe, and candle contracts.
5. Closed-candle detection.
6. Candle normalization and validation.
7. Deduplication and correction detection.
8. Gap detection and repair.
9. Durable candle persistence.
10. Stream ingestion state.
11. Historical and incremental candle read APIs.
12. Health, readiness, logging, and metrics.
13. Graceful startup, reconnect, recovery, and shutdown.

## 4. Non-goals

Version 1 does not:

- calculate EMA, RSI, ATR, ADX, DMI, or other features;
- understand strategies, setups, entries, exits, or positions;
- generate trade signals;
- call Abi Executor;
- execute or manage exchange orders;
- replace or migrate BBB research storage;
- expose unconfirmed in-progress candles as canonical data;
- provide a generic multi-exchange platform;
- precompute or persist strategy indicators;
- orchestrate downstream consumers through consumer-specific callbacks.

## 4.1 Audited old-engine semantics adopted into the skeleton

The Step 1 audit has been applied to code and documentation. The skeleton now
contains explicit contracts for:

- half-open `[start_ms, end_ms)` windows;
- canonical timeframe registry with mandatory `1m`;
- deterministic grid math;
- pure gap detection over unsorted duplicate input;
- bounded aligned REST fetch-window planning;
- stable instrument and stream identities;
- observed versus canonical candles;
- explicit ingestion classifications;
- historical metadata and candle-source ports;
- atomic unit-of-work boundary.

The complete preserved/rejected semantic list is normative in
`docs/ported-semantics.md`.

## 4A. Instrument and stream identity decision

Version 1 uses compact ticker identity:

```text
InstrumentKey = ticker
StreamKey = ticker + registered timeframe
```

Canonical tickers are `BTCUSDT.P` and `ETHUSDT.P`. Their exact Bybit API symbols are explicit mappings to `BTCUSDT` and `ETHUSDT`. Bybit `category=linear` is configured once at the source-adapter level and is not repeated in every domain key or database row. Every enabled instrument must include canonical `1m`. See `docs/instrument-stream-semantics.md`.

## 5. Fundamental architecture rule

Both Bybit transports must converge into one canonical ingestion pipeline:

```text
Bybit REST --------┐
                   ├─> normalize -> validate -> classify -> atomic commit
Bybit WebSocket ---┘
```

REST and WebSocket adapters must not write directly to the database.

They produce transport-neutral observed-candle objects. One application use case classifies each observation as:

- invalid;
- new canonical candle;
- exact duplicate;
- correction;
- unexpected stream;
- out-of-order observation.

## 6. Canonical candle semantics

Version 1 publishes only confirmed closed candles.

A candle is identified by:

```text
ticker
timeframe
open_time_ms
```

Initial supported scope:

- venue: `bybit`;
- market category: `linear`;
- multiple explicitly configured instruments;
- initial examples: BTCUSDT and ETHUSDT USDT-settled perpetuals;
- `1m` candles are mandatory canonical data for every configured symbol;
- additional timeframes may be configured after their storage/derivation policy is approved.

A confirmed WebSocket close is only an observation. It becomes canonical only after validation and successful SQLite commit.

## 6.1 Multi-instrument configuration and identity

Market coverage is declared in `config/markets.toml`. Adding a supported symbol is a configuration change, not a new ingestion pipeline.

```text
BTCUSDT.P <-> BTCUSDT -> 1m full available history
ETHUSDT.P <-> ETHUSDT -> 1m full available history
```

All bootstrap, gap, repair, realtime subscription, and readiness state is scoped by `StreamKey`. Global singleton state for one current symbol is prohibited.

## 7. Data ownership and storage

Version 1 uses SQLite owned exclusively by the Market Data Service process. Other services do not mount or read the database file directly.

The approved schema contains only:

```text
schema_meta
instruments
streams
candles
stream_state
quarantine
```

`instruments` stores the canonical ticker, exact Bybit API symbol, and current launch-time metadata. `streams` registers ticker/timeframe rows. `candles` stores canonical OHLCV by `(stream_id, open_time_ms)`. `stream_state` stores current per-stream operational state. `quarantine` durably records rare ingestion and repair problems. The persisted lifecycle is deliberately small: `uninitialized`, `bootstrapping`, `auditing`, `repairing`, `connecting`, `ready`, `degraded`, and `failed`; see `docs/stream-state-machine.md`.

The normative schema is documented in `docs/database-schema-v1.md` and defined by `src/market_data_service/adapters/sqlite/schema_v1.sql`.

Schema v1 intentionally excludes event logs, consumer cursors, bootstrap-run/window history, persisted gap history, candle revision history, metadata revision history, and feature storage.

## 8. Atomic commit invariant

For realtime ingestion, the candle mutation and corresponding `stream_state` update occur in one transaction.

For historical ingestion, one bounded REST response window is one transaction. Exact duplicates are no-ops. Conflicting values are never silently overwritten and must create quarantine diagnostics before the approved correction policy is applied.

## 9. Correction semantics

If an observation has the same canonical identity but different OHLCV values:

1. classify it as a correction;
2. preserve diagnostic visibility of old and new values;
3. update the canonical candle according to the accepted policy;
4. emit `CandleCorrected`;
5. never silently treat it as an exact duplicate.

The precise immutable-history strategy may evolve later. Version 1 records conflicting observations in quarantine and applies the approved REST-authority policy without a correction-event requirement.

## 10. Validation

Validation is transport-independent.

Minimum checks:

- supported venue/category/symbol/timeframe;
- open time aligned to timeframe grid;
- close time consistent with timeframe;
- finite numeric OHLCV values;
- `high >= max(open, close, low)`;
- `low <= min(open, close, high)`;
- volume non-negative;
- candle confirmed closed;
- timestamp is not unreasonably in the future;
- source payload can be traced in structured diagnostics.

Invalid candles do not enter the canonical candle table.

## 10.1 Historical depth policy

The intended default for configured symbols is to retain the deepest minute history that Bybit can actually provide. The service is expected to become a future source for both live consumers and BBB research, so a shallow rolling retention window is not the default design.

The service SHALL distinguish:

```text
instrument_launch_time_ms
earliest_available_1m_open_time_ms
required_history_start_ms
```

For the default full-history policy:

```text
required_history_start_ms = earliest_available_1m_open_time_ms
```

The launch time remains exchange metadata and a search floor; it is not itself proof that a candle exists at that timestamp.

Historical candles are durable long-lived data. Normal operation SHALL append and repair rather than reload the entire history.

## 10.2 Historical bootstrap and future consumer separation

Full historical bootstrap writes canonical candles without assuming a future live-consumer notification design. Research will read historical ranges from candle storage/API. Realtime consumer delivery is deferred to a later approved integration change and does not require an event table in schema v1.

## 11. Gap model

A gap is a missing expected candle identity inside a configured closed interval.

Gap detection uses canonical timeframe-grid logic.

Repair rules:

- REST is the repair authority;
- repair writes through the same canonical ingestion use case;
- repair must be idempotent;
- post-repair audit verifies the expected interval;
- repaired candles update canonical storage; readiness remains false until post-repair audit succeeds.

## 11.1 Operational scenarios

Startup and recovery behavior is normative and is specified in `docs/operational-scenarios.md`. It covers:

- database absent, empty, populated, or schema-invalid;
- historical lower-bound discovery;
- distinction between instrument `launchTime` and observed earliest available candle;
- deterministic REST window splitting;
- internal and trailing gap audit/repair;
- WebSocket disconnect/reconnect;
- crash consistency and replay idempotency;
- adding or removing configured streams;
- per-stream readiness.

## 12. Startup lifecycle

Target startup sequence:

1. load and validate configuration;
2. initialize database and migrations;
3. load configured streams;
4. inspect persisted ingestion state independently for every enabled stream;
5. calculate the latest fully closed exchange interval;
6. perform REST catch-up;
7. perform gap audit and repair;
8. establish WebSocket subscriptions;
9. perform a short post-connect REST audit to close the startup race;
10. mark each stream ready only when its own canonical history is current;
11. expose strict aggregate readiness plus per-stream details.

Version 1 may implement a simpler sequence before introducing WebSocket buffering, but the race and recovery behavior must be explicit and tested.

## 13. Reconnect lifecycle

On WebSocket loss:

1. mark affected streams degraded;
2. continue serving persisted historical reads;
3. reconnect with bounded backoff;
4. determine the missing closed interval;
5. repair it through REST;
6. audit continuity;
7. return streams to ready;
8. expose recovered readiness and diagnostics.

WebSocket is the low-latency delivery path. REST and the local database provide recovery and durable truth.

## 14. External API

Planned versioned API:

```text
GET /health
GET /readiness
GET /v1/candles
GET /v1/candles/latest
```

### 14.1 Candle range API

Filters:

- venue;
- market_category;
- symbol;
- timeframe;
- from_open_time_ms;
- to_open_time_ms;
- limit.

Ordering is deterministic and ascending by open time unless explicitly documented otherwise.

### 14.2 Latest candle API

Returns the latest committed candle for one stream.

## 15. Health and readiness

`/health` answers whether the process is alive.

`/readiness` answers whether configured streams are safe for current consumption.

Readiness is per stream and includes:

- status;
- latest committed candle;
- expected latest closed candle;
- active gap count;
- WebSocket state;
- repair state;
- staleness information.

A running process with stale or incomplete data is healthy but not ready.

## 16. Configuration

Configuration is explicit and environment-driven.

Initial concepts:

- database path;
- API host and port;
- Bybit environment/base URLs;
- path to versioned market configuration;
- configured venue/category/instrument/timeframe streams;
- initial BTCUSDT and ETHUSDT linear perpetual definitions;
- REST timeout and retry limits;
- WebSocket reconnect policy;
- staleness threshold;
- logging level.

Public market-data ingestion must not require trading API credentials.

## 17. Observability

Structured logs must include stable identifiers:

- stream key;
- candle identity;
- source transport;
- repair window;
- retry attempt;
- classification result.

Planned metrics:

- last WebSocket message age;
- last committed candle age;
- committed candles total;
- duplicates total;
- corrections total;
- invalid observations total;
- active gaps;
- repairs total;
- repair failures;
- reconnects;

## 18. Docker boundary

Initial deployment:

```text
market-data-service container
└── /data/market.sqlite
```

The database volume has one writer owner: this service.

The service must support:

- graceful `SIGTERM`;
- deterministic migration startup;
- persistent named volume;
- restart without duplicate candles or invalid state advancement;
- readiness suitable for Docker health checks.

## 19. Contract ownership

The HTTP contract must be defined by versioned OpenAPI generated from or checked against the application schema.

When downstream services are introduced, contract tests must protect compatibility.

Shared DTOs must not be maintained by manual copy-and-paste between repositories as the long-term solution.

## 20. Source reuse policy

The old BBB Data Engine is a reference and source of proven pure algorithms, not a package dependency and not a template to copy wholesale.

Reusable candidates:

- timeframe mapping;
- timeframe-grid calculations;
- time-window contracts;
- gap detection;
- REST fetch-window splitting;
- Bybit interval mapping;
- kline response parsing;
- retry concepts;
- OHLC validation rules.

Components that must be redesigned:

- database ownership;
- migrations;
- atomic candle-plus-state commit;
- service lifecycle;
- WebSocket handling;
- readiness;
- correction classification;
- long-running process orchestration.

See `docs/source-reuse-audit.md`.

## 20A. Sequential bounded backfill

Version 1 deliberately avoids a parallel multi-stream REST scheduler. Deep history is loaded through finite administrative runs. One bounded REST window is processed and committed atomically at a time, and `--all` visits configured streams sequentially in deterministic order with a per-stream window budget. Completed data is durable and later runs resume from the latest committed candle in stream state. That resume point is not a continuity proof; audit remains responsible for detecting gaps. Unlimited deep bootstrap is not part of normal service startup.

## 21. Implementation phases

### Phase 0 — Architecture baseline

Deliver:

- standalone repository;
- master plan;
- source-reuse audit;
- OpenSpec proposal/design/spec/tasks;
- Python and Docker baseline;
- no production ingestion logic.

Acceptance:

- boundaries and non-goals are explicit;
- first vertical slice is unambiguous;
- the repository can be installed and tested;
- no BBB runtime dependency exists.

### Phase 1 — Domain and atomic storage foundation

Deliver:

- market-stream and candle domain models;
- timeframe/grid contracts;
- transport-neutral validation;
- SQLite migrations;
- candle repository;
- ingestion-state repository;
- atomic commit use case;
- duplicate and correction classification;
- unit and integration tests.

Acceptance:

- one valid new candle creates one canonical row and advances stream state atomically;
- replaying the same candle is an idempotent no-op;
- changed OHLCV creates a correction path;
- invalid candles are rejected;
- restart preserves idempotency.

### Phase 2 — REST ingestion and repair primitives

Deliver:

- Bybit REST adapter;
- native interval mapping;
- pagination/window splitting;
- retries and typed failures;
- historical backfill;
- gap detection;
- repair use case;
- post-repair audit;
- fake-adapter integration tests.

Acceptance:

- an empty stream can be populated for a bounded closed interval;
- repeated backfill is idempotent;
- deliberate gaps are repaired;
- repair uses the same canonical ingestion path.

### Phase 3 — Service runtime

Deliver:

- settings;
- process entrypoint;
- configured streams;
- startup catch-up;
- health and readiness;
- graceful shutdown;
- structured logging;
- Docker image and compose file.

Acceptance:

- service starts against an empty persistent volume;
- catches up configured streams;
- exposes accurate per-stream readiness;
- restarts without duplicating candles or skipping continuity checks.

### Phase 4 — WebSocket realtime

Deliver:

- Bybit public WebSocket adapter;
- subscription lifecycle;
- confirmed-candle parsing;
- reconnect/backoff;
- stale-stream detection;
- reconnect REST repair;
- duplicate suppression.

Acceptance:

- confirmed close reaches canonical storage;
- partial candle updates do not become canonical candles;
- disconnect and reconnect repair all missed closed candles;
- readiness degrades and recovers correctly.

### Phase 5 — Consumer API

Deliver:

- candle range endpoint;
- latest candle endpoint;
- readiness endpoint;
- deterministic pagination;
- OpenAPI contract;
- API contract tests.

Acceptance:

- an independent consumer can bootstrap history;
- keep its own last-processed cursor;
- pause decisions while a stream is not ready;
- catch up by ordered range read after readiness recovery;
- never require direct SQLite access.

### Phase 6 — Hardening

Deliver:

- long-running demo smoke;
- boundary-time tests;
- network fault tests;
- malformed payload tests;
- database fault behavior;
- metrics;
- maintenance and retention decisions;
- operational runbook.

## 22. First vertical slice

The first production-code change after Phase 0 is deliberately narrow:

```text
Given:
  an empty database
  one configured stream
  a fake or REST-sourced valid confirmed candle

When:
  the canonical ingestion use case processes it

Then:
  the candle is persisted
  the canonical candle is persisted
  stream state is advanced
  both changes are committed atomically
  replay is idempotent
```

WebSocket implementation must wait until this invariant exists.

## 23. Architectural dependency rules

Target import direction:

```text
domain
  ↑
application
  ↑
adapters
  ↑
entrypoints
```

Rules:

- domain imports no adapters;
- application depends on ports, not Bybit or SQLite concrete classes;
- REST and WebSocket adapters share the same ingestion application use case;
- API handlers do not contain storage or ingestion rules;
- database code does not know downstream consumers;
- no package imports from BBB or Abi Executor.

## 24. Open questions deferred beyond Phase 0

These do not block repository initialization:

1. Exact SQLite driver and async strategy.
2. FastAPI versus another thin HTTP framework.
3. Exact numeric representation is resolved: Python `Decimal` in the domain and normalized decimal strings in SQLite/API; binary floats are rejected at ingestion.
4. Whether correction history later receives a dedicated revision table.
6. Whether PostgreSQL replaces SQLite after the local production version.
7. Which concrete symbols form the first demo deployment. `1m` is mandatory.
8. Whether higher timeframes are stored as native Bybit candles, derived from canonical `1m`, or both with explicit provenance.

Each deferred decision must be resolved before its implementation phase, not guessed inside code.

## Approved SQLite schema v1

Step 3 approves the intentionally small schema documented in `docs/database-schema-v1.md` and defined by `src/market_data_service/adapters/sqlite/schema_v1.sql`:

```text
schema_meta
instruments
streams
candles
stream_state
quarantine
```

Schema v1 deliberately excludes event logs, consumer cursors, bootstrap-run history, persisted gap history, correction revisions, and feature storage. These can be added later without changing candle identity.

## Step 4 accepted precision policy

Canonical OHLCV values are exact `Decimal` values. SQLite and JSON use one normalized non-exponential decimal text per numeric value. No rounding or quantization occurs during persistence. See `docs/decimal-policy.md`.


## Acceptance test contract

The complete pre-implementation acceptance matrix is normative and lives in:

```text
docs/acceptance-test-matrix.md
```

Implementation phases must cite and satisfy the relevant scenario IDs. The first real integration milestone is SQLite-only; the first Bybit smoke follows only after the canonical storage path passes.


## Architecture Decision Records

Accepted durable decisions are recorded in `docs/adr/`. ADRs are short and normative; they do not duplicate implementation specifications.
