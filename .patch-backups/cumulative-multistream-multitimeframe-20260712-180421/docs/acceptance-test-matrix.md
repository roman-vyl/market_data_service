# Acceptance Test Matrix

This matrix is the implementation contract for Market Data Service v1.

It is intentionally defined before SQLite, Bybit REST, WebSocket, and HTTP adapters are implemented. Production work is accepted only when the relevant scenarios pass at the appropriate layer.

## Test levels

- **Domain** — pure contracts and deterministic calculations; no filesystem or network.
- **SQLite integration** — temporary real SQLite database and real transactions.
- **Application integration** — real application use cases with fake source/clock adapters.
- **Bybit smoke** — bounded calls to public Bybit endpoints; never required for unit CI.
- **Runtime smoke** — local process/container lifecycle with real persistence.

## A. Configuration and identity

| ID | Scenario | Level | Required result |
|---|---|---|---|
| CFG-01 | Load BTCUSDT.P and ETHUSDT.P from `markets.toml` | Application integration | Two unique enabled instruments and two mandatory 1m streams are produced. |
| CFG-02 | Duplicate ticker | Application integration | Configuration is rejected before database mutation. |
| CFG-03 | Duplicate exchange symbol | Application integration | Configuration is rejected before database mutation. |
| CFG-04 | Missing mandatory 1m stream | Application integration | Configuration is rejected. |
| CFG-05 | Unknown timeframe | Domain | Configuration is rejected by the timeframe registry. |
| CFG-06 | Ticker mapping mismatch | Bybit smoke/application integration | Stream remains unready and the mismatch is quarantined. |

## B. Decimal and candle validation

| ID | Scenario | Level | Required result |
|---|---|---|---|
| VAL-01 | Equivalent decimal strings (`1`, `1.0`, `1.000`) | Domain | Normalize to the same canonical text. |
| VAL-02 | Negative zero | Domain | Normalize to `0`. |
| VAL-03 | Exponent notation | Domain | Normalize without exponent notation. |
| VAL-04 | NaN or infinity | Domain | Reject. |
| VAL-05 | Binary float input | Domain | Reject at the canonical boundary. |
| VAL-06 | Invalid OHLC relationship | Domain | Reject with a typed validation failure. |
| VAL-07 | Negative volume | Domain | Reject. |
| VAL-08 | Off-grid open time | Domain | Reject. |
| VAL-09 | Unconfirmed WebSocket candle | Domain/application integration | Reject from canonical persistence. |

## C. Schema and database lifecycle

| ID | Scenario | Level | Required result |
|---|---|---|---|
| DB-01 | Database file absent | SQLite integration | Schema v1 is created exactly once. |
| DB-02 | Existing empty schema v1 | SQLite integration | Opens successfully without destructive recreation. |
| DB-03 | Existing populated schema v1 | SQLite integration | Data remains intact after reopen. |
| DB-04 | Unknown schema version | SQLite integration | Fail closed; database is preserved. |
| DB-05 | Missing required table | SQLite integration | Fail schema validation; do not recreate silently. |
| DB-06 | Foreign key violation | SQLite integration | Transaction fails. |
| DB-07 | WAL and required connection pragmas | SQLite integration | Required pragmas are active. |

## D. Canonical candle persistence

| ID | Scenario | Level | Required result |
|---|---|---|---|
| ING-01 | New valid candle | SQLite/application integration | Insert one candle and advance matching stream state atomically. |
| ING-02 | Exact duplicate from same source | SQLite/application integration | No second row and no state regression. |
| ING-03 | REST/WS formatting difference only | Application integration | Classify as duplicate after Decimal normalization. |
| ING-04 | Different OHLCV with same key | SQLite/application integration | Detect correction; do not silently overwrite; write quarantine record. |
| ING-05 | REST correction of WS candle | SQLite/application integration | Apply approved REST-authority policy atomically and quarantine the difference. |
| ING-06 | WS conflicts with existing REST candle | SQLite/application integration | Preserve REST canonical row and quarantine the conflict. |
| ING-07 | Invalid candle in a batch | SQLite/application integration | Defined batch policy is applied without partial hidden state advancement. |
| ING-08 | Failure before commit | SQLite integration | Candle and stream state both roll back. |
| ING-09 | Failure after candle insert but before state update | SQLite integration | Entire transaction rolls back. |
| ING-10 | Reopen after committed insert | SQLite integration | Candle and stream state persist. |

## E. Historical lower bound and full bootstrap

| ID | Scenario | Level | Required result |
|---|---|---|---|
| BST-01 | `launchTime` is grid-aligned | Application integration | Earliest-candle search starts at the correct 1m boundary. |
| BST-02 | `launchTime` is not grid-aligned | Domain/application integration | Search starts at the next valid 1m boundary. |
| BST-03 | First API windows are empty | Application integration | Search advances without treating pre-history as gaps. |
| BST-04 | First real candle is later than `launchTime` | Application integration | Persist the observed earliest candle as the continuity floor. |
| BST-05 | Empty database, bounded one-stream backfill | Application integration | Process no more than the requested window limit. |
| BST-06 | Restart halfway through bootstrap | SQLite/application integration | Resume from persisted candles; do not restart from launch time. |
| BST-07 | Re-run same completed windows | Application integration | All rows classify as duplicates; no duplicated storage. |
| BST-08 | `--all` with several streams | Application integration | Process streams sequentially in deterministic configuration order. |
| BST-09 | One stream fails during `--all` | Application integration | Preserve all committed progress and report the failing stream explicitly. |
| BST-10 | Ten configured tickers | Application integration | Still execute sequentially with bounded work and no unbounded in-memory queue. |
| BST-11 | Observed lower bound is already cached | Application integration | Do not repeat metadata or kline discovery calls. |
| BST-12 | Ordinary bounded backfill starts later than history | Application integration | Do not persist the requested window start as the historical lower bound. |
| BST-13 | Full bootstrap lower-bound discovery exhausts budget | Application integration | Return incomplete without backfill, keep the stream bootstrapping, and do not persist a false lower bound. |
| BST-14 | Full bootstrap discovery and backfill share budget | Application integration | Discovery windows plus backfill attempted windows never exceed the requested `max_windows`. |

## F. Gap audit and repair

| ID | Scenario | Level | Required result |
|---|---|---|---|
| GAP-01 | Complete interval | Domain/application integration | No gaps. |
| GAP-02 | One internal missing minute | Domain/application integration | Detect exactly one half-open gap. |
| GAP-03 | Consecutive missing minutes | Domain | Merge into one half-open gap. |
| GAP-04 | Duplicate and unsorted timestamps | Domain | Ignore duplicates and detect the same canonical gaps. |
| GAP-05 | Trailing missing interval | Application integration | Fetch and persist the trailing range. |
| GAP-06 | Repair succeeds | Application integration | Re-audit returns no gaps before readiness. |
| GAP-07 | Repair leaves a gap | Application integration | Stream remains non-ready and the problem is quarantined. |
| GAP-08 | REST returns rows outside requested window | Application integration | Ignore/reject unexpected rows and record diagnostics. |
| GAP-09 | Audit explicit stored range | SQLite/application integration | Read canonical candles for one stream/range and return a continuity report without state changes. |
| GAP-10 | Empty audited range | SQLite/application integration | Return non-continuous with a gap covering the checked range. |
| GAP-11 | Multi-stream continuity audit | SQLite/application integration | BTC and ETH reports are independent and one stream's gaps do not affect another report. |
| GAP-12 | Gap at audited range beginning or end | SQLite/application integration | Report leading and trailing missing intervals inside the requested range. |
| GAP-13 | Unknown stream audit | Application integration | Raise a typed application error without scanning storage or changing state. |

## G. Persisted state machine and restart

| ID | Scenario | Level | Required result |
|---|---|---|---|
| STM-01 | New stream | Domain/SQLite integration | Starts as `uninitialized`. |
| STM-02 | Normal cold-start path | Domain/application integration | `uninitialized → bootstrapping → auditing → connecting → ready`. |
| STM-03 | Gaps found | Domain/application integration | `auditing → repairing → auditing`; never directly to ready. |
| STM-04 | Illegal direct transition to ready | Domain | Reject. |
| STM-05 | Persisted ready after process restart | Runtime/application integration | Re-enter audit/catch-up; do not trust old ready blindly. |
| STM-06 | Restart during bootstrapping | Application integration | Resume from actual stored maximum. |
| STM-07 | Restart during repairing | Application integration | Recompute gaps, then repair; do not trust stale in-memory progress. |
| STM-08 | WebSocket disconnect | Application integration | `ready → degraded`. |
| STM-09 | Successful recovery | Application integration | Catch up, audit, connect, then return to ready. |
| STM-10 | Fatal invariant failure | Domain/application integration | Enter `failed`; no direct transition from failed to ready. |

## H. Multi-symbol isolation and readiness

| ID | Scenario | Level | Required result |
|---|---|---|---|
| MUL-01 | BTC ready, ETH bootstrapping | Domain/application integration | Per-stream statuses differ; strict service readiness is false. |
| MUL-02 | BTC repair fails | Application integration | ETH persisted state and history are unchanged. |
| MUL-03 | ETH backfill succeeds after BTC failure | Application integration | ETH progress commits independently. |
| MUL-04 | All required streams ready | Domain/application integration | Strict service readiness is true. |
| MUL-05 | No configured streams | Domain | Service readiness is false. |

## I. Consumer readiness contract

| ID | Scenario | Level | Required result |
|---|---|---|---|
| CON-01 | Consumer connects while stream is non-ready | API/application integration | Status can be read, but processing is gated. |
| CON-02 | Consumer cursor is behind after service restart | API/application integration | Range query returns every candle after the cursor in ascending order. |
| CON-03 | Stream degrades during consumer operation | API/application integration | Consumer stops decisions and preserves its own cursor. |
| CON-04 | Stream returns to ready after repair | API/application integration | Consumer catches up from its cursor; no replay event is required. |
| CON-05 | Lost future live notification | API/application integration | Consumer recovers through readiness plus range read. |

## J. Bybit REST adapter

| ID | Scenario | Level | Required result |
|---|---|---|---|
| RST-01 | Resolve BTCUSDT instrument metadata | Bybit smoke | Active symbol and launch time are parsed. |
| RST-02 | Fetch one closed 1m window | Bybit smoke | Rows normalize to ascending observed candles. |
| RST-03 | Bybit returns reverse order | Application integration | Adapter returns ascending canonical order. |
| RST-04 | Half-open request window | Application integration | Request conversion uses the approved inclusive exchange boundary without leaking rows. |
| RST-05 | 429 or retryable 5xx | Application integration | Bounded retry/backoff; no retry storm. |
| RST-06 | Non-retryable Bybit error | Application integration | Typed failure, quarantine where appropriate, no partial readiness. |
| RST-07 | Re-fetch same window | Bybit/SQLite smoke | Second ingestion produces duplicates only. |
| RST-08 | Real bounded backfill smoke | Bybit/SQLite smoke | Temporary SQLite is created, `BackfillStreamHistory` imports a small BTCUSDT 1m range, duplicate replay adds no rows, persistence reopens cleanly, and smoke-only 1m continuity passes. |
| RST-09 | Real backfill plus continuity audit smoke | Bybit/SQLite smoke | Temporary SQLite is created, bounded BTCUSDT 1m backfill succeeds, `AuditStreamContinuity` over the same range is continuous, and no gaps are reported. |
| RST-10 | Real full-history bootstrap restart/resume smoke | Bybit/SQLite smoke | Temporary SQLite resolves observed BTCUSDT.P 1m lower bound with a shared small window budget, reopens through a fresh workflow, runs again, and confirms total candle windows stay within budget, cached discovery uses zero windows, and backfill resumes. |

## K. WebSocket realtime

| ID | Scenario | Level | Required result |
|---|---|---|---|
| WSS-01 | Subscribe to BTC and ETH | Runtime smoke | One lifecycle manages both configured subscriptions without shared stream state. |
| WSS-02 | Partial candle update | Application integration | No canonical insert. |
| WSS-03 | Confirmed candle update | Application/SQLite integration | Pass through the same ingestion use case as REST. |
| WSS-04 | Duplicate confirmed candle after REST catch-up | Application integration | Duplicate no-op. |
| WSS-05 | Missing expected minute | Application integration | Stream becomes degraded and REST repair is requested. |
| WSS-06 | Disconnect and reconnect | Runtime smoke | Catch-up and audit complete before ready is restored. |

## L. API and runtime

| ID | Scenario | Level | Required result |
|---|---|---|---|
| API-01 | Candle range query | API integration | Half-open range, ascending order, decimal strings. |
| API-02 | Latest candle query | API integration | Return latest committed candle for one stream. |
| API-03 | Per-stream readiness query | API integration | Return state and timestamps for each configured stream. |
| API-04 | Overall readiness query | API integration | Strict aggregation across all required streams. |
| RUN-01 | SIGTERM during idle | Runtime smoke | Graceful shutdown with no corruption. |
| RUN-02 | SIGTERM during REST window transaction | Runtime smoke | Current transaction commits completely or rolls back completely. |
| RUN-03 | Container restart with persistent volume | Runtime smoke | Schema and progress survive. |

## First executable integration milestone

The first production milestone is accepted when these scenarios pass against a temporary real SQLite database:

```text
DB-01, DB-03, DB-04, DB-06, DB-07
ING-01, ING-02, ING-03, ING-04, ING-08, ING-09, ING-10
STM-01
MUL-02
```

No network adapter is required for this milestone.

## First Bybit smoke milestone

The first external smoke is accepted when:

```text
RST-01, RST-02, RST-04, RST-07
```

pass for a small closed BTCUSDT 1m interval without modifying any production database.

## First real backfill smoke milestone

The first end-to-end bounded backfill smoke is accepted when:

```text
RST-08, BST-05, ING-01, ING-02, ING-10, STM-06
```

pass through real Bybit REST, canonical ingestion, temporary SQLite persistence,
duplicate replay, and the smoke-only 1m continuity assertion.

## First real continuity audit smoke milestone

The first end-to-end continuity audit smoke is accepted when:

```text
RST-09, GAP-09, GAP-10, GAP-11, GAP-12, GAP-13
```

pass through real Bybit REST backfill, temporary SQLite persistence, and
`AuditStreamContinuity` over the same explicit bounded range.

## First real full-history bootstrap smoke milestone

The first real full-history restart/resume smoke is accepted when:

```text
RST-10, BST-01, BST-02, BST-04, BST-05, BST-06, BST-11, BST-13, BST-14, ING-01, ING-10, STM-06
```

pass through real Bybit REST lower-bound discovery, temporary SQLite
persistence, two bounded bootstrap invocations, and durable resume.
