# Market Data Service v1 Specification

## Requirement: Independent service boundary

The system SHALL run as a standalone service with its own package, database, Docker image, and lifecycle.

The system SHALL NOT require runtime imports from BBB or Abi Executor.

## Requirement: Market-data-only responsibility

The system SHALL obtain, validate, repair, persist, and expose market candle data.

The system SHALL NOT compute strategy features, evaluate strategies, generate trading signals, execute orders, or manage positions.

## Requirement: Canonical candle identity

Each canonical candle SHALL be uniquely identified by canonical ticker, validated timeframe, and open time in milliseconds. SQLite SHALL enforce the equivalent physical key `(stream_id, open_time_ms)`.

## Requirement: Closed candle publication

The system SHALL persist only confirmed closed candles in v1. Unconfirmed current-candle updates SHALL NOT enter canonical candle storage.

## Requirement: Unified ingestion

REST and WebSocket observations SHALL be normalized into a common transport-neutral candle observation and SHALL pass through the same validation and commit use case.

Neither transport adapter SHALL directly mutate canonical storage.

## Requirement: Atomic commit

For a newly accepted or authoritatively corrected candle, the system SHALL commit the candle mutation and corresponding `stream_state` advancement atomically.

## Requirement: Idempotency

Reprocessing an exact duplicate candle SHALL NOT create another canonical row or mutate the existing row.

Idempotency SHALL survive process restart.

## Requirement: Correction visibility

An observation with an existing canonical identity and different accepted values SHALL be classified as a correction rather than a duplicate.

The system SHALL preserve durable diagnostic visibility of the correction.

## Requirement: Validation

The system SHALL reject invalid candles before canonical persistence.

Validation SHALL include timeframe alignment, finite numeric values, valid OHLC relationships, non-negative volume, configured stream membership, and closed-candle status.

## Requirement: REST recovery authority

The system SHALL use Bybit REST candle data to perform bootstrap, catch-up, gap repair, and reconnect repair.

Repair data SHALL use the same canonical ingestion use case as realtime data.

## Requirement: Gap auditing

The system SHALL detect missing expected candles on the canonical timeframe grid for an audited closed interval.

After repair, the system SHALL perform a continuity audit.

## Requirement: Storage ownership

The v1 SQLite database SHALL have a single process owner: Market Data Service.

Downstream consumers SHALL NOT require direct database-file access.


## Requirement: Persisted per-stream lifecycle

Each enabled stream SHALL persist exactly one current lifecycle state from: `uninitialized`, `bootstrapping`, `auditing`, `repairing`, `connecting`, `ready`, `degraded`, or `failed`.

Lifecycle state SHALL be scoped by stream and SHALL NOT be shared globally across symbols.

The domain SHALL reject illegal transitions. In particular, `uninitialized`, `bootstrapping`, `repairing`, and `failed` SHALL NOT transition directly to `ready`; repair SHALL return to audit, and connecting plus trailing catch-up SHALL precede ready.

A persisted `ready` state SHALL NOT be trusted blindly after restart. The system SHALL reconcile actual stored candles, continuity, and freshness before restoring readiness.

## Requirement: Strict readiness projection

Per-stream readiness SHALL be true only when lifecycle state is `ready`.

Default aggregate readiness SHALL be true only when at least one required stream exists and every enabled required stream is ready.

## Requirement: Health and readiness

The system SHALL expose liveness separately from readiness.

Readiness SHALL reflect current per-stream data continuity, repair state, staleness, and realtime connection state.

## Requirement: Reconnect recovery

When realtime connectivity is lost, affected streams SHALL become degraded or unready.

After reconnect, the system SHALL repair missed closed intervals through REST before restoring readiness.

## Requirement: Versioned API

Consumer-facing candle and readiness endpoints SHALL be versioned.

The service SHALL expose a machine-readable API contract before downstream integration is considered stable.

## Requirement: Historical lower bound

The system SHALL distinguish instrument launch metadata from the observed earliest available candle for each timeframe stream.

The system SHALL NOT treat instrument `launchTime` alone as proof that a candle exists at that timestamp.

The resolved observed earliest available candle SHALL be durably cached and used as the normal lower bound for continuity obligations.

## Requirement: Cold-start audit

On startup, the system SHALL validate schema and durable stream state, audit internal and trailing continuity for every configured stream, repair fetchable gaps, and expose readiness only after required repair succeeds.

The system SHALL NOT infer continuity solely from the latest stored candle timestamp.


## Requirement: Mandatory minute stream

For every configured symbol, the system SHALL maintain a canonical `1m` candle stream.

## Requirement: Full available minute history

By default, the required historical interval SHALL begin at the earliest valid minute candle that Bybit actually exposes for the symbol.

Instrument `launchTime` SHALL be used as discovery metadata and a search floor, not as proof of the canonical first candle.

## Requirement: Resumable bootstrap

Full-history bootstrap SHALL persist durable progress and SHALL resume after process restart without discarding already committed history.

## Requirement: Readiness-first consumer recovery

Bootstrap, startup catch-up, reconnect catch-up, and repair SHALL persist canonical candles without requiring per-candle consumer events.

A consumer SHALL own its own per-stream `last_processed_open_time_ms`, SHALL pause trading, feature, and strategy decisions while the stream is not `ready`, and SHALL catch up through ordered candle range reads after readiness is restored.

Schema v1 SHALL NOT require a market-event log, replay broker, correction events, or server-owned consumer offsets.

## Requirement: Multi-instrument configuration

The system SHALL load enabled market instruments from a versioned validated configuration.

The initial configuration example SHALL include Bybit linear BTCUSDT and ETHUSDT perpetual contracts with canonical `1m` streams and full-available-history policy.

Adding another supported instrument SHALL NOT require duplicated ingestion orchestration code.

## Requirement: Per-stream state isolation

Every candle, gap, bootstrap progress value, ingestion state, realtime subscription, and readiness record SHALL be scoped by canonical ticker and timeframe.

A failure, correction, duplicate, or bootstrap update for one stream SHALL NOT mutate another stream's state.

The system SHALL NOT rely on a global current symbol or global last-candle cursor.

## Requirement: Aggregate and per-stream readiness

The system SHALL expose readiness for every enabled stream.

The default aggregate readiness SHALL be true only when every enabled required stream is ready.

## Requirement: Preserved half-open interval semantics

Historical audit, fetch, repair, and repository range contracts SHALL use
half-open intervals `[start_ms, end_ms)`.

Exchange-specific inclusive-end translation SHALL remain inside the relevant
adapter.

## Requirement: Single timeframe registry

The system SHALL maintain one canonical timeframe registry containing duration,
Bybit interval mapping, and resampling frequency.

The registry SHALL include `1m` with a duration of 60,000 milliseconds and
Bybit interval `1`.

## Requirement: Deterministic grid mathematics

The domain SHALL expose deterministic floor, ceiling, and latest-closed-candle
grid operations independent of transport and persistence.

At an exact current timeframe boundary, latest-closed-candle resolution SHALL
return the previous candle open.

## Requirement: Pure gap semantics

Gap detection SHALL tolerate unsorted duplicate stored timestamps, merge
adjacent missing candles into half-open gaps, and reject off-grid timestamps.

## Requirement: Bounded repair windows

A missing gap SHALL be divisible into aligned half-open request windows that do
not exceed the configured source request limit.

## Requirement: Observation versus canonical candle

The system SHALL model an externally observed candle separately from a
validated canonical candle.

Transport observations SHALL include source and confirmation status. Canonical
candles SHALL only be created by the application ingestion path.

## Requirement: Stable instrument identity

The canonical `InstrumentKey` SHALL consist only of the canonical ticker, such as `BTCUSDT.P`. The Bybit API symbol SHALL be stored as an explicit mapping and SHALL NOT be part of identity.

Mutable or descriptive fields including assets, contract kind, launch time, status, precision, enabled state, and history policy SHALL NOT be part of instrument identity.

## Requirement: Separate exchange metadata

Exchange-observed launch metadata and exact Bybit API symbol SHALL be represented separately from stable ticker identity. Metadata refresh SHALL NOT create a new instrument identity.

Instrument `launchTime` SHALL remain discovery metadata rather than the canonical first-candle timestamp.

## Requirement: Separate operator coverage

Operator configuration SHALL declare canonical ticker, exact Bybit API symbol, enabled state, canonical timeframes, and history policy. A mismatch SHALL leave the affected instrument unready and SHALL NOT silently substitute another instrument or rewrite configuration.

## Requirement: Validated stream identity

The canonical `StreamKey` SHALL consist of `InstrumentKey` plus a timeframe resolved through the single supported-timeframe registry. Arbitrary timeframe strings SHALL be rejected.

Every enabled instrument coverage SHALL include the canonical `1m` stream and SHALL reject duplicate timeframe declarations after normalization.

## Requirement: Scope-correct state

Instrument metadata and metadata validation state SHALL be instrument-scoped. Candle history, historical bounds, bootstrap progress, gaps, subscriptions, and readiness SHALL be stream-scoped.

The system SHALL NOT use process-wide current-instrument, current-stream, or last-candle singleton state.

## Requirement: Canonical ticker identity

The service SHALL identify v1 instruments by canonical perpetual tickers such as `BTCUSDT.P` and `ETHUSDT.P`.

The exact Bybit API symbols `BTCUSDT` and `ETHUSDT` SHALL be stored as explicit mappings and SHALL NOT be inferred at every call site.

## Requirement: Minimal SQLite schema v1

The first database schema SHALL contain only `schema_meta`, `instruments`, `streams`, `candles`, `stream_state`, and `quarantine`.

The system SHALL NOT add event-log, bootstrap-window, correction-history, metadata-history, or feature tables without a later approved change.

## Requirement: Candle storage identity

A canonical candle SHALL be unique by `(stream_id, open_time_ms)`.

Exact duplicate observations SHALL be no-ops. Conflicting values SHALL NOT be silently overwritten and SHALL create durable quarantine diagnostics.

## Requirement: Atomic state advancement

A candle write and the corresponding `stream_state` advancement SHALL occur in the same SQLite transaction.

A bounded REST response window SHALL be the normal historical transaction boundary.

## Requirement: Exact decimal policy

Canonical OHLCV values SHALL use Python `Decimal` semantics in the domain. SQLite persistence and JSON APIs SHALL use normalized non-exponential decimal strings.

The canonical ingestion boundary SHALL reject binary float input, `NaN`, positive or negative infinity, empty values, and non-numeric values.

Numerically equivalent spellings such as `1`, `1.0`, `1.000`, and `1E+0` SHALL normalize to the same canonical text. Negative zero SHALL normalize to `0`.

## Requirement: Exact candle validation

Candle validation SHALL use exact decimal comparisons. Volume SHALL be non-negative. High SHALL be greater than or equal to open, close, and low. Low SHALL be less than or equal to open, close, and high.

## Requirement: Exact duplicate classification

Duplicate/correction classification SHALL compare canonical OHLCV text, not raw transport strings and not binary floats. Equivalent REST and WebSocket decimal spellings SHALL classify as duplicates.


## Requirement: Sequential bounded REST work

Version 1 SHALL execute historical REST window work sequentially by default and SHALL NOT require a parallel multi-stream scheduler.

Administrative backfill SHALL support selecting one configured stream or all enabled streams in deterministic configuration order. Each invocation SHALL have an explicit positive window budget and SHALL terminate when that budget is exhausted.

Completed windows SHALL remain durable across stop, failure, and restart. A later invocation SHALL resume from the latest committed candle in persisted stream state rather than restarting full history. That resume point SHALL NOT be treated as proof of continuity; audit SHALL remain responsible for proving absence of gaps.

Recoverable REST/source failures during bootstrap SHALL move the affected stream to `degraded`. Fatal configuration, schema, storage-corruption, or impossible-invariant failures SHALL move the affected stream to `failed`.

Normal long-running service startup SHALL NOT silently initiate an unlimited deep-history bootstrap.

A recoverable failure for one stream SHALL NOT erase progress for another stream.


## Requirement: Acceptance scenarios

The system SHALL maintain a version-controlled acceptance matrix with stable scenario identifiers covering configuration, validation, schema lifecycle, canonical persistence, bootstrap, gaps, restart, multi-symbol isolation, consumer readiness, Bybit REST, WebSocket, API, and runtime behavior.

Production implementation SHALL be accepted only against the relevant scenario identifiers.
