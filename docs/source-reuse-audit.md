# Source Reuse Audit — Existing BBB Data Engine

## Purpose

Preserve proven behavior from `_bbb_new_gen/data_engine` without importing the old package at runtime or copying its historical CLI architecture wholesale.

The existing BBB Data Engine remains unchanged.

## Reuse policy

The old implementation is:

- a source of proven pure algorithms;
- a behavioral reference;
- a source of parity fixtures and edge cases.

It is not:

- a runtime dependency;
- the new storage architecture;
- the new service lifecycle;
- a package to copy wholesale.

## File-by-file decisions

### `contracts/timeframes.py` — port semantics with an expanded API

Keep:

- one authoritative `TimeframeSpec`;
- internal id, duration, Bybit interval, and pandas frequency in one definition;
- strict unsupported-timeframe rejection.

Change:

- add mandatory `1m` (`60_000`, Bybit interval `1`, pandas `1min`);
- place the definition in the new domain layer;
- avoid research-specific assumptions.

Evidence required:

- parity tests for old supported timeframes;
- new boundary tests for `1m`.

### `engine/time_grid.py` — port semantics

Keep:

- deterministic grid alignment;
- ceiling to grid;
- latest fully closed candle calculation;
- half-open interval reasoning.

Change:

- inject/use a clock through application ports where current time is needed;
- document timestamp units explicitly.

### `contracts/time_window.py` — port with a narrower immutable contract

Keep:

- explicit bounded windows;
- half-open `[start, end)` semantics.

Change:

- remove CLI/report concerns;
- validate alignment at use-case boundaries rather than hide it in adapters.

### `contracts/fetch_request.py` — port with changed ownership

Keep:

- explicit symbol/timeframe/window fetch intent.

Change:

- include venue and market category in stream identity;
- make the application use case own request construction;
- keep Bybit-specific interval conversion inside the adapter.

### `contracts/gap.py` — port semantics

Keep:

- explicit missing half-open intervals.

Change:

- distinguish pre-history from repairable internal/trailing gaps;
- bind continuity to a resolved required-history lower bound.

### `engine/gaps.py` — port algorithm with parity and new edge cases

Keep:

- pure expected-grid gap detection;
- deterministic ordering.

Add tests for:

- empty series;
- leading interval after required lower bound;
- internal gap;
- trailing catch-up gap;
- duplicate timestamps;
- out-of-order input;
- one-minute streams;
- no gap before observed earliest available history.

### `engine/dim.py::iter_fetch_windows()` — port nearly unchanged in semantics

Keep:

- split a half-open gap into aligned bounded windows;
- obey maximum exchange candle count;
- deterministic coverage without overlap or omission.

Do not port:

- the full `fix_candles()` orchestration as one function.

Replace with focused application use cases:

- discover historical lower bound;
- bootstrap stream history;
- audit stream continuity;
- repair stream gaps;
- ingest observed candle.

### `engine/dim.py::_count_invalid_ohlc` — preserve rules, redesign API

Keep:

- OHLC relationship checks.

Change:

- validate each observed candle before persistence;
- return typed failures instead of only a count;
- apply identical validation to REST and WebSocket observations.

### `fetcher/bybit_rest.py` — port behavior behind a new adapter port

Keep:

- Bybit interval mapping;
- `retCode` validation;
- kline parsing;
- ascending canonical ordering;
- bounded retries for transient failures;
- rejection of malformed rows.

Change:

- use a transport-neutral observed-candle output;
- keep concrete client-library types out of application/domain;
- expose typed adapter errors;
- add `1m` explicitly.

### `fetcher/depth_resolver.py` — preserve launch-time caching, strengthen semantics

Keep:

- fetch `launchTime` from Bybit instruments info;
- cache exchange metadata durably;
- avoid repeated metadata calls.

Change:

- do not treat `launchTime` as proof of an actual candle;
- use it only as the search floor;
- probe kline windows to resolve `observed_earliest_candle_open_time_ms`;
- cache the observed earliest candle per stream/timeframe;
- for v1, set required `1m` history start to the observed earliest minute candle.

### `store/ddl.py` — reference only

Keep as evidence:

- useful indexes;
- prior schema fields;
- WAL-oriented deployment assumptions.

Reject as the final schema because it lacks:

- venue and market category identity;
- durable event log;
- ingestion state;
- migration history;
- correction diagnostics;
- atomic candle/quarantine/state semantics.

### `store/db.py` — reference only, do not copy the class

Keep after explicit review:

- SQLite WAL baseline;
- `busy_timeout=30000` baseline;
- `synchronous=NORMAL` baseline;
- range-query/index lessons;
- metadata caching concept.

Redesign:

- connection and transaction ownership;
- migrations;
- batch insert behavior;
- atomic unit of work;
- duplicate/correction result classification;
- resumable bootstrap progress;
- durable diagnostics.

### `service/cli.py` — reject as application center

Administrative commands may later call application use cases, but CLI wiring must not own ingestion or repair rules.

### Existing postflight audit and quarantine behavior — preserve concept

Keep:

- postflight gap audit after fetch/repair;
- durable visibility of fetch errors, unexpected rows, and unresolved gaps.

Change:

- model diagnostics explicitly rather than relying only on CLI reports or process logs.

## Minute-history implications

The new service SHALL support `1m` as the mandatory canonical stream for every configured symbol.

Default history policy:

```text
required_history_start_1m = observed_earliest_available_1m_candle
```

Cold bootstrap is expected to be heavy once and resumable thereafter.

## Higher-timeframe decision remains evidence-driven

Do not yet assume whether 5m/15m/1h/4h/1d are:

- Bybit-native stored streams;
- materialized derivations from canonical `1m`;
- or both with explicit provenance.

Before implementation, compare:

1. resampled stored `1m`;
2. Bybit-native higher-timeframe candles;
3. existing BBB research datasets.

## Required parity process

For each ported behavior:

1. capture old fixed fixtures where practical;
2. execute old behavior;
3. execute new behavior against the same fixtures;
4. document intentional differences;
5. add realtime-specific edge cases.
