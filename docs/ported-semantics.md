# Ported Semantics from the Old BBB Data Engine

This document is normative for semantics intentionally preserved from the old
BBB Data Engine after the Step 1 audit. It prevents accidental reinvention or
semantic drift while allowing the new service architecture to remain clean.

## Preserved domain semantics

### Half-open windows

All historical audit, fetch, repair, and repository range contracts use:

```text
[start_ms, end_ms)
```

The end timestamp is exclusive. Bybit adapters may translate this to an
inclusive exchange request boundary using `end_ms - 1`, but that translation
must stay inside the adapter.

### One timeframe registry

`domain/timeframes.py` is the single code-level registry for:

- canonical timeframe id;
- duration in milliseconds;
- Bybit interval value;
- pandas resampling frequency.

The new service adds mandatory `1m` support while preserving the old registry
principle and grid behavior.

### Grid mathematics

The following old semantics are preserved:

- floor timestamps with `align_to_grid`;
- ceil timestamps with `ceil_to_grid`;
- at an exact current boundary, `last_closed_open_time_ms` returns the previous
  candle open because the candle starting now is not closed.

### Gap detection

The pure gap detector:

- accepts unsorted timestamps;
- ignores duplicates;
- audits only an explicit half-open interval;
- merges adjacent missing candles into one gap;
- rejects off-grid timestamps rather than silently changing the grid.

### Bounded REST windows

One missing gap is divided into aligned half-open windows containing no more
than the configured exchange request limit.

### Historical lower-bound discovery

Bybit instrument `launchTime` is preserved as a discovery floor and cached
metadata. It is not treated as proof of a candle.

The application must probe forward through bounded windows until the first
actual valid candle is observed, then persist the resolved lower bound by full
`StreamKey`.

### Recovery workflow

The old preflight/repair/postflight principle is preserved:

```text
inspect canonical history
-> find gaps
-> fetch bounded windows
-> ingest through the canonical path
-> audit continuity again
```

A successful HTTP response is not proof of a successful repair.

## Explicitly rejected old semantics

The new project must not restore:

- binary float as canonical price/volume representation;
- identity limited to symbol and timeframe;
- silent `INSERT OR REPLACE` behavior;
- one database class owning every storage responsibility;
- validation after persistence;
- transport adapters writing directly to candle tables;
- one CLI function combining discovery, fetch, validation, persistence,
  diagnostics, repair, and readiness;
- a global current symbol or last-candle cursor;
- treating the number of input rows as the number of actual writes.

## Code mapping

The current skeleton makes preserved semantics explicit in:

```text
src/market_data_service/domain/identity.py
src/market_data_service/domain/timeframes.py
src/market_data_service/domain/windows.py
src/market_data_service/domain/gaps.py
src/market_data_service/domain/candles.py
src/market_data_service/domain/classification.py
src/market_data_service/application/use_cases.py
src/market_data_service/ports/market_data_source.py
src/market_data_service/ports/unit_of_work.py
```

These are not permission to skip the remaining design steps. SQLite schema,
exact persistence normalization, bootstrap state, consumer readiness recovery,
and scheduling remain subject to their planned decisions.
