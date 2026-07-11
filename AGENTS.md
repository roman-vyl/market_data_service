# Agent Rules — Market Data Service

These rules apply to every implementation and review task in this repository.

## 1. Preserve the service boundary

This repository owns canonical market data only.

Agents MUST NOT add:

- strategy feature calculations;
- indicators such as EMA, RSI, ATR, ADX, or DMI;
- entry/exit logic;
- signal generation;
- order or position management;
- Abi Executor orchestration;
- runtime imports from BBB or Abi Executor.

## 2. Enforce architectural layers

The dependency direction is:

```text
domain <- application <- ports/adapters <- entrypoints
```

Allowed responsibilities:

- `domain`: immutable market concepts, validation rules, classifications, timeframe math;
- `application`: use cases and orchestration expressed through ports;
- `ports`: interfaces required by application use cases;
- `adapters`: Bybit, SQLite, HTTP, clock, and logging implementations;
- `entrypoints`: process startup, API wiring, administrative CLI wiring.

Forbidden dependencies:

- domain importing application, ports, adapters, or entrypoints;
- application importing concrete SQLite, Bybit, FastAPI, or WebSocket clients;
- adapters containing canonical business decisions;
- HTTP handlers or CLI commands implementing ingestion, repair, or validation logic;
- REST and WebSocket adapters writing directly to canonical tables.

## 3. Keep modules small and cohesive

Each module SHALL have one clear reason to change.

Agents MUST:

- prefer focused files and explicit composition;
- extract transport-independent logic from transport adapters;
- split code before a file becomes a mixed-responsibility module;
- avoid generic `utils.py`, `helpers.py`, `manager.py`, or `service.py` dumping grounds;
- use precise names that describe the owned concept or use case;
- keep public interfaces narrow.

A large file is not automatically wrong, but a file combining domain rules, storage SQL, network I/O, process lifecycle, and API presentation is always wrong.

## 3.1 Preserve proven old Data Engine semantics

Agents MUST inspect the old BBB Data Engine before redesigning timeframe math, gap detection, REST window splitting, Bybit retry behavior, launch-time caching, quarantine diagnostics, WAL settings, or postflight audits.

Good existing solutions must be ported with parity tests unless a documented architectural reason requires different behavior. Agents MUST NOT replace proven behavior merely to produce a cosmetically new implementation.

## 4. One canonical ingestion path

Every observed candle, regardless of source, MUST pass through the same application use case:

```text
source payload -> adapter normalization -> observed candle -> validation/classification -> atomic commit
```

REST backfill, REST repair, startup catch-up, reconnect catch-up, and WebSocket delivery MUST NOT implement separate candle acceptance semantics.

## 5. Transaction and readiness invariant

For a newly accepted candle, the candle mutation and corresponding `stream_state` update MUST commit atomically.

Agents MUST NOT:

- advance `stream_state` without the durable candle mutation;
- invoke consumer callbacks inside the transaction;
- add an event log, replay queue, or consumer cursor table without an approved later change;
- treat a transport notification as the source of truth.

Readiness is the processing gate. Consumers own their own last-processed cursor and catch up through canonical candle range reads after startup or recovery.

## 5.1 Minute-history contract

Every configured symbol has a mandatory canonical `1m` stream. The default product goal is full available minute history from the earliest candle Bybit actually exposes.

Agents MUST NOT introduce a shallow rolling retention default without an approved specification change. Historical bootstrap, catch-up, and repair MUST update canonical storage without inventing mandatory per-candle consumer events.

## 6. Explicit operational state

Cold start, catch-up, audit, repair, degraded state, reconnect, and readiness are first-class application scenarios.

Do not hide lifecycle state in logs, process-local booleans, or incidental exceptions. Persist durable stream progress where recovery depends on it.

## 7. No blind code copying

The old BBB Data Engine is a behavioral reference only.

When porting code:

- inspect the original implementation;
- port only the required semantics;
- adapt APIs to the new architecture;
- add parity tests;
- document intentional differences;
- do not import the old package at runtime;
- do not copy historical CLI orchestration wholesale.

## 8. Specification-driven work

Before changing architecture or observable behavior:

- read `docs/master-plan.md`;
- read the active change under `openspec/changes/`;
- update proposal/design/spec/tasks when the agreed contract changes;
- implement only the approved phase;
- do not silently expand scope.

## 9. Verification

Every behavior change requires tests at the lowest useful level.

At minimum, preserve:

- deterministic timeframe boundary tests;
- validation tests;
- duplicate/correction classification tests;
- transaction rollback tests;
- restart idempotency tests;
- gap and repair tests;
- architecture dependency tests once package layers exist.

Run `make verify` before declaring a task complete.

## 10. No legacy compatibility by default

This is a new service. Do not introduce compatibility shims, dual reads, deprecated shapes, or migration paths for designs that have never been released.

When a pre-production contract changes, update the implementation and tests directly unless an explicit compatibility requirement is approved.

## Multi-symbol rules

- Never hard-code BTCUSDT as the production default.
- Every candle, gap, bootstrap cursor, subscription, and readiness state must carry an explicit stream identity.
- Adding ETHUSDT or another supported pair must require configuration and tests, not duplicated orchestration code.
- Do not share mutable per-symbol state through module globals or singleton managers.
- Failure or bootstrap progress for one stream must not overwrite another stream's durable state.

## Instrument and stream identity rules

- `InstrumentKey` is exactly the canonical `.P` ticker for v1; the exact Bybit API symbol is separate instrument metadata.
- Do not add mutable metadata, enabled state, history policy, or display fields to identity.
- Keep `InstrumentMetadata` and operator `InstrumentCoverage` separate.
- `StreamKey` must validate timeframe through the canonical registry.
- Every enabled instrument must include a canonical `1m` stream.
- Instrument-level and stream-level state must not be collapsed into global singleton state.

## Canonical numeric policy

- Domain OHLCV values use Python `Decimal`.
- SQLite and JSON persist canonical decimal strings, never binary floats.
- Bybit adapters preserve decimal strings; repositories do not normalize independently.
- `NaN`, infinities, binary float input, and non-canonical storage text are rejected.


## Persisted stream lifecycle

- Read `docs/stream-state-machine.md` before changing startup, bootstrap, audit, repair, reconnect, or readiness behavior.
- Never jump directly from `uninitialized`, `bootstrapping`, `repairing`, or `failed` to `ready`.
- Repair returns to audit. Connecting plus trailing catch-up precedes ready.
- A persisted ready state is not trusted after restart without reconciliation.
- Keep transition validation in the domain, scenario decisions in application use cases, and persistence in the SQLite adapter.
- Do not create a universal lifecycle manager.

## Backfill simplicity

- Do not introduce a parallel REST scheduler in v1.
- Historical work is sequential, bounded by an explicit window budget, and resumable from durable data.
- Do not add Redis, distributed jobs, worker pools, or priority queues for initial backfill.
- Keep planning, fetching, ingestion, and CLI wiring in separate small modules.
- A long-running service startup must not silently begin an unlimited deep-history bootstrap.
