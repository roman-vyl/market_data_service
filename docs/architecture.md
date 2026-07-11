# Architecture Boundaries

## Dependency direction

```text
domain <- application <- ports/adapters <- entrypoints
```

In practical Python imports:

```text
entrypoints -> application, ports, adapters
adapters    -> ports, domain
application -> ports, domain
ports       -> domain where contract types are needed
domain      -> Python standard library only
```

## Package map

```text
src/market_data_service/
├── domain/          # market rules and immutable concepts
├── application/     # use cases and lifecycle scenarios
├── ports/           # interfaces required by application
├── adapters/
│   ├── bybit/       # REST and WebSocket normalization
│   ├── sqlite/      # migrations and persistence
│   └── http/        # consumer-facing API
└── entrypoints/     # composition roots and process startup
```

## Hard rules

1. REST and WebSocket adapters never write directly to canonical tables.
2. Every candle enters through one application ingestion use case.
3. Candle mutation and `stream_state` advancement share one transaction.
4. Cold start, repair, reconnect, and readiness are explicit use cases.
5. Historical bootstrap, catch-up, and repair do not require consumer events; readiness plus range reads provide recovery.
6. `1m` is the mandatory canonical source stream for every configured symbol.
7. Full available minute history is the default continuity obligation.
8. Old BBB Data Engine algorithms are inspected and ported with parity tests where useful; its CLI/storage architecture is not copied wholesale.
9. Files remain focused. Mixed network + SQL + domain + lifecycle mega-modules are forbidden.

## File-size discipline

No numerical line limit substitutes for design review, but the following are review triggers:

- a production module exceeds roughly 300 lines;
- a class owns more than one application scenario;
- a module contains both SQL and network calls;
- a module contains both external payload parsing and canonical acceptance decisions;
- a file requires broad names such as `manager`, `helpers`, or `utils`.

When a trigger appears, split by owned concept or use case before adding more behavior.

## Multi-symbol state isolation

The service is not a single-symbol daemon. All mutable state is keyed by `StreamKey = ticker + timeframe`.

Forbidden process-wide state includes:

- `current_symbol`;
- one global `last_candle_time`;
- one global bootstrap cursor;
- one global gap collection;
- one global readiness boolean without per-stream evidence.

Adding a supported instrument is a validated configuration change. Domain, application, and storage APIs must accept explicit identities instead of reading hidden globals.


## Audited domain module map

The old BBB Data Engine audit is reflected directly in the package skeleton:

```text
domain/identity.py       stable instrument and stream identity
domain/timeframes.py     one registry plus timeframe-grid math
domain/windows.py        half-open interval contract
domain/gaps.py           pure gap detection and bounded fetch windows
domain/candles.py        observed-versus-canonical candle boundary
domain/classification.py ingestion outcome vocabulary
application/use_cases.py explicit orchestration boundaries
ports/market_data_source.py vendor-neutral metadata/history capabilities
ports/unit_of_work.py     future atomic commit capability
```

No `utils.py`, catch-all manager, database facade, or transport-specific domain
type may replace these boundaries.

## Consumer recovery boundary

The service guarantees canonical continuity only while a stream is `ready`. A consumer may remain connected for status and reads while a stream is not ready, but must pause trading or feature decisions. The consumer owns its own `last_processed_open_time_ms`; after startup or recovery it requests all candles after that cursor and then resumes.

Schema v1 contains no market-event log, replay broker, or server-owned consumer cursor. Any future push transport is only a latency hint and never replaces canonical range reads.

## Sequential backfill boundary

Backfill planning is pure application logic. The entrypoint parses command arguments, the planner selects ordered streams and a finite budget, the Bybit adapter fetches one window, and the ingestion use case commits that window. No module may combine CLI parsing, REST pagination, candle validation, SQLite writes, and lifecycle transitions into one backfill manager.
