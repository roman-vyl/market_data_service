# Design: Complete Phase 2 Operations v1

## Existing components to reuse

- validated domain identity and timeframe registry;
- `BootstrapFullStreamHistory` for one stream;
- Bybit REST candle and instrument metadata adapters;
- existing failure persistence and stream lifecycle;
- canonical market configuration model;
- canonical ingestion and SQLite UoW.

## Configuration boundary

The loader returns validated `InstrumentCoverage` values. It validates schema version, venue, category, canonical ticker, exact exchange symbol, enabled state, timeframes, and history policy. Duplicate canonical tickers, exchange symbols, or normalized stream keys are rejected before any network or storage mutation.

## Failure classification

One application-level classifier maps transport timeout/network errors and approved transient Bybit conditions to recoverable failure. Malformed payloads, invalid configuration, symbol mismatch, schema/storage corruption, and impossible invariants are fatal. This change classifies failures; it does not add automatic retries.

## Multi-stream orchestration

`backfill --all` loads enabled streams in deterministic configuration order and invokes the existing single-stream full-bootstrap use case sequentially. Each stream receives an explicit positive `max_windows_per_stream`. A recoverable failure for one stream is reported and does not erase progress or block later streams. Fatal process-level configuration/schema failures stop execution.

## Persistence

No orchestration progress table is introduced. Resume remains stream-owned through existing durable state.
