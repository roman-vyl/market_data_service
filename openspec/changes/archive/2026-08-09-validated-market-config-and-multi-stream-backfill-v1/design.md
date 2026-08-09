# Design: Validated Market Configuration and Multi-Stream Backfill v1

## Existing components to reuse

- validated domain identity and timeframe registry;
- `BootstrapFullStreamHistory` for one stream;
- Bybit REST candle and instrument metadata adapters;
- existing failure persistence and stream lifecycle;
- canonical market configuration model;
- canonical ingestion and SQLite UoW.

## Configuration boundary

The loader returns validated `InstrumentCoverage` values. It validates schema version, venue, category, canonical ticker, exact exchange symbol, enabled state, non-empty supported timeframes, and history policy. Duplicate canonical tickers, exchange symbols, or normalized stream keys are rejected before any network or storage mutation. The loader does not require every enabled instrument to include `1m`; operators may declare narrower bounded smoke or runtime coverage.

## Failure classification

One application-level classifier maps transport timeout/network errors and approved transient Bybit conditions to recoverable failure. Malformed payloads, invalid configuration, symbol mismatch, schema/storage corruption, and impossible invariants are fatal. This change classifies failures; it does not add automatic retries.

## Multi-stream orchestration

`backfill --all` expands enabled instrument coverage into every configured `StreamKey` in deterministic instrument-then-timeframe order and invokes the existing single-stream full-bootstrap use case sequentially. Each ticker-by-timeframe stream receives an explicit positive `max_windows_per_stream`; metadata verification remains instrument-scoped while bootstrap, progress, audit, and repair remain stream-scoped. A recoverable failure for one stream is reported and does not erase progress or block later streams. Fatal process-level configuration/schema failures stop execution.

## Persistence

No orchestration progress table is introduced. Resume remains stream-owned through existing durable state.
