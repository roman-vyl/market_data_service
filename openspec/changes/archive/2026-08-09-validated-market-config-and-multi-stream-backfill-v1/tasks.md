# Tasks: Validated Market Configuration and Multi-Stream Backfill v1

- [x] Audit the current partial `markets.toml` loader against the normative configuration schema.
- [x] Validate schema version, venue, category, ticker, exchange symbol, timeframes, enabled state, and history policy.
- [x] Reject duplicate ticker, exchange-symbol, and normalized stream identities.
- [x] Add negative configuration tests.
- [x] Validate BTCUSDT.P and ETHUSDT.P metadata assumptions against Bybit linear instruments-info.
- [x] Define and implement the shared source-failure classification table.
- [x] Apply the classifier consistently to lower-bound discovery, backfill, and repair.
- [x] Add deterministic sequential `backfill --all` orchestration for every enabled ticker-by-timeframe stream.
- [x] Add bounded `backfill --all` CLI wiring with explicit per-stream budget.
- [x] Add multi-symbol and multi-timeframe isolation, recoverable-failure continuation, fatal-stop, and restart/resume tests.
- [x] Add a real Bybit bounded smoke across all configured ticker-by-timeframe streams.
- [x] Update README, acceptance matrix, and the base `market-data-service-v1` task statuses.
