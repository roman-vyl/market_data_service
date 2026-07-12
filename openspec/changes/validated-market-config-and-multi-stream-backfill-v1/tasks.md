# Tasks: Validated Market Configuration and Multi-Stream Backfill v1

- [ ] Audit the current partial `markets.toml` loader against the normative configuration schema.
- [ ] Validate schema version, venue, category, ticker, exchange symbol, timeframes, enabled state, and history policy.
- [ ] Reject duplicate ticker, exchange-symbol, and normalized stream identities.
- [ ] Add negative configuration tests.
- [ ] Validate BTCUSDT.P and ETHUSDT.P metadata assumptions against Bybit linear instruments-info.
- [ ] Define and implement the shared source-failure classification table.
- [ ] Apply the classifier consistently to lower-bound discovery, backfill, and repair.
- [ ] Add deterministic sequential `backfill --all` application orchestration.
- [ ] Add bounded `backfill --all` CLI wiring with explicit per-stream budget.
- [ ] Add multi-stream isolation, recoverable-failure continuation, fatal-stop, and restart/resume tests.
- [ ] Add a real Bybit two-stream bounded smoke.
- [ ] Update README, acceptance matrix, and the base `market-data-service-v1` task statuses.
