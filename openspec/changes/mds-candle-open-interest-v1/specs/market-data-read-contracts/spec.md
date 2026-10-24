## ADDED Requirements

### Requirement: Candle reads carry open interest
`/v1/candles` and `/v1/historical-candles` SHALL return `open_interest` for each candle (null where not stored) and an
`open_interest_hash` over `(open_time_ms, open_interest)`. `market_data_hash` SHALL stay computed over OHLCV only.

#### Scenario: Experiment saved before OI was loaded
- **WHEN** OI history is loaded for a range an experiment already read with a hash-bound historical read
- **THEN** that read still succeeds with the same `market_data_hash`.
