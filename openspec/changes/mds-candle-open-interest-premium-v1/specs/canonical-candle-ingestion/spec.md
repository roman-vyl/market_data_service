## ADDED Requirements

### Requirement: Optional open interest and premium on 5m candles
For an instrument that enables `candle_extras`, each 5m candle row SHALL carry the Bybit open interest at its open time and the
close of the Bybit premium-index 5m bar with the same open time, as exact decimals. Other rows SHALL leave them empty.

#### Scenario: Instrument without extras
- **WHEN** an instrument does not enable `candle_extras`
- **THEN** its candles, reads and hashes are exactly as before.

### Requirement: Extras never change OHLCV or the candle hash
Writing extras SHALL update only the extra fields of an existing candle row, SHALL NOT create a candle, and SHALL NOT change
`market_data_hash`, which stays computed over OHLCV.

#### Scenario: Extras loaded after an experiment was saved
- **WHEN** OI is backfilled for a range an experiment already read
- **THEN** the hash-bound historical read of that experiment still succeeds.

### Requirement: Extras are loaded for history and live
MDS SHALL fill extras for the full available history and, live, for each newly committed 5m bar, through one use case with
REST retry and atomic commit of the values with the extras progress.

#### Scenario: Live bar
- **WHEN** the 5m candle opening at 10:00 is committed at 10:05
- **THEN** MDS writes its open interest and premium shortly after, and reads of that bar return them.
