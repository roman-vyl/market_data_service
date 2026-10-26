## MODIFIED Requirements

### Requirement: Candle responses preserve exact data and provenance
A successful candle-range response SHALL include `ticker`, `timeframe`, `from_ms`, `to_ms`, `market_data_hash`, and ordered candles. Candle OHLCV SHALL be JSON decimal strings. `market_data_hash` SHALL be the canonical lowercase 64-hex SHA-256 identity of the exact stream/range/ordered OHLCV set.

Every candle of a successful `GET /v1/candles` or `POST /v1/historical-candles` response SHALL carry `open_interest`: the stored open interest of the same stream at the same `open_time_ms` as a JSON decimal string, or `null` when no open interest is stored for that bar. It SHALL NOT be taken from another bar, filled forward, or replaced by zero. Candles and their open interest SHALL be read in the same read transaction as the stream state. `open_interest` SHALL NOT enter `market_data_hash` or the `coverage_stale` comparison.

#### Scenario: Same exact range has stable provenance identity
- **WHEN** the canonical stream/range and ordered candle OHLCV set are unchanged
- **THEN** MDS computes the same `market_data_hash`, whether or not open interest is stored for those bars.

#### Scenario: Candle carries the open interest of its own bar
- **WHEN** open interest is stored for the stream at the candle's `open_time_ms`
- **THEN** that candle's `open_interest` is the stored value as a decimal string.

#### Scenario: Bar without open interest
- **WHEN** no open interest is stored for the stream at the candle's `open_time_ms` (instrument without open interest, bar before the open interest history, or a stream other than the 5m stream of an instrument with `open_interest = true`)
- **THEN** that candle's `open_interest` is `null`
- **AND** the value of a neighbouring bar is not used.

#### Scenario: Historical read with open interest stored after the audit
- **WHEN** open interest for bars of an audited range is stored after the audit and the OHLCV is unchanged
- **THEN** `POST /v1/historical-candles` with the audited hash succeeds and returns the stored open interest.
