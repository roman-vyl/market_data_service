## ADDED Requirements

### Requirement: Open-interest streams serve only the joint window
For a stream whose instrument has `open_interest = true`, `GET /v1/candles` and `POST /v1/historical-candles` SHALL serve only ranges where every bar has both canonical OHLCV and open interest for the same `open_time_ms`: from the later of the two lower bounds to the earlier of the latest committed candle and the latest open interest, plus one step. Unflagged streams SHALL keep their current window.

#### Scenario: Latest bar has no open interest yet
- **WHEN** a flagged stream has committed bar T but open interest at T is not stored
- **THEN** a `/v1/candles` range that includes T is rejected as unavailable

#### Scenario: Range before the open interest history
- **WHEN** a range of a flagged stream starts before its open interest lower bound
- **THEN** it is rejected as unavailable

#### Scenario: Open interest hole inside the range
- **WHEN** a requested range of a flagged stream misses open interest for one bar inside it
- **THEN** it fails the complete-grid rule

### Requirement: Open interest values are returned only on request
When a request for a flagged stream sets `include_open_interest=true`, every candle SHALL carry `open_interest` as a JSON decimal string. Without the parameter, and for every unflagged stream, the response SHALL be identical to the response before this change.

#### Scenario: Existing consumer without the parameter
- **WHEN** a consumer requests a flagged stream without `include_open_interest`
- **THEN** the candles and `market_data_hash` are exactly as before this change

#### Scenario: Opt-in consumer
- **WHEN** a consumer requests a flagged stream with `include_open_interest=true`
- **THEN** every candle carries `open_interest`

### Requirement: Open interest has its own provenance hash
`market_data_hash` SHALL remain the OHLCV-only identity for every stream. An opt-in response SHALL carry `open_interest_hash`, the canonical lowercase 64-hex SHA-256 of ticker, timeframe, range and the ordered `(open_time_ms, open_interest)` pairs. An opt-in `POST /v1/historical-candles` SHALL also accept `expected_open_interest_hash` and fail closed on mismatch.

#### Scenario: Stored provenance still replays
- **WHEN** a hash-bound historical read uses a `market_data_hash` stored before this change for an unchanged range
- **THEN** it still matches

#### Scenario: Open interest changed under a stored hash
- **WHEN** an opt-in historical read gives an `expected_open_interest_hash` that no longer matches the stored values
- **THEN** the read fails closed as stale provenance
