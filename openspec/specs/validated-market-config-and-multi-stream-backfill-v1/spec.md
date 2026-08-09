# validated-market-config-and-multi-stream-backfill-v1 Specification

## Purpose
Defines validated multi-market/timeframe configuration and deterministic bounded historical backfill across configured streams.
## Requirements
### Requirement: Fully validated market configuration

The system SHALL load a versioned market configuration and SHALL validate schema version, venue, category, canonical ticker, exact exchange symbol, enabled state, canonical timeframes, and history policy before network or storage mutation.

Duplicate canonical instrument identities, duplicate exact exchange-symbol mappings, and duplicate normalized stream identities SHALL be rejected.

A configuration MAY declare any non-empty subset of the supported canonical timeframe registry for an enabled instrument. `1m` support remains available, but `1m` SHALL NOT be required in every market configuration.

#### Scenario: Fully validated configuration loads successfully before mutation

- **WHEN** a versioned market configuration passes schema version, venue, category, ticker, exchange-symbol, timeframe, enabled-state, and history-policy validation
- **THEN** the loader returns validated instrument coverage
- **AND** no network or storage mutation occurs before validation completes

#### Scenario: Invalid normative field is rejected before mutation

- **WHEN** a configuration entry has an invalid schema version, venue, category, ticker, exchange symbol, timeframe, enabled state, or history policy
- **THEN** loading is rejected before any network or storage mutation occurs

#### Scenario: Duplicate canonical ticker is rejected

- **WHEN** a configuration declares the same canonical instrument identity more than once
- **THEN** loading is rejected

#### Scenario: Duplicate exact exchange-symbol mapping is rejected

- **WHEN** a configuration maps more than one instrument to the same exact exchange symbol
- **THEN** loading is rejected

#### Scenario: Configuration without the minute stream is accepted

- **WHEN** an enabled instrument's configured canonical timeframes omit `1m`
- **THEN** the configuration loads successfully, because `1m` is not required in every market configuration

### Requirement: Verified initial instrument metadata

The initial BTCUSDT.P and ETHUSDT.P mappings SHALL be verified against Bybit linear perpetual instrument metadata. A mismatch SHALL leave the affected configuration invalid and SHALL NOT be silently corrected.

#### Scenario: Exact linear perpetual mapping is accepted

- **WHEN** the configured BTCUSDT.P or ETHUSDT.P exchange-symbol and category mapping exactly matches Bybit linear perpetual instrument metadata
- **THEN** the configured instrument is accepted as verified

#### Scenario: Metadata mismatch invalidates the configuration without silent correction

- **WHEN** the configured exchange-symbol or category mapping for BTCUSDT.P or ETHUSDT.P does not match Bybit linear perpetual instrument metadata
- **THEN** the affected configuration is left invalid
- **AND** the mismatch is not silently corrected

### Requirement: Shared source-failure classification

Historical lower-bound discovery, backfill, and repair SHALL use one source-failure classification contract.

Transport timeout/network failures and explicitly approved transient Bybit responses SHALL be recoverable. Malformed payloads, invalid configuration, symbol mismatch, impossible invariants, and storage/schema corruption SHALL be fatal.

This change SHALL NOT require automatic retry loops.

#### Scenario: Transport and approved transient failures classify as recoverable

- **WHEN** a transport timeout/network failure or an explicitly approved transient Bybit response occurs during lower-bound discovery, backfill, or repair
- **THEN** the shared classifier reports the failure as recoverable

#### Scenario: Payload, configuration, and storage failures classify as fatal

- **WHEN** a malformed payload, invalid configuration, symbol mismatch, impossible invariant, or storage/schema corruption occurs during lower-bound discovery, backfill, or repair
- **THEN** the shared classifier reports the failure as fatal

#### Scenario: Classification is a single decision without a built-in retry loop

- **WHEN** the shared classifier produces a recoverable or fatal disposition for a source failure
- **THEN** the classifier returns that single disposition to the caller
- **AND** does not itself perform automatic retries

### Requirement: Sequential bounded all-stream backfill

Administrative `backfill --all` SHALL expand every enabled instrument into all configured `canonical_timeframes` and SHALL process the resulting ticker-by-timeframe streams sequentially in deterministic configuration order by invoking the existing single-stream full-bootstrap use case.

Each configured ticker-by-timeframe stream SHALL receive an explicit positive candle-window budget. Timeframe grid, Bybit interval mapping, lower-bound discovery, durable resume, audit, and repair SHALL remain stream-scoped. Completed progress SHALL remain durable. A recoverable failure for one stream SHALL be reported and SHALL NOT erase progress or prevent later streams from being attempted. Fatal configuration or schema failures SHALL terminate the command.

No parallel scheduler, worker pool, orchestration table, or second ingestion path SHALL be introduced.

#### Scenario: All configured streams run sequentially in deterministic order

- **WHEN** `backfill --all` runs against a configuration with multiple enabled instruments and timeframes
- **THEN** every resulting ticker-by-timeframe stream is processed exactly once, in deterministic instrument-then-timeframe configuration order, by invoking the existing single-stream full-bootstrap use case

#### Scenario: A recoverable stream failure is reported and later streams still run

- **WHEN** one stream's bootstrap invocation reports a recoverable failure
- **THEN** the failure is reported for that stream
- **AND** that stream's already-durable progress is not erased
- **AND** later configured streams are still attempted

#### Scenario: A fatal stream failure stops the command before later streams

- **WHEN** one stream's bootstrap invocation reports a fatal failure
- **THEN** `backfill --all` terminates
- **AND** later configured streams are not attempted

#### Scenario: A recoverable metadata failure reports every stream for that instrument and continues

- **WHEN** instrument-scoped metadata verification for one instrument reports a recoverable failure
- **THEN** every configured stream for that instrument is reported as affected
- **AND** later instruments are still attempted

#### Scenario: A fatal metadata mismatch stops later instruments

- **WHEN** instrument-scoped metadata verification for one instrument reports a fatal mismatch
- **THEN** `backfill --all` terminates
- **AND** later configured instruments are not attempted

### Requirement: Multi-timeframe stream isolation

Configured streams for the same instrument but different timeframes SHALL have independent storage identity, lower bounds, progress, lifecycle state, continuity gaps, and repair operations. Requests to Bybit SHALL use the canonical interval mapping for each stream timeframe.

#### Scenario: Same-instrument streams at different timeframes carry independent state

- **WHEN** the same instrument is configured with multiple canonical timeframes (for example `1m`, `5m`, and `1h`)
- **THEN** each ticker-by-timeframe stream has its own independent storage identity, lower bound, progress, lifecycle state, continuity gaps, and repair operations

#### Scenario: Each stream's Bybit requests use its own timeframe's interval mapping

- **WHEN** a request to Bybit is made for a configured ticker-by-timeframe stream
- **THEN** the request uses the canonical Bybit interval mapping for that stream's own timeframe

