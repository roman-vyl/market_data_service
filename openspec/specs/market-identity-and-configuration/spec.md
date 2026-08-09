# Market Identity and Configuration Specification

## Purpose
Defines canonical instrument/stream identity and validated operator-declared market coverage used by all MDS ingestion and read paths.

## Requirements

### Requirement: Canonical instrument identity
MDS SHALL identify a configured perpetual instrument by a canonical `.P` ticker. The exchange symbol SHALL remain transport metadata and SHALL NOT be part of canonical instrument identity.

#### Scenario: Canonical ticker and exchange symbol remain distinct
- **WHEN** `BTCUSDT.P` is configured with exchange symbol `BTCUSDT`
- **THEN** the canonical instrument identity is `BTCUSDT.P`
- **AND** `BTCUSDT` is used only as the exchange mapping.

### Requirement: Canonical stream identity
A canonical candle stream SHALL be identified by canonical instrument plus one supported canonical timeframe.

Supported timeframe identifiers SHALL be `1m`, `5m`, `15m`, `1h`, `4h`, and `1d`, each with one deterministic Bybit interval mapping and duration.

#### Scenario: Stream identity is normalized
- **WHEN** MDS constructs a stream from a configured canonical ticker and supported timeframe
- **THEN** its identity is `<canonical ticker>:<canonical timeframe>`.

### Requirement: Timeframe grid semantics are deterministic
Each supported canonical timeframe SHALL define one duration and one Bybit interval mapping used consistently by domain, REST, WebSocket, audit, repair, and read paths. Grid floor/ceiling and latest-closed calculations SHALL be deterministic; at an exact timeframe boundary, the latest closed candle open SHALL be the previous grid point.

#### Scenario: Exact boundary resolves the previous closed candle
- **WHEN** current time is exactly on a canonical timeframe boundary
- **THEN** latest-closed resolution returns the previous candle open rather than the candle opening at the current boundary.

### Requirement: Validated versioned market configuration
MDS SHALL validate configuration schema version, source venue/category, instrument identity, exact exchange symbol, enabled state, canonical timeframe list, and history policy before using the configuration for runtime work.

Duplicate canonical tickers, duplicate exact exchange symbols, duplicate timeframes within one instrument, and duplicate normalized stream identities SHALL be rejected.

#### Scenario: Invalid or duplicate configuration is rejected
- **WHEN** configuration contains an unsupported normative field value or duplicate canonical identity
- **THEN** configuration loading fails before the configuration is admitted for runtime work.

### Requirement: Configurable timeframe coverage
Each enabled instrument SHALL declare a non-empty subset of the supported canonical timeframe registry. `1m` SHALL be supported but SHALL NOT be mandatory.

#### Scenario: Configuration without one-minute coverage is valid
- **WHEN** an enabled instrument declares only `5m`, `1h`, `4h`, and `1d`
- **THEN** the configuration is valid if all other validation rules pass.

### Requirement: Exchange metadata verification
Before administrative all-stream bootstrap accepts an instrument mapping, MDS SHALL verify the configured exchange symbol against Bybit linear-perpetual metadata, including category, contract type, trading status, settlement coin, and launch time.

A metadata mismatch SHALL fail that mapping rather than silently rewriting it.

#### Scenario: Mismatched perpetual metadata fails verification
- **WHEN** Bybit metadata for a configured instrument does not match its declared linear-perpetual mapping
- **THEN** MDS reports the mismatch and does not silently correct the configuration.
