# Canonical Candle Ingestion Specification

## Purpose
Defines the single canonical decision path that validates, classifies, and persists REST and realtime candle observations.

## Requirements

### Requirement: Exact decimal candle values
MDS SHALL parse OHLCV as finite exact decimal values and SHALL NOT accept binary floating-point input into the canonical decimal model. Persisted/API decimal text SHALL use one normalized non-exponential representation.

#### Scenario: Decimal value is canonicalized exactly
- **WHEN** an observation contains a valid decimal-text OHLCV value
- **THEN** MDS preserves its exact numeric meaning and can serialize it as canonical decimal text without float conversion.

### Requirement: Canonical candle validity
A canonical candle SHALL be confirmed closed, have non-negative timeframe-aligned open time, have the exact timeframe-derived close time, satisfy OHLC ordering invariants, and have non-negative volume.

Invalid or unconfirmed observations SHALL NOT become canonical candles.

#### Scenario: Invalid observation is rejected
- **WHEN** an observation is unconfirmed, off-grid, has invalid close time/OHLC, or negative volume
- **THEN** canonical persistence is not mutated by that observation.

### Requirement: Single ingestion classification
For a configured valid observation, MDS SHALL classify the observation against the existing canonical candle as `committed`, `duplicate`, or `corrected`. Unconfigured, invalid, and unconfirmed observations SHALL be rejected with explicit ingestion classifications.

#### Scenario: Existing identical candle is a duplicate
- **WHEN** a valid observation has the same stream, open time, and OHLCV values as the canonical candle
- **THEN** it is classified as `duplicate` and no replacement write occurs.

### Requirement: Correction authority depends on source
A differing valid REST observation for an existing canonical candle SHALL replace the canonical candle and record correction evidence. A differing WebSocket observation SHALL record correction evidence but SHALL NOT overwrite the existing canonical candle.

#### Scenario: REST correction is authoritative
- **WHEN** a valid Bybit REST observation differs from the stored canonical candle at the same stream/open time
- **THEN** MDS records correction evidence and replaces the canonical candle.

#### Scenario: WebSocket correction is non-authoritative
- **WHEN** a valid WebSocket observation differs from the stored canonical candle at the same stream/open time
- **THEN** MDS records correction evidence without overwriting the canonical candle.

### Requirement: Quarantine preserves rejected evidence
MDS SHALL record quarantine evidence for deterministic candle-validation failures, correction conflicts, unexpected historical rows, and unresolved repair gaps where the corresponding workflow records such evidence.

#### Scenario: Invalid candle creates quarantine evidence
- **WHEN** a configured observation fails canonical candle validation
- **THEN** MDS records the affected range and reason in quarantine while leaving canonical candle storage unchanged.

### Requirement: Candle and stream progress commit atomically
A newly committed canonical candle and any advancement of `latest_committed_open_time_ms` SHALL occur in the same storage transaction. A failed transaction SHALL NOT leave one committed without the other.

#### Scenario: Storage failure rolls back the ingestion decision
- **WHEN** persistence fails before the ingestion transaction commits
- **THEN** neither the candle mutation nor its stream-progress mutation remains committed.

### Requirement: REST and realtime share the canonical ingestion path
Historical REST import and confirmed realtime WebSocket ingestion SHALL use the same canonical validation/classification/persistence use case rather than separate candle mutation rules.

#### Scenario: Same candle yields shared classification semantics
- **WHEN** equivalent REST and confirmed WebSocket observations enter MDS
- **THEN** both are evaluated through the same canonical ingestion classification rules, except for source-specific correction authority.
