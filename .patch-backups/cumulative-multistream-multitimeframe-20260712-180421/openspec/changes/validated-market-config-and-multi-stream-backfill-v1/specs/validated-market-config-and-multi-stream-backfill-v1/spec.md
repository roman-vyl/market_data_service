# Specification: Validated Market Configuration and Multi-Stream Backfill v1

## Requirement: Fully validated market configuration

The system SHALL load a versioned market configuration and SHALL validate schema version, venue, category, canonical ticker, exact exchange symbol, enabled state, canonical timeframes, and history policy before network or storage mutation.

Duplicate canonical instrument identities, duplicate exact exchange-symbol mappings, and duplicate normalized stream identities SHALL be rejected.

## Requirement: Verified initial instrument metadata

The initial BTCUSDT.P and ETHUSDT.P mappings SHALL be verified against Bybit linear perpetual instrument metadata. A mismatch SHALL leave the affected configuration invalid and SHALL NOT be silently corrected.

## Requirement: Shared source-failure classification

Historical lower-bound discovery, backfill, and repair SHALL use one source-failure classification contract.

Transport timeout/network failures and explicitly approved transient Bybit responses SHALL be recoverable. Malformed payloads, invalid configuration, symbol mismatch, impossible invariants, and storage/schema corruption SHALL be fatal.

This change SHALL NOT require automatic retry loops.

## Requirement: Sequential bounded all-stream backfill

Administrative `backfill --all` SHALL process enabled streams sequentially in deterministic configuration order by invoking the existing single-stream full-bootstrap use case.

Each stream SHALL receive an explicit positive candle-window budget. Completed progress SHALL remain durable. A recoverable failure for one stream SHALL be reported and SHALL NOT erase progress or prevent later streams from being attempted. Fatal configuration or schema failures SHALL terminate the command.

No parallel scheduler, worker pool, orchestration table, or second ingestion path SHALL be introduced.
