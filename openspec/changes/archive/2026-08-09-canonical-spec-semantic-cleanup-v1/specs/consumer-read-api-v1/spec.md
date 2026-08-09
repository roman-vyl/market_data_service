# Specification: Consumer Read API v1 — canonical semantic cleanup

## REMOVED Requirements

### Requirement: Pre-implementation architecture gate

Reason: this was a one-time pre-implementation delivery gate ("before adding
production API code, the change SHALL document..."), not a durable runtime
contract. It is preserved as historical evidence in
`openspec/changes/archive/2026-08-09-consumer-read-api-v1/`.

### Requirement: BBB reference-consumer contract

Reason: this is an instruction to a different repository (BBB) about future
work in that repository, not a contract MDS itself is bound by. The MDS-side
guarantees it depends on (canonical `.P` tickers, Decimal-text OHLCV, the
error envelope) are already stated as durable requirements elsewhere in this
capability. Preserved as historical evidence in the archive.

### Requirement: Unpaginated v1 performance evidence

Reason: the durable behavioral guarantee here (one unpaginated JSON response)
duplicates the "Canonical candle range endpoint" requirement; the remainder
is acceptance-benchmark evidence to record during delivery, not a runtime
contract. Preserved as historical evidence in the archive.

### Requirement: Cumulative patch completeness

Reason: pure delivery/patch bookkeeping ("the cumulative installable patch
SHALL include..."), not a runtime contract. Preserved as historical evidence
in the archive.

## MODIFIED Requirements

### Requirement: Dependency and growth guards

Application consumer-read code SHALL NOT import HTTP framework modules or SQLite adapters. SQLite consumer-read code SHALL NOT import HTTP schemas. HTTP modules SHALL NOT execute SQL or import `sqlite3`. BBB-specific and Workbench-specific DTOs SHALL remain outside `market_data_service`.

#### Scenario: Architecture guard rejects a wrong-direction import

- **WHEN** the architecture test suite runs against the consumer-read modules
- **THEN** it rejects application consumer-read code importing HTTP framework modules or SQLite adapters
- **AND** it rejects SQLite consumer-read code importing HTTP schemas
- **AND** it rejects HTTP modules executing SQL or importing `sqlite3`

#### Scenario: No BBB/Workbench DTOs exist in this service

- **WHEN** the consumer-read module tree is inspected
- **THEN** it contains no BBB-specific or Workbench-specific DTOs
