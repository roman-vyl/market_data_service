# Service Boundary and Ownership Specification

## Purpose
Defines the durable responsibility and ownership boundary of Market Data Service as an independent market-data service.

## Requirements

### Requirement: MDS is an independent service boundary
MDS SHALL run as an independently deployable service with its own package, process lifecycle, configuration, and canonical database. Production MDS code SHALL NOT require runtime imports from BBB, Strategy Engine, Strategy Runtime, ABI Executor, or other downstream consumer/trading services.

#### Scenario: Downstream service is absent
- **WHEN** MDS starts without any downstream BBB trading/research service imported into its process
- **THEN** its market-data runtime can initialize and operate through its own service boundaries.

### Requirement: MDS owns market-data responsibilities only
MDS SHALL obtain, validate, reconcile, persist, and expose canonical market candle data. It SHALL NOT compute strategy features, evaluate strategies, generate trading signals, execute orders, or manage positions.

#### Scenario: Strategy logic is requested
- **WHEN** a consumer needs indicators, strategy evaluation, or order execution
- **THEN** that responsibility remains outside MDS rather than being added to its market-data runtime.

### Requirement: MDS is the single owner of canonical SQLite
The canonical SQLite database SHALL have one service-process owner: MDS. Downstream consumers SHALL use service contracts and SHALL NOT require direct access to the canonical database file.

#### Scenario: Consumer reads canonical candles
- **WHEN** another service needs MDS candle data
- **THEN** it uses an MDS read contract rather than opening the canonical SQLite file directly.
