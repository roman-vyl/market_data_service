# Consumer Read API v1 Specification

## ADDED Requirements

### Requirement: Pre-implementation architecture gate

Before adding production API code, the change SHALL document the exact existing range-read, stream-state, HTTP registration, and runtime-wiring paths that will be reused. Implementation SHALL proceed only after confirming that the dependency direction remains HTTP adapter → application use case → focused port → SQLite/state adapter.

The gate SHALL reject a design that duplicates canonical range SQL, introduces cyclic dependencies, adds BBB/Workbench presentation models to this service, or makes an existing broad file own validation, serialization, persistence, readiness policy, and routing together.

#### Scenario: Architecture audit is recorded before production code is added

- **WHEN** implementation of the consumer read API begins
- **THEN** the exact existing range-read, stream-state, HTTP-registration, and runtime-wiring paths to be reused are documented first
- **AND** the dependency direction is confirmed as HTTP adapter → application use case → focused port → SQLite/state adapter before production code is added

#### Scenario: Gate rejects a design with duplicated SQL, cycles, or mixed responsibilities

- **WHEN** the architecture audit finds a design that duplicates canonical range SQL, introduces a cyclic dependency, adds BBB/Workbench presentation models to this service, or makes an existing broad file own validation, serialization, persistence, readiness policy, and routing together
- **THEN** the gate rejects that design and implementation does not proceed on it

### Requirement: Cohesive module ownership

Consumer-read orchestration, pure range validation, result invariants, read ports, SQLite adaptation, HTTP routing, transport schemas, Decimal serialization, exception mapping, and concrete wiring SHALL be represented by focused modules or by existing modules already dedicated to the exact same responsibility.

`adapters/http/runtime_server.py` SHALL remain a server/route-composition boundary and SHALL NOT become the implementation location for SQL, readiness decisions, response serialization, or result-grid validation. Runtime and reconciliation modules SHALL NOT absorb consumer API behavior.

#### Scenario: Each consumer-read responsibility has a focused owning module

- **WHEN** the consumer-read implementation is inspected
- **THEN** orchestration, pure range validation, result invariants, read ports, SQLite adaptation, HTTP routing, transport schemas, Decimal serialization, exception mapping, and wiring are each represented by a focused module or an existing module already dedicated to that exact responsibility

#### Scenario: The runtime server module stays a composition boundary only

- **WHEN** `adapters/http/runtime_server.py` registers the consumer-read route
- **THEN** it does not itself implement SQL, readiness decisions, response serialization, or result-grid validation
- **AND** runtime and reconciliation modules do not absorb consumer API behavior

### Requirement: Dependency and growth guards

Application consumer-read code SHALL NOT import HTTP framework modules or SQLite adapters. SQLite consumer-read code SHALL NOT import HTTP schemas. HTTP modules SHALL NOT execute SQL or import `sqlite3`. BBB-specific and Workbench-specific DTOs SHALL remain outside `market_data_service`.

Implementation acceptance SHALL include structural/dependency checks and a file-growth review for existing central modules, with extracted focused modules whenever the change would add a second independent responsibility.

#### Scenario: Architecture guard rejects a wrong-direction import

- **WHEN** the architecture test suite runs against the consumer-read modules
- **THEN** it rejects application consumer-read code importing HTTP framework modules or SQLite adapters
- **AND** it rejects SQLite consumer-read code importing HTTP schemas
- **AND** it rejects HTTP modules executing SQL or importing `sqlite3`

#### Scenario: No BBB/Workbench DTOs exist in this service

- **WHEN** the consumer-read module tree is inspected
- **THEN** it contains no BBB-specific or Workbench-specific DTOs

### Requirement: Canonical candle range endpoint

The service SHALL expose `GET /v1/candles` with required query parameters `ticker`, `timeframe`, `from_ms`, and `to_ms`.

The requested interval SHALL use aligned half-open semantics `[from_ms, to_ms)`. Version 1 SHALL return the complete requested range in one JSON response and SHALL NOT expose pagination, cursor, offset, page, limit, request chunking, or response-streaming parameters.

#### Scenario: Endpoint accepts the four required query parameters

- **WHEN** a client issues `GET /v1/candles` with `ticker`, `timeframe`, `from_ms`, and `to_ms`
- **THEN** the request is accepted for further validation and processing using half-open `[from_ms, to_ms)` semantics

#### Scenario: Complete range returns in one response without pagination parameters

- **WHEN** a valid request for a complete range is served
- **THEN** the full requested range is returned in one JSON response
- **AND** the response accepts no pagination, cursor, offset, page, limit, chunking, or streaming parameters

### Requirement: Canonical stream identity

The endpoint SHALL accept only canonical configured ticker/timeframe identities. Canonical perpetual tickers SHALL retain their market suffix, for example `BTCUSDT.P` and `ETHUSDT.P`.

An unconfigured ticker/timeframe pair SHALL return `404 configured_stream_not_found`. Version 1 SHALL NOT provide legacy alias resolution such as `BTCUSDT` to `BTCUSDT.P`.

#### Scenario: Configured canonical ticker with market suffix is accepted

- **WHEN** a request uses a configured canonical ticker such as `BTCUSDT.P`
- **THEN** the ticker is accepted for stream resolution

#### Scenario: Unconfigured ticker/timeframe pair is rejected

- **WHEN** a request's ticker/timeframe pair is not configured
- **THEN** the service returns `404 configured_stream_not_found`
- **AND** it does not resolve a legacy alias such as `BTCUSDT` to `BTCUSDT.P`

### Requirement: Ready-only consumer admission

The service SHALL return candle data only when the requested configured stream's current status is `ready`.

Every other lifecycle state, including bootstrap, audit, repair, connecting, recovery, degraded, and failed states, SHALL be rejected with `409 stream_not_ready` and SHALL return no candles.

The range query SHALL NOT run continuity audit, gap repair, bootstrap, or realtime recovery. Existing runtime reconciliation remains the authority that establishes and revokes readiness.

#### Scenario: Ready stream serves candle data

- **WHEN** the requested configured stream's current status is `ready`
- **THEN** the request proceeds to range validation and candle data may be returned

#### Scenario: Non-ready lifecycle state is rejected without candles

- **WHEN** the requested configured stream's current status is bootstrap, audit, repair, connecting, recovery, degraded, or failed
- **THEN** the service returns `409 stream_not_ready` with no candle data

#### Scenario: The range query never triggers audit, repair, bootstrap, or recovery itself

- **WHEN** a candle range request is processed
- **THEN** it does not itself run continuity audit, gap repair, bootstrap, or realtime recovery
- **AND** it relies on existing runtime reconciliation as the sole authority that establishes and revokes readiness

### Requirement: Aligned valid range

The service SHALL require `from_ms < to_ms` and SHALL require both boundaries to align exactly to the requested timeframe grid.

Malformed or reversed requests SHALL return `400 invalid_request`. Non-aligned boundaries SHALL return `422 range_not_aligned`.

#### Scenario: Reversed or malformed range is rejected

- **WHEN** a request has `from_ms >= to_ms`, or a malformed query parameter
- **THEN** the service returns `400 invalid_request`

#### Scenario: Non-aligned boundary is rejected

- **WHEN** `from_ms` or `to_ms` does not align exactly to the requested timeframe grid
- **THEN** the service returns `422 range_not_aligned`

### Requirement: Proven available window

For a ready stream, the service SHALL determine a proven available half-open window `[available_from_ms, available_to_ms)` from canonical storage and existing stream metadata/state.

A requested range SHALL be served only when it lies completely inside that available window. The service SHALL NOT clamp, truncate, or partially satisfy an out-of-bounds request.

An out-of-bounds request SHALL return `422 range_out_of_bounds` with requested and available boundaries.

#### Scenario: Range fully inside the available window is served

- **WHEN** a requested aligned range lies completely inside the ready stream's proven available window `[available_from_ms, available_to_ms)`
- **THEN** the request proceeds to be read and returned

#### Scenario: Out-of-bounds range is rejected without clamping

- **WHEN** a requested range extends before `available_from_ms` or beyond `available_to_ms`
- **THEN** the service returns `422 range_out_of_bounds` including the requested and available boundaries
- **AND** it does not clamp, truncate, or partially satisfy the request

### Requirement: Deterministic complete response

A successful response SHALL contain the resolved `ticker`, `timeframe`, `from_ms`, `to_ms`, and a `candles` array.

Candles SHALL be ordered strictly ascending by `open_time_ms`, SHALL have unique open times, SHALL lie inside the requested half-open interval, and SHALL contain only confirmed canonical closed candles.

For an aligned range inside a ready stream's available window, the response SHALL contain the complete expected timeframe grid. The service SHALL NOT return `200` with missing, duplicate, off-grid, or mixed-stream rows.

#### Scenario: Successful response echoes request identity and returns ordered candles

- **WHEN** a valid request is served successfully
- **THEN** the response contains the resolved `ticker`, `timeframe`, `from_ms`, `to_ms`, and a `candles` array ordered strictly ascending by `open_time_ms` with unique open times, each candle inside `[from_ms, to_ms)`, containing only confirmed canonical closed candles

#### Scenario: Complete grid is required for a 200 response

- **WHEN** an aligned range inside a ready stream's available window is served
- **THEN** the response contains the complete expected timeframe grid for that range
- **AND** the service does not return `200` for a result with missing, duplicate, off-grid, or mixed-stream rows

### Requirement: Decimal-text OHLCV

The response SHALL serialize `open`, `high`, `low`, `close`, and `volume` as normalized decimal JSON strings preserving canonical persisted value semantics.

The consumer API SHALL NOT serialize OHLCV as JSON floating-point numbers.

#### Scenario: OHLCV fields are decimal text, never floating-point JSON numbers

- **WHEN** a candle is serialized into the response
- **THEN** `open`, `high`, `low`, `close`, and `volume` are normalized decimal JSON strings preserving canonical persisted value semantics
- **AND** none of them are serialized as JSON floating-point numbers

### Requirement: Ready-state invariant protection

If a SQLite read for a ready stream violates expected order, uniqueness, stream identity, range membership, timeframe alignment, or complete-grid continuity, the service SHALL refuse the successful response and return `500 continuity_invariant_broken`.

The failure SHALL be observable through structured diagnostics. The API SHALL NOT silently reinterpret the result as an ordinary empty or partial range.

#### Scenario: Broken ready-state invariant returns 500 instead of partial success

- **WHEN** a SQLite read for a ready stream violates expected order, uniqueness, stream identity, range membership, timeframe alignment, or complete-grid continuity
- **THEN** the service refuses the successful response and returns `500 continuity_invariant_broken` with structured diagnostics
- **AND** it does not silently reinterpret the result as an ordinary empty or partial range

### Requirement: Stable error envelope

Consumer API errors SHALL use a stable JSON envelope containing `error`, `message`, `ticker` when known, `timeframe` when known, and `details`.

The service SHALL implement at least these mappings:

- `400 invalid_request`;
- `404 configured_stream_not_found`;
- `409 stream_not_ready`;
- `422 range_not_aligned`;
- `422 range_out_of_bounds`;
- `500 continuity_invariant_broken`;
- `503 service_unavailable`.

#### Scenario: Every error response uses the stable envelope

- **WHEN** any consumer API error occurs
- **THEN** the response body contains `error`, `message`, `ticker` when known, `timeframe` when known, and `details`

#### Scenario: All required HTTP/error-code mappings are implemented

- **WHEN** the corresponding failure condition occurs
- **THEN** the service returns `400 invalid_request`, `404 configured_stream_not_found`, `409 stream_not_ready`, `422 range_not_aligned`, `422 range_out_of_bounds`, `500 continuity_invariant_broken`, or `503 service_unavailable` respectively

### Requirement: Existing runtime endpoints remain stable

Adding `GET /v1/candles` SHALL NOT change the existing contracts of `/health` or `/readiness`.

The process SHALL remain healthy and able to expose diagnostics while candle requests are rejected because one or more streams are not ready.

#### Scenario: Health and readiness contracts are unchanged by the new endpoint

- **WHEN** `GET /v1/candles` is added to the HTTP application
- **THEN** `/health` and `/readiness` keep their existing contracts

#### Scenario: Process stays healthy while candle requests are rejected

- **WHEN** one or more configured streams are not ready and candle requests for them are rejected
- **THEN** the process remains healthy and continues to expose `/health` and `/readiness` diagnostics

### Requirement: BBB reference-consumer contract

The OpenSpec SHALL document the required subsequent BBB integration while excluding BBB repository changes from this implementation.

The subsequent BBB change SHALL preserve `research_api` as Workbench BFF, preserve Workbench's existing `/api/market/candles-window` contract, replace direct legacy SQLite reads behind the BBB market-reader boundary with an HTTP client, migrate market-data-facing identity to canonical `.P` tickers, parse Decimal text into Python `Decimal`, and retain explicit conversion to existing float-based research or chart representations only where required.

The subsequent BBB change SHALL include parity tests between the legacy SQLite reader and HTTP reader and SHALL NOT connect Workbench directly to `market_data_service`.

#### Scenario: BBB integration is documented, not implemented, by this change

- **WHEN** this change is delivered
- **THEN** the required subsequent BBB integration contract is documented in the OpenSpec
- **AND** no BBB repository changes are made as part of this implementation

#### Scenario: Documented BBB contract preserves Workbench boundaries and adds parity tests

- **WHEN** the documented subsequent BBB change is later implemented
- **THEN** it preserves `research_api` as the Workbench BFF and Workbench's existing `/api/market/candles-window` contract, replaces direct legacy SQLite reads behind the BBB market-reader boundary with an HTTP client, migrates market-data-facing identity to canonical `.P` tickers, parses Decimal text into Python `Decimal`, includes parity tests between the legacy SQLite reader and the HTTP reader, and does not connect Workbench directly to `market_data_service`

### Requirement: Unpaginated v1 performance evidence

Version 1 SHALL support one-response reads for normal Workbench windows and large research ranges without pagination or chunking.

Implementation acceptance SHALL record SQLite read, JSON serialization, and local HTTP round-trip measurements for representative Workbench-size, approximately 10,000-candle, approximately 100,000-candle, and large multi-year `5m` scenarios.

These measurements SHALL inform future changes. Pagination, chunking, streaming, compact formats, compression, Arrow/Parquet, and consumer caching SHALL remain future options and SHALL NOT block v1 acceptance unless the implementation is operationally unsafe.

#### Scenario: Normal and large ranges are served as one unpaginated response

- **WHEN** a normal Workbench-size window or a large research range is requested
- **THEN** it is served as one complete JSON response without pagination or chunking

#### Scenario: Benchmark measurements are recorded for representative range sizes

- **WHEN** implementation acceptance evidence is recorded
- **THEN** SQLite read, JSON serialization, and local HTTP round-trip measurements exist for representative Workbench-size, approximately 10,000-candle, approximately 100,000-candle, and large multi-year `5m` scenarios
- **AND** pagination, chunking, streaming, compact formats, compression, Arrow/Parquet, and consumer caching remain documented future options rather than v1 blockers

### Requirement: Cumulative patch completeness

The cumulative installable patch SHALL include this OpenSpec, all previous runtime reconciliation changes included in its baseline, `docs/integrations/bbb-consumer-api-current-state.docx`, and every new or modified source, test, API documentation, benchmark, README, and planning file produced by implementation.

#### Scenario: Cumulative patch includes the OpenSpec, baseline, docs, and all produced files

- **WHEN** the consumer read API change is delivered
- **THEN** the cumulative installable patch includes this OpenSpec, the previous runtime reconciliation baseline, `docs/integrations/bbb-consumer-api-current-state.docx`, and every new or modified source, test, API documentation, benchmark, README, and planning file produced by the implementation
