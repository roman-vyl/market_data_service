# Consumer Read API v1 Specification

## Requirement: Pre-implementation architecture gate

Before adding production API code, the change SHALL document the exact existing range-read, stream-state, HTTP registration, and runtime-wiring paths that will be reused. Implementation SHALL proceed only after confirming that the dependency direction remains HTTP adapter → application use case → focused port → SQLite/state adapter.

The gate SHALL reject a design that duplicates canonical range SQL, introduces cyclic dependencies, adds BBB/Workbench presentation models to this service, or makes an existing broad file own validation, serialization, persistence, readiness policy, and routing together.

## Requirement: Cohesive module ownership

Consumer-read orchestration, pure range validation, result invariants, read ports, SQLite adaptation, HTTP routing, transport schemas, Decimal serialization, exception mapping, and concrete wiring SHALL be represented by focused modules or by existing modules already dedicated to the exact same responsibility.

`adapters/http/runtime_server.py` SHALL remain a server/route-composition boundary and SHALL NOT become the implementation location for SQL, readiness decisions, response serialization, or result-grid validation. Runtime and reconciliation modules SHALL NOT absorb consumer API behavior.

## Requirement: Dependency and growth guards

Application consumer-read code SHALL NOT import HTTP framework modules or SQLite adapters. SQLite consumer-read code SHALL NOT import HTTP schemas. HTTP modules SHALL NOT execute SQL or import `sqlite3`. BBB-specific and Workbench-specific DTOs SHALL remain outside `market_data_service`.

Implementation acceptance SHALL include structural/dependency checks and a file-growth review for existing central modules, with extracted focused modules whenever the change would add a second independent responsibility.

## Requirement: Canonical candle range endpoint

The service SHALL expose `GET /v1/candles` with required query parameters `ticker`, `timeframe`, `from_ms`, and `to_ms`.

The requested interval SHALL use aligned half-open semantics `[from_ms, to_ms)`. Version 1 SHALL return the complete requested range in one JSON response and SHALL NOT expose pagination, cursor, offset, page, limit, request chunking, or response-streaming parameters.

## Requirement: Canonical stream identity

The endpoint SHALL accept only canonical configured ticker/timeframe identities. Canonical perpetual tickers SHALL retain their market suffix, for example `BTCUSDT.P` and `ETHUSDT.P`.

An unconfigured ticker/timeframe pair SHALL return `404 configured_stream_not_found`. Version 1 SHALL NOT provide legacy alias resolution such as `BTCUSDT` to `BTCUSDT.P`.

## Requirement: Ready-only consumer admission

The service SHALL return candle data only when the requested configured stream's current status is `ready`.

Every other lifecycle state, including bootstrap, audit, repair, connecting, recovery, degraded, and failed states, SHALL be rejected with `409 stream_not_ready` and SHALL return no candles.

The range query SHALL NOT run continuity audit, gap repair, bootstrap, or realtime recovery. Existing runtime reconciliation remains the authority that establishes and revokes readiness.

## Requirement: Aligned valid range

The service SHALL require `from_ms < to_ms` and SHALL require both boundaries to align exactly to the requested timeframe grid.

Malformed or reversed requests SHALL return `400 invalid_request`. Non-aligned boundaries SHALL return `422 range_not_aligned`.

## Requirement: Proven available window

For a ready stream, the service SHALL determine a proven available half-open window `[available_from_ms, available_to_ms)` from canonical storage and existing stream metadata/state.

A requested range SHALL be served only when it lies completely inside that available window. The service SHALL NOT clamp, truncate, or partially satisfy an out-of-bounds request.

An out-of-bounds request SHALL return `422 range_out_of_bounds` with requested and available boundaries.

## Requirement: Deterministic complete response

A successful response SHALL contain the resolved `ticker`, `timeframe`, `from_ms`, `to_ms`, and a `candles` array.

Candles SHALL be ordered strictly ascending by `open_time_ms`, SHALL have unique open times, SHALL lie inside the requested half-open interval, and SHALL contain only confirmed canonical closed candles.

For an aligned range inside a ready stream's available window, the response SHALL contain the complete expected timeframe grid. The service SHALL NOT return `200` with missing, duplicate, off-grid, or mixed-stream rows.

## Requirement: Decimal-text OHLCV

The response SHALL serialize `open`, `high`, `low`, `close`, and `volume` as normalized decimal JSON strings preserving canonical persisted value semantics.

The consumer API SHALL NOT serialize OHLCV as JSON floating-point numbers.

## Requirement: Ready-state invariant protection

If a SQLite read for a ready stream violates expected order, uniqueness, stream identity, range membership, timeframe alignment, or complete-grid continuity, the service SHALL refuse the successful response and return `500 continuity_invariant_broken`.

The failure SHALL be observable through structured diagnostics. The API SHALL NOT silently reinterpret the result as an ordinary empty or partial range.

## Requirement: Stable error envelope

Consumer API errors SHALL use a stable JSON envelope containing `error`, `message`, `ticker` when known, `timeframe` when known, and `details`.

The service SHALL implement at least these mappings:

- `400 invalid_request`;
- `404 configured_stream_not_found`;
- `409 stream_not_ready`;
- `422 range_not_aligned`;
- `422 range_out_of_bounds`;
- `500 continuity_invariant_broken`;
- `503 service_unavailable`.

## Requirement: Existing runtime endpoints remain stable

Adding `GET /v1/candles` SHALL NOT change the existing contracts of `/health` or `/readiness`.

The process SHALL remain healthy and able to expose diagnostics while candle requests are rejected because one or more streams are not ready.

## Requirement: BBB reference-consumer contract

The OpenSpec SHALL document the required subsequent BBB integration while excluding BBB repository changes from this implementation.

The subsequent BBB change SHALL preserve `research_api` as Workbench BFF, preserve Workbench's existing `/api/market/candles-window` contract, replace direct legacy SQLite reads behind the BBB market-reader boundary with an HTTP client, migrate market-data-facing identity to canonical `.P` tickers, parse Decimal text into Python `Decimal`, and retain explicit conversion to existing float-based research or chart representations only where required.

The subsequent BBB change SHALL include parity tests between the legacy SQLite reader and HTTP reader and SHALL NOT connect Workbench directly to `market_data_service`.

## Requirement: Unpaginated v1 performance evidence

Version 1 SHALL support one-response reads for normal Workbench windows and large research ranges without pagination or chunking.

Implementation acceptance SHALL record SQLite read, JSON serialization, and local HTTP round-trip measurements for representative Workbench-size, approximately 10,000-candle, approximately 100,000-candle, and large multi-year `5m` scenarios.

These measurements SHALL inform future changes. Pagination, chunking, streaming, compact formats, compression, Arrow/Parquet, and consumer caching SHALL remain future options and SHALL NOT block v1 acceptance unless the implementation is operationally unsafe.

## Requirement: Cumulative patch completeness

The cumulative installable patch SHALL include this OpenSpec, all previous runtime reconciliation changes included in its baseline, `docs/integrations/bbb-consumer-api-current-state.docx`, and every new or modified source, test, API documentation, benchmark, README, and planning file produced by implementation.
