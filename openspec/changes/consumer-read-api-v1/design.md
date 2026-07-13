# Design: Consumer Read API v1

## 1. Architectural position and implementation gate

The HTTP API is an inbound adapter over an application read use case. It is not a second business layer and SHALL NOT contain SQL, continuity repair, runtime orchestration, or BBB presentation behavior.

```text
HTTP route
→ GetCandleRange application use case
→ focused consumer-read ports
→ SQLite read/state adapters
```

### 1.1 Repository-aware placement

The current repository uses flat application modules, focused adapter packages, `ports/storage.py`, `adapters/http/runtime_server.py`, and runtime composition in `runtime/wiring.py`. Consumer API implementation SHALL respect those conventions while preventing existing files from accumulating unrelated responsibilities.

The target module ownership is:

```text
src/market_data_service/
├── application/
│   └── consumer_read/
│       ├── __init__.py
│       ├── get_candle_range.py      # use-case orchestration only
│       ├── models.py                # application request/result records
│       ├── validation.py            # half-open/alignment/window rules
│       ├── invariants.py            # complete-grid/result checks
│       └── errors.py                # application-level read failures
├── ports/
│   └── consumer_read.py             # candle-range and availability/state read protocols
├── adapters/
│   ├── sqlite/
│   │   └── consumer_candle_reader.py # port implementation; reuse canonical repository semantics
│   └── http/
│       ├── runtime_server.py         # route registration only; minimal composition changes
│       └── consumer_read/
│           ├── __init__.py
│           ├── router.py             # HTTP parameter extraction and use-case call
│           ├── schemas.py            # transport response/error schema
│           ├── serialization.py      # Decimal-text conversion only
│           └── exception_mapping.py  # application error → HTTP response
└── runtime/
    └── wiring.py                     # concrete dependency assembly only
```

Exact filenames may be adjusted only when an existing focused module already owns that exact responsibility. The implementation SHALL NOT create a parallel abstraction merely to match this diagram. Any deviation SHALL be explained in the implementation notes.

### 1.2 Responsibility boundaries

- `get_candle_range.py` owns orchestration: resolve stream, require ready, validate range, read, enforce invariants, return an application result.
- `validation.py` owns pure boundary/alignment/available-window rules and contains no I/O.
- `invariants.py` owns pure result-grid/order/identity checks and contains no HTTP or SQL.
- `ports/consumer_read.py` defines only read capabilities required by this use case. It does not expose HTTP status codes or SQLite rows.
- `consumer_candle_reader.py` owns persistence mapping and range SQL/repository delegation only. It does not decide readiness or HTTP errors.
- HTTP `router.py` performs transport parsing and calls the use case. It does not inspect SQLite, runtime internals, or calculate continuity.
- `schemas.py`, `serialization.py`, and `exception_mapping.py` remain transport-only modules.
- `runtime/wiring.py` is the only place that connects concrete SQLite/state adapters to the use case and route.

### 1.3 Mandatory pre-implementation audit

Before Slice 1 implementation, record in the change notes or design update:

1. the exact existing range-read method that will be reused or minimally extended;
2. the existing stream-state/availability reads used to prove `ready` and available boundaries;
3. the exact minimal route-registration change in `runtime_server.py`;
4. the dependency graph showing application code does not import HTTP or SQLite adapters;
5. whether `ports/storage.py` remains unchanged or why a focused extension is unavoidable;
6. the files expected to grow and their responsibilities before and after the change.

Production implementation SHALL not begin until this audit shows no duplicated SQL path, cyclic dependency, or mixed-responsibility file.

### 1.4 File-size and cohesion guard

There is no arbitrary line-count acceptance threshold, but the implementation SHALL treat substantial growth of `runtime_server.py`, `runtime/wiring.py`, `ports/storage.py`, or `adapters/sqlite/candle_repository.py` as an architectural review trigger. New consumer concerns SHALL be extracted rather than appended when they introduce a second independent responsibility.

Tests SHALL include dependency/import guards or equivalent structural assertions proving:

- application consumer-read modules do not import `adapters.http` or `adapters.sqlite`;
- SQLite consumer adapters do not import HTTP schemas or framework types;
- HTTP router/serialization modules do not import `sqlite3` or execute SQL;
- BBB/Workbench-specific DTOs are absent from `market_data_service`;
- ingestion, reconciliation, and realtime modules remain unchanged except for minimal wiring/read-state reuse.

## 2. Public endpoint

```http
GET /v1/candles?ticker=BTCUSDT.P&timeframe=5m&from_ms=1710000000000&to_ms=1710100000000
```

Parameters:

| Parameter | Type | Contract |
|---|---:|---|
| `ticker` | string | Configured canonical ticker, including market suffix such as `.P` |
| `timeframe` | string | Configured textual timeframe such as `5m`, `1h`, `4h`, `1d` |
| `from_ms` | integer | Inclusive aligned candle-open boundary |
| `to_ms` | integer | Exclusive aligned boundary |

No v1 parameter exists for `limit`, `offset`, `page`, `cursor`, or chunks.

## 3. Successful response

The exact v1 response contract is:

```json
{
  "ticker": "BTCUSDT.P",
  "timeframe": "5m",
  "from_ms": 1710000000000,
  "to_ms": 1710100000000,
  "candles": [
    {
      "open_time_ms": 1710000000000,
      "open": "68450.10",
      "high": "68520.00",
      "low": "68390.40",
      "close": "68480.70",
      "volume": "123.456"
    }
  ]
}
```

The envelope repeats the resolved request identity and boundaries so consumers can validate that the response corresponds to the requested stream and window. Each candle contains only canonical OHLCV data; ticker and timeframe are not repeated per row.

Response guarantees:

- `candles` is ordered strictly ascending by `open_time_ms`;
- `open_time_ms` values are unique;
- every candle lies inside `[from_ms, to_ms)`;
- only confirmed canonical closed candles are returned;
- OHLCV values use normalized decimal text and are never JSON floating-point numbers;
- a successful response is complete for the requested valid window.

## 4. Ready-only admission

The application query SHALL resolve the configured stream and read current stream state before reading candles.

```text
stream.status == ready
→ continue

stream.status != ready
→ reject without candle data
```

The range API does not run continuity audit or repair. Runtime reconciliation is the authority that permits `ready`. A stream with an unresolved prefix, internal, suffix, or realtime-recovery gap cannot be `ready` and therefore cannot produce a successful candle response.

A persisted status is consumed through the existing runtime/state repository contract. The API SHALL not create an independent readiness model.

## 5. Requested-range validation

Validation occurs before forming a successful response:

```text
resolve configured stream
→ require status == ready
→ validate from_ms < to_ms
→ require both boundaries aligned to timeframe
→ determine proven available window
→ require requested window fully inside available window
→ read SQLite range
→ enforce result invariants
→ return 200
```

The proven available window is:

```text
[earliest_available_open_time_ms, available_end_ms)
```

where `available_end_ms` is the exclusive boundary immediately after the latest canonical closed candle proven available for the ready stream.

The request SHALL satisfy:

```text
from_ms >= earliest_available_open_time_ms
and
to_ms <= available_end_ms
and
from_ms < to_ms
```

The service does not clamp requests and does not return partial success. A range before the first available candle or after the proven available end is rejected with explicit available boundaries.

## 6. Result invariant

Ready status is the normal proof of continuity; the query SHALL not execute a fresh deep audit per request. The SQLite result is nevertheless checked as a defensive invariant.

For an aligned range inside a ready stream's available window, the result SHALL contain exactly the expected candle-open grid for that timeframe. Missing, duplicate, off-grid, or out-of-window rows indicate a broken ready guarantee.

The service SHALL not return `200` with such a result. It SHALL return an internal invariant error and expose enough diagnostics to investigate. Whether the read path also transitions the stream to `degraded` is deferred to implementation design only if an existing safe state-transition service can be reused without coupling the query to runtime orchestration; logging and refusal to return partial data are mandatory.

## 7. Error contract

All errors use one stable object shape:

```json
{
  "error": "stream_not_ready",
  "message": "Configured stream is not ready for consumer reads.",
  "ticker": "BTCUSDT.P",
  "timeframe": "5m",
  "details": {}
}
```

Required mappings:

| HTTP | Error code | Meaning |
|---:|---|---|
| `400` | `invalid_request` | Missing, malformed, or unsupported query parameter syntax |
| `404` | `configured_stream_not_found` | Canonical ticker/timeframe pair is not configured |
| `409` | `stream_not_ready` | Configured stream exists but current status is not `ready` |
| `422` | `range_not_aligned` | One or both boundaries are not aligned to timeframe |
| `422` | `range_out_of_bounds` | Aligned range is outside proven available boundaries |
| `500` | `continuity_invariant_broken` | Ready-stream data violates expected grid/order/uniqueness |
| `503` | `service_unavailable` | Required storage/runtime dependency cannot serve the request |

`range_out_of_bounds` details SHALL include requested and available boundaries. `stream_not_ready` details SHALL include the current stream status without returning candles.

## 8. SQLite read path

The implementation SHALL reuse or minimally extend the existing canonical range-read capability. It SHALL not introduce an independent SQL query with divergent ordering or boundary semantics when an existing repository method can satisfy the port.

The read port SHALL express:

```text
stream identity
start inclusive
end exclusive
ascending result
```

The SQLite adapter may return domain candles or application read records according to existing repository conventions. Decimal text must remain exact through persistence-to-HTTP serialization.

## 9. BBB reference-consumer integration requirements

BBB changes are required for adoption but are outside the implementation scope of this repository change.

A subsequent BBB OpenSpec SHALL require:

1. Keep `research_api` as the single Workbench BFF.
2. Keep Workbench's existing `/api/market/candles-window` request and response contract.
3. Add a persistent pooled `MarketDataServiceClient` inside BBB.
4. Replace the legacy direct SQLite `range_get` implementation behind the current market-reader boundary.
5. Send canonical `ticker` values such as `BTCUSDT.P`.
6. Update only market-data-facing ticker validation and naming; avoid blind global `symbol` replacement where `symbol` remains a valid broader trading-domain term.
7. Parse API OHLCV decimal text into Python `Decimal` at the HTTP transport boundary.
8. Preserve Decimal through the BBB candle DTO where practical.
9. Convert Decimal to the existing NumPy/pandas `float64` representation only at explicit research boundaries that require vectorized numeric arrays.
10. Convert Decimal to JSON numbers only at the existing Workbench `ChartBar` presentation boundary.
11. Preserve chart timestamps in seconds and Workbench coverage timestamps in milliseconds.
12. Preserve BBB ownership of coverage, EMA, overlays, reports, traces, diagnostics, and component events.
13. Map `stream_not_ready`, `range_out_of_bounds`, invariant, and transport failures into explicit BBB application errors.
14. Add parity tests comparing the legacy SQLite reader and the new HTTP reader over the same canonical fixture.
15. Do not connect Workbench directly to `market_data_service`.

## 10. Performance policy

Version 1 deliberately sends one JSON response for the complete requested range. A one-second increase for a large backtest history load is currently acceptable.

The implementation SHALL record benchmark measurements for representative ranges, including:

- a normal Workbench window;
- approximately 10,000 candles;
- approximately 100,000 candles;
- the largest practical multi-year `5m` range available in test fixtures or a generated equivalent;
- SQLite read time;
- JSON serialization time;
- full local HTTP round trip.

These measurements are informative and do not impose a hard v1 latency gate unless memory usage or response construction is operationally unsafe.

Pagination, chunking, compact row formats, response streaming, compression, Arrow/Parquet, and BBB caching are documented future options only. They are not unfinished tasks or blockers for this change.

## 11. HTTP lifecycle

The candle router SHALL be registered in the existing runtime HTTP application alongside `/health` and `/readiness`. Adding the endpoint SHALL not change those endpoint contracts or runtime startup ownership.

The HTTP server must continue to serve health diagnostics even when candle reads are rejected because streams are not ready.

## 12. Documentation and patch discipline

The cumulative patch SHALL include:

- this OpenSpec;
- all implementation and test changes when the OpenSpec is executed;
- API/OpenAPI documentation;
- benchmark results;
- README examples;
- `docs/integrations/bbb-consumer-api-current-state.docx`;
- every other new or modified file from the cumulative runtime baseline.
