# Proposal: Consumer Read API v1

## Why

`market_data_service` now owns canonical closed candles, historical continuity, and per-stream readiness, but independent consumers cannot yet read candle ranges through a stable service boundary. BBB still reads its legacy market SQLite directly through its internal market reader. That couples BBB to another component's storage layout and prevents the new service from becoming the sole owner of canonical market data.

The first consumer API should make the smallest architectural change necessary:

```text
Workbench frontend
→ BBB research_api / BFF
→ BBB market reader
→ market_data_service HTTP API
→ canonical SQLite
```

The existing Workbench HTTP contract remains owned by BBB. This change adds only a canonical backend-to-backend range-read API.

## Architectural gate before implementation

Before production code is changed, the implementation SHALL complete and record a file-by-file placement audit of the existing HTTP application, storage ports, SQLite repositories, stream-state access, runtime wiring, and tests. The audit SHALL confirm that the API can be added without moving SQL, readiness policy, JSON serialization, or BBB-specific behavior into existing mixed-responsibility modules.

The approved implementation shape SHALL keep the current repository conventions while introducing narrow modules for consumer reads:

```text
application/consumer_read/
ports/consumer_read.py
adapters/sqlite/consumer_candle_reader.py
adapters/http/consumer_read/
runtime/wiring.py
```

`adapters/http/runtime_server.py` may register the route and dependencies, but SHALL NOT absorb request validation, response schemas, exception mapping, serialization, or SQL. Existing broad ports such as `ports/storage.py` SHALL NOT be expanded with unrelated consumer-transport concerns when a focused read port is clearer.

Implementation SHALL stop at the architectural gate if the proposed placement creates cyclic dependencies, duplicates existing canonical range semantics, or requires a single file to own more than one layer responsibility. The OpenSpec design SHALL be updated before proceeding in that case.

## What changes

The service SHALL expose one versioned read-only candle range endpoint:

```text
GET /v1/candles
```

The endpoint accepts:

```text
ticker
timeframe
from_ms
to_ms
```

where the requested window is aligned and half-open:

```text
[from_ms, to_ms)
```

Version 1 deliberately returns the complete requested range in one JSON response. It does not introduce pagination, cursoring, response chunking, streaming JSON, Arrow, or Parquet.

The endpoint SHALL:

- accept only configured canonical tickers such as `BTCUSDT.P`;
- accept the existing textual timeframe values such as `5m`, `1h`, `4h`, and `1d`;
- serve only streams whose current lifecycle status is `ready`;
- reject ranges outside the stream's proven available window;
- return canonical closed candles ordered by `open_time_ms` ascending;
- serialize OHLCV as normalized decimal text;
- never return a partial or silently gapped successful response.

## BBB integration intent

BBB is the reference consumer, but BBB repository changes are not implemented by this change. The integration contract SHALL be explicit:

- BBB `research_api` remains the Workbench BFF;
- Workbench continues using its existing `/api/market/candles-window` endpoint;
- BBB replaces direct legacy SQLite reads with an HTTP implementation of its market-reader boundary;
- BBB migrates market-data-facing instrument identity to canonical tickers such as `BTCUSDT.P`;
- BBB parses OHLCV decimal text into `Decimal` at the transport boundary;
- BBB may convert to `float64` only at explicit existing research or presentation boundaries where required;
- Workbench `ChartBar` remains numeric JSON and chart time remains seconds;
- BBB preserves its current coverage, EMA, overlays, reports, traces, and component-event responsibilities.

Any required BBB code changes SHALL be delivered through a separate approved BBB integration change.

## Intended outcome

After this change, `market_data_service` provides a stable canonical range-read contract without exposing SQLite. A subsequent BBB change can switch the internal market reader from direct database access to HTTP while leaving the Workbench frontend and BFF endpoints unchanged.

## Scope

This change includes:

- an explicit pre-implementation architecture and file-responsibility gate;
- one unpaginated candle range HTTP endpoint;
- application query and read-port boundaries;
- reuse or extension of the existing SQLite range-read path;
- canonical ticker and timeframe validation;
- ready-only stream admission;
- aligned available-range validation;
- Decimal-text JSON serialization;
- deterministic error contracts;
- OpenAPI/schema documentation;
- performance measurement for representative ranges;
- BBB integration requirements and parity scenarios.

## Non-goals

- pagination, cursoring, or request chunking;
- streaming responses, NDJSON, Arrow, or Parquet;
- a latest-candle endpoint in this change;
- direct Workbench access to `market_data_service`;
- migration of BBB code in this repository;
- indicators, EMA, reports, traces, or Workbench coverage calculation;
- shared-volume or direct SQLite access by consumers;
- public-internet authentication and complex rate limiting;
- consumer WebSocket or SSE delivery;
- changing candle persistence, continuity audit, repair, or realtime ingestion semantics.
