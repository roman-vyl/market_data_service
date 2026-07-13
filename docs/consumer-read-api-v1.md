# Consumer Read API v1

## Endpoint

```http
GET /v1/candles?ticker=BTCUSDT.P&timeframe=5m&from_ms=1710000000000&to_ms=1710000300000
```

The window is aligned and half-open: `[from_ms, to_ms)`. Version 1 returns the complete range in one JSON response and has no pagination, cursor, limit, or response chunking.

A successful response is available only when the configured stream lifecycle state is `ready`, the requested range lies fully inside the proven available window, and the stored rows form the complete timeframe grid.

```json
{
  "ticker": "BTCUSDT.P",
  "timeframe": "5m",
  "from_ms": 1710000000000,
  "to_ms": 1710000300000,
  "candles": [
    {
      "open_time_ms": 1710000000000,
      "open": "68450.1",
      "high": "68520",
      "low": "68390.4",
      "close": "68480.7",
      "volume": "123.456"
    }
  ]
}
```

OHLCV values are normalized decimal text and are never converted to JSON floating-point numbers by this service.

## Errors

- `404 configured_stream_not_found`: ticker/timeframe is not a configured canonical stream.
- `409 stream_not_ready`: the stream is not in durable `ready` state.
- `422 invalid_range`: missing, duplicate, unsupported, non-integer, zero, or reversed parameters.
- `422 range_not_aligned`: boundaries are not on the timeframe grid.
- `422 range_out_of_bounds`: the request is not fully inside the proven available window.
- `500 continuity_invariant_broken`: a stream marked ready did not return the complete expected grid; no partial success is returned.

The API never starts audit or repair. Historical continuity and ready-state ownership remain in the runtime reconciliation pipeline.

## BBB integration contract

BBB remains the Workbench BFF. A later BBB change replaces its direct SQLite market reader with a persistent HTTP client to this endpoint, migrates market-data-facing identity to `.P` tickers, parses decimal text to `Decimal`, and preserves existing Workbench endpoints and numeric `ChartBar` output.

## Future transport options

Pagination, request chunking, streaming JSON, compact row encoding, compression, and BBB caching are intentionally deferred until real performance evidence requires them.

## Sandbox benchmark evidence

Synthetic standard-library serialization on the implementation sandbox:

| Candles | Serialization time | JSON size |
|---:|---:|---:|
| 1,000 | 0.029 s | 0.11 MiB |
| 10,000 | 0.259 s | 1.07 MiB |
| 100,000 | 2.745 s | 10.84 MiB |

These figures are evidence, not a latency SLA. Real Docker/BBB measurements remain the basis for any later pagination or streaming change.
