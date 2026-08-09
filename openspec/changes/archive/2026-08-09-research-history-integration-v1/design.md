# Design: Research history integration v1

## 1. Ownership

MDS remains the owner of canonical persisted candle facts. Research Service chooses a requested backtest interval, while MDS reports committed bounds and verifies continuity for an explicit half-open interval.

Lifecycle state is returned as informational metadata. It does not suppress bounds or continuity responses.

## 2. Committed bounds

```http
GET /v1/streams/{ticker}/{timeframe}/bounds
```

Response:

```json
{
  "contract_version": "market_stream_bounds.v1",
  "ticker": "BTCUSDT.P",
  "timeframe": "1m",
  "state": "degraded",
  "earliest_committed_open_time_ms": 0,
  "latest_committed_open_time_ms": 120000
}
```

Bounds are calculated with `MIN(open_time_ms)` and `MAX(open_time_ms)` from canonical committed candles. They do not reuse source-discovery metadata as a substitute for actual storage contents. Empty registered streams return both boundaries as `null`.

The query performs no lifecycle mutation, audit, repair, or upstream fetch.

## 3. Explicit continuity audit

```http
POST /v1/streams/{ticker}/{timeframe}/continuity-audits
```

```json
{
  "start_time_ms": 0,
  "end_time_ms": 180000
}
```

The endpoint delegates to the existing side-effect-free `AuditStreamContinuity` use case. It returns the exact checked half-open interval, candle count, continuity result, gaps, and informational current lifecycle state.

The endpoint must not call `RepairStreamGaps`, backfill, or Bybit REST.

## 4. Canonical market data hash

`GET /v1/candles` returns `market_data_hash` for the exact ordered response identity:

```json
{
  "ticker": "BTCUSDT.P",
  "timeframe": "1m",
  "from_ms": 0,
  "to_ms": 120000,
  "candles": []
}
```

The hash input excludes the hash field itself. Serialization uses UTF-8 JSON with sorted keys, compact separators and canonical decimal text already owned by MDS. SHA-256 is returned as 64 lowercase hexadecimal characters.

Research Service and Strategy Engine must consume and propagate this MDS-owned value rather than independently defining another candle-set identity.

## 5. Existing readiness contract

This change deliberately does not alter the existing readiness-gated `/v1/candles` admission rule. Bounds and continuity audit can report a historically usable range while a stream is `degraded`, but a later three-service integration test may expose the need for a separate historical candle-read admission contract.

That decision must be made explicitly rather than silently weakening the existing consumer-read readiness contract.

## 6. Layering

- application use cases own storage-bound and audit orchestration;
- the SQLite adapter owns `MIN`/`MAX` queries;
- HTTP handlers parse and serialize only;
- runtime composition wires focused handlers;
- no application module imports SQLite or HTTP implementations.
