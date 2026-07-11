# ADR-005: One canonical ingestion path

**Status:** Accepted

## Context

REST bootstrap, REST repair, and WebSocket realtime all produce candle observations. Separate writers would create inconsistent validation and duplicate behavior.

## Decision

All candle sources pass through one transport-neutral ingestion path:

```text
normalize -> parse Decimal -> validate -> classify -> persist
```

Adapters do not write canonical candles directly.

## Consequences

- Identical duplicate and correction semantics.
- One validation implementation.
- Reconnect repair and realtime use the same domain rules.

## Rejected alternatives

- Separate REST and WebSocket writers.
- Validation inside SQLite repositories.
