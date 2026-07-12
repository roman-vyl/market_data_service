# Design: Consumer Read API v1

## Read boundary

The API reads canonical storage through application/query ports. HTTP handlers perform validation, wiring, serialization, and status mapping only. They do not contain SQL or readiness rules.

## Range endpoint

The range endpoint uses canonical ticker/timeframe identity and half-open `[start_ms, end_ms)` semantics. Results are ordered by `open_time_ms` and paginated deterministically by an opaque or explicit continuation derived from the last returned open time.

## Latest endpoint

The latest endpoint returns the newest canonical closed candle for one stream or a clear not-found/not-ready response according to the endpoint contract.

## Exact serialization

OHLCV values are returned as normalized decimal strings. Timestamps remain integer milliseconds.

## Consumer recovery

A consumer owns `last_processed_open_time_ms` per stream. While readiness is false, it pauses decisions. When readiness becomes true, it requests ordered candles after its cursor and advances its cursor only after its own durable processing.

## BBB boundary

BBB adopts this HTTP contract through a separate consumer-side change. This service remains unaware of strategies and does not persist BBB cursors.
