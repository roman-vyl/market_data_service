# Specification: Consumer Read API v1

## Requirement: Canonical candle range endpoint

The service SHALL expose a versioned candle range endpoint addressed by canonical ticker and timeframe. The requested range SHALL use half-open `[start_ms, end_ms)` semantics.

Returned candles SHALL be ordered by `open_time_ms` and SHALL contain canonical normalized decimal strings.

## Requirement: Deterministic pagination

Large range reads SHALL use deterministic pagination. A continuation SHALL resume strictly after the last returned canonical candle and SHALL NOT duplicate or skip rows in an unchanged dataset.

## Requirement: Latest candle endpoint

The service SHALL expose the latest canonical closed candle for a configured stream. Unknown streams and empty streams SHALL produce explicit typed API outcomes.

## Requirement: Readiness-first consumption

The public contract SHALL expose aggregate and per-stream readiness. Consumers SHALL pause market-dependent decisions while a required stream is not ready.

A consumer SHALL own its per-stream processing cursor and SHALL catch up through ordered range reads after readiness is restored. The service SHALL NOT persist consumer cursors.

## Requirement: No consumer event log

The v1 consumer API SHALL NOT require a candle event log, replay broker, push delivery guarantee, or server-owned offset.

## Requirement: BBB integration boundary

BBB integration SHALL use the published API contract and SHALL NOT read the market-data SQLite database directly. Strategy, feature, signal, order, and position semantics remain outside this service.
