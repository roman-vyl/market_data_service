# Specification: WebSocket Realtime Recovery v1

## Requirement: One realtime ingestion path

Confirmed WebSocket candle closes SHALL be normalized to transport-neutral observations and SHALL enter the same canonical ingestion use case used by REST imports.

The WebSocket adapter SHALL NOT write SQLite or advance stream state directly.

## Requirement: Confirmed closes only

Only exchange-confirmed closed candles SHALL become canonical closed candles. Partial or unconfirmed updates SHALL NOT be persisted as closed history.

## Requirement: Multi-stream subscription isolation

Subscriptions and freshness SHALL be tracked per canonical stream. A failure or stale condition for one stream SHALL NOT mutate another stream's candle or progress state.

## Requirement: Bounded reconnect

Reconnect SHALL use bounded cancellable backoff. Disconnect or staleness SHALL make affected streams not ready.

## Requirement: REST-authoritative reconnect recovery

Before readiness is restored after a disconnect or stale interval, the service SHALL use existing bounded continuity audit and REST gap repair to recover missed closed candles.

WebSocket observations SHALL NOT silently overwrite conflicting REST-authoritative candles.

## Requirement: No realtime event log

Realtime support SHALL NOT require a persisted market-event log, replay broker, or server-owned consumer cursor in v1.
