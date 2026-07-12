# Design: WebSocket Realtime Recovery v1

## Adapter boundary

The WebSocket adapter owns connection, subscription messages, transport parsing, heartbeat, and reconnect signals. It emits transport-neutral confirmed candle observations and connection lifecycle events. It never writes storage or changes stream state directly.

## Canonical close ingestion

Only confirmed closed candles are passed to `IngestObservedCandle`. Partial/in-progress updates may be observed for transport health but are not canonical candles and are not persisted as closed history.

## Connection lifecycle

Configured streams transition through the existing connecting/ready/degraded lifecycle under a top-level realtime coordinator. Readiness is restored only after subscription is active, freshness is acceptable, and any reconnect gap has passed REST audit/repair.

## Reconnect recovery

After disconnect or staleness:

1. mark affected streams not ready;
2. reconnect with bounded backoff;
3. determine the bounded interval since durable latest committed candle;
4. run existing audit and repair through REST;
5. re-establish subscription freshness;
6. project readiness.

The WebSocket observation and REST repair paths converge at canonical ingestion.
