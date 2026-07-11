# Consumer Readiness Contract

## Purpose

Define the minimal contract between Market Data Service and future consumers without turning the service into an event broker.

## Source of truth

Canonical candle storage and ordered range reads are the source of truth. A future WebSocket or SSE notification may reduce latency, but it is never authoritative and is never required for recovery.

## Processing gate

A consumer may connect to health, readiness, and read APIs at any time. It MUST NOT make trading, feature, or strategy decisions from a stream unless that stream is `ready`.

When readiness is lost, the consumer pauses that stream. Other ready streams remain independent.

## Consumer-owned cursor

Each consumer owns and persists its own `last_processed_open_time_ms` per stream. Market Data Service schema v1 does not store consumer cursors.

On initial startup or after readiness recovery, the consumer requests candles strictly after its cursor, processes them in ascending order, advances its cursor, and resumes normal operation.

## Service restart and downtime

Candles closed while the service was offline are fetched through REST before the stream returns to `ready`. They do not require per-candle replay events.

## Gap repair

When the service detects a gap, it removes the stream from `ready`, repairs and audits canonical storage, then restores readiness. No correction/replay event is required. The consumer catches up from its own cursor.

## Non-goals for v1

Schema and API v1 do not include:

- market event log;
- exactly-once delivery;
- replay broker;
- server-owned consumer offsets;
- correction events;
- one notification per historical candle.
