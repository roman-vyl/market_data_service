# ADR-008: Readiness-first consumer contract

**Status:** Accepted

## Context

Consumers must survive service restarts, catch-up, and repaired gaps without requiring exactly-once event delivery.

## Decision

A consumer may process a stream only while it is `ready`. Each consumer owns its own `last_processed_open_time_ms`. After readiness returns, it reads all candles after that cursor before continuing.

## Consequences

- No event log or replay broker is required in v1.
- Catch-up and repair do not require per-candle external events.
- Canonical candle history is the source of truth.

## Rejected alternatives

- Server-owned consumer cursors.
- Exactly-once event delivery.
- Mandatory correction or replay events.
