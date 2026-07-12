# Specification: Hardening and Operations v1

## Requirement: Fault evidence

The service SHALL have version-controlled tests for malformed transport payloads, timeframe boundaries, clock skew, SQLite transaction failure, persistence restart, and reconnect recovery.

## Requirement: Operational metrics

The service SHALL expose bounded metrics sufficient to observe process health, per-stream readiness/state, candle freshness, source failures, repair outcomes, quarantine growth, and SQLite health.

Metrics SHALL NOT be used as authoritative lifecycle or candle state.

## Requirement: SQLite operational policy

The project SHALL document backup, integrity-check, WAL checkpoint, corruption response, and persistent-volume recovery procedures compatible with a single service-owner process.

## Requirement: Runbook

The project SHALL provide an operator runbook for startup, bounded bootstrap, degraded/failed streams, gap repair, reconnect incidents, database maintenance, backup, restore, and shutdown.
