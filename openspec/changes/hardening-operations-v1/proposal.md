# Proposal: Hardening and Operations v1

## Why

After runtime, realtime, and consumer surfaces exist, the service needs explicit resilience evidence, observability, maintenance policy, and an operator runbook before production use.

## What changes

- add malformed-payload, timeframe-boundary, clock-skew, and database-fault coverage;
- add long-running restart/reconnect smoke;
- add bounded operational metrics;
- define SQLite maintenance/backup policy;
- publish an operational runbook.

## What does not change

This change does not add new market-data capabilities or alter canonical semantics.
