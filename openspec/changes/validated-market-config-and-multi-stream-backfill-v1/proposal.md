# Proposal: Validated Market Configuration and Multi-Stream Backfill v1

## Why

The historical core is implemented for one stream, but operator-safe multi-instrument operation is incomplete. Configuration loading is only partial, source-failure classification is broad, BTC/ETH metadata assumptions are not both proven, and no deterministic bounded `backfill --all` entrypoint exists.

## What changes

Complete the validated configuration and deterministic multi-stream backfill slice:

- validate the versioned market configuration fully;
- reject duplicate instrument, exchange-symbol, and stream identities;
- verify configured BTCUSDT and ETHUSDT linear perpetual metadata against Bybit;
- define one shared source-failure classification contract;
- add deterministic sequential bounded `backfill --all` orchestration by composing the existing single-stream full bootstrap.

## What does not change

- no WebSocket;
- no daemon startup coordinator;
- no parallel REST scheduler;
- no new persistence tables;
- no new candle ingestion path;
- no readiness or consumer API implementation.

## Outcome

An operator can validate configuration and advance every enabled stream by a finite explicit per-stream budget, with isolated results and durable resume.
