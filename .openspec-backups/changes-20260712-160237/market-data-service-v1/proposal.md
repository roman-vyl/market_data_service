# Proposal: Market Data Service v1

## Why

BBB research currently consumes historical market data through an internal Data Engine. A separate live execution service, Abi Executor, exists, but there is no independent backend responsible for continuously maintaining canonical current candles for future live strategy consumers.

Extending the historical BBB Data Engine in place would mix research migration, realtime ingestion, service lifecycle, and storage changes. The safer path is a standalone market-data service in a neighboring repository while leaving the current BBB historical pipeline unchanged.

## What changes

Create a standalone Python service that:

- obtains Bybit linear closed candles for multiple configured instruments through REST and WebSocket;
- normalizes and validates all observations through one canonical ingestion path;
- stores canonical candles in its own SQLite database with full instrument and stream identity;
- atomically stores durable stream state and quarantine diagnostics;
- repairs startup and reconnect gaps through REST;
- exposes health, readiness, and candle reads;
- runs in its own Docker container.

## What does not change

- BBB research remains on its existing historical data path.
- The existing BBB Data Engine is not modified.
- Strategy features and signals are not part of this service.
- Abi Executor is not called by this service.
- No downstream live strategy runtime is implemented in this change.

The initial configuration example includes BTCUSDT and ETHUSDT linear perpetual contracts with full available `1m` history. Adding supported instruments is configuration-driven.

## Primary architectural invariant

A validated candle mutation and the corresponding stream-state advancement are durably committed in the same SQLite transaction.

## First implementation slice

The first production slice processes one valid confirmed candle through a transport-neutral ingestion use case and atomically persists:

- the canonical candle;
- updated stream ingestion state.

The slice must prove idempotent duplicate handling before WebSocket work begins.
