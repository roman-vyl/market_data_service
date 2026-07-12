# Proposal: WebSocket Realtime Recovery v1

## Why

The service can build and repair canonical history through REST, but it cannot yet maintain current closed candles continuously. Realtime support must reuse the accepted ingestion and repair architecture rather than create a parallel live path.

## What changes

- add a Bybit public WebSocket adapter for configured linear candle streams;
- subscribe to multiple enabled symbols through one simple lifecycle;
- ingest only confirmed closed `1m` candles through canonical ingestion;
- ignore non-canonical partial updates;
- detect disconnect and stale streams;
- reconnect with bounded backoff;
- use REST audit/repair for missed intervals before restoring readiness.

## What does not change

- no parallel REST worker scheduler;
- no direct WebSocket writes to SQLite;
- no event log;
- no server-owned consumer offsets;
- no strategy or order logic.
