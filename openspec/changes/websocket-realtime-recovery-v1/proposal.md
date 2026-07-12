# Proposal: WebSocket Realtime Ingestion and Recovery v1

## Why

The service can build, audit, and repair canonical history through REST, but it cannot yet maintain current closed candles continuously.

Realtime support must remain simple on the normal path:

```text
connect
→ subscribe
→ receive confirmed candle close
→ canonical ingestion
→ report outcome
```

Transport handling, candle ingestion, connection supervision, and historical recovery are different responsibilities. Combining them in one runtime object would make WebSocket callbacks responsible for SQLite, lifecycle, gap detection, reconnect, and REST repair. This change defines explicit boundaries before implementation.

## What changes

Add a realtime subsystem composed of five focused roles:

1. **Bybit WebSocket Adapter** — exchange protocol and transport only.
2. **Realtime Connector** — connection/subscription lifecycle and normalized event delivery.
3. **Realtime Candle Handler** — confirmed-close validation and canonical ingestion.
4. **Realtime Supervisor** — per-stream live/stale/connection observation and recovery signals.
5. **Realtime Recovery Coordinator** — bounded REST catch-up, continuity audit, gap repair, and post-recovery proof.

The subsystem SHALL support every enabled configured `ticker × canonical_timeframe` stream, not only `1m`.

## Intended outcome

For a healthy stream:

```text
Bybit confirmed close
→ normalized realtime observation
→ IngestObservedCandle
→ committed | duplicate | corrected | rejected | failed
→ outcome reported to supervisor
```

After disconnect, staleness, or a detected sequence gap:

```text
stream becomes not ready
→ connector restores transport/subscription
→ recovery coordinator performs bounded REST reconciliation
→ post-recovery continuity is proven
→ fresh realtime activity is observed
→ stream may become ready
```

## What does not change

- no direct WebSocket writes to SQLite;
- no gap detection or REST repair inside the adapter or connector;
- no audit or repair on every normal candle;
- no persisted realtime event log;
- no replay broker or server-owned consumer cursor;
- no strategy, signal, order, or position logic;
- no HTTP health/readiness server or process startup orchestration in this change;
- no generic background scheduler or parallel REST worker pool.

## Architectural constraint

This change explicitly rejects a single `WebSocketManager` or equivalent God object. Exchange
transport/protocol, connection lifecycle, confirmed-candle ingestion, per-stream supervision,
and historical recovery are separate responsibilities with one-way dependencies and architecture
tests. Runtime connection facts remain in memory; canonical storage remains owned by the
existing ingestion and persistence layers.
