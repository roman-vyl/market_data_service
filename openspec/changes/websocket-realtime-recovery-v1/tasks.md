# Tasks: WebSocket Realtime Ingestion and Recovery v1

## Contracts and routing

- [x] Define transport-neutral connection, subscription, heartbeat, candle, and stop event contracts.
- [x] Define the realtime ingestion outcome contract (`committed`, `duplicate`, `corrected`, `rejected`, `failed`).
- [x] Build deterministic `(exchange_symbol, bybit_interval) → StreamKey` routing from validated configuration.
- [x] Add routing tests for multiple symbols and multiple configured timeframes.

## Bybit WebSocket adapter

- [x] Add the Bybit public linear WebSocket adapter.
- [x] Implement subscription/unsubscription message construction for all configured streams.
- [x] Parse heartbeat, subscription acknowledgement, disconnect/error, and kline events.
- [x] Reject malformed, unknown, duplicate, and ambiguous topic mappings.
- [x] Keep the adapter free of storage, lifecycle, audit, repair, and readiness logic.

## Realtime connector

- [x] Add the cancellable connection/subscription lifecycle.
- [x] Deliver normalized candle observations to the handler and lifecycle events through the supervisor-facing event callback.
- [x] Add bounded reconnect policy with explicit exhaustion.
- [x] Keep the connector free of REST recovery work so receive callbacks cannot block on historical reconciliation.
- [x] Add clean stop, bounded reconnect exhaustion, and resource-release tests.

## Realtime candle handler

- [x] Ignore unconfirmed candle updates for canonical persistence.
- [x] Normalize confirmed closes to the existing candle observation contract.
- [x] Route confirmed closes through existing `IngestObservedCandle`.
- [x] Report canonical ingestion outcomes without implementing audit or repair.
- [x] Add REST/WebSocket duplicate and correction parity tests.
- [x] Add storage-failure and rejected-observation outcome tests.

## Realtime supervisor

- [x] Track per-stream subscription, transport activity, confirmed-close activity, and ingestion status in memory.
- [x] Add configurable per-stream stale detection based on timeframe-aware expectations.
- [x] Add cheap sequence-discontinuity signalling without claiming a full continuity proof.
- [x] Emit recovery-required signals for disconnect, stale, sequence discontinuity, and rejected observations.
- [x] Keep independent stream state isolated across symbols and timeframes.

## Realtime recovery coordinator

- [x] Define the bounded recovery request/result contract.
- [x] Derive recovery intervals from durable latest committed state and latest fully closed boundaries.
- [x] Compose existing trailing backfill, continuity audit, and `RepairStreamGaps` workflows.
- [x] Require post-recovery audit before reporting `restored`.
- [x] Serialize recovery per stream while allowing independent streams to remain unaffected.
- [x] Classify incomplete, recoverable, and fatal recovery outcomes consistently.
- [x] Do not add a recovery-job table or a second candle ingestion path.

## Readiness facts and integration boundary

- [x] Expose per-stream realtime facts needed by future service readiness projection.
- [x] Require active subscription and successful recovery before data readiness can be true, with fresh realtime activity tracked separately as live diagnostics.
- [x] Leave process startup, HTTP `/health`, HTTP `/readiness`, and aggregate service readiness to `runtime-startup-orchestration-v1`.

## Verification and documentation

- [x] Add a fake WebSocket + fake REST + temporary SQLite integration matrix.
- [x] Cover multi-symbol and multi-timeframe confirmed/unconfirmed ingestion.
- [x] Cover disconnect, reconnect, stale detection, sequence discontinuity, bounded catch-up, gap repair, and post-audit.
- [x] Cover duplicate, correction, rejected observation, storage failure, and stream isolation.
- [x] Add a bounded real Bybit WebSocket smoke without consumer dependencies.
- [x] Update README and acceptance matrix for the completed ingestion slice; base task references remain deferred until the full change closes.

## Architecture and cohesion guards

- [x] Define the physical transport/protocol/topics/adapter module split.
- [x] Define separate connector, handler, supervisor, supervisor-state, recovery, recovery-planning, recovery-state, and contract modules.
- [x] Keep connector dependencies behind realtime ports instead of concrete Bybit adapters.
- [x] Add architecture tests for forbidden transport/storage, supervisor/history, and recovery/WebSocket dependencies.
- [x] Keep connection/subscription/stale/reconnect facts in memory without new persistence tables.
- [x] Enforce one primary responsibility per realtime module and split modules before increasing file-size limits.
