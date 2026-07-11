# Contributing

Read before changing code:

- `AGENTS.md`;
- `docs/architecture.md`;
- `docs/master-plan.md`;
- `docs/operational-scenarios.md`;
- active OpenSpec under `openspec/changes/market-data-service-v1/`.

## Architecture rules

- Keep the dependency direction `domain <- application <- ports/adapters <- entrypoints`.
- Do not import BBB or Abi Executor packages.
- Do not add strategy, indicator, signal, order, or position logic.
- REST and WebSocket adapters must not write directly to canonical storage.
- All accepted candles must pass through one application ingestion use case.
- A canonical candle mutation and its stream-state update must commit atomically.
- Do not introduce consumer-specific webhook orchestration into the core service.
- Do not copy the old BBB Data Engine package wholesale.
- Do not introduce a parallel REST scheduler for v1 backfill; use finite sequential runs.
- Inspect and port proven old algorithms with parity tests.
- Keep modules focused; do not create mixed-responsibility mega-files or generic dumping grounds.

## Data-policy rules

- `1m` is mandatory for every configured symbol.
- The default continuity obligation is full available Bybit minute history.
- `launchTime` is a search floor; the observed earliest candle is the canonical lower bound.
- Bootstrap must be resumable.
- Readiness is the consumer processing gate; consumers catch up by range read from their own cursor after startup or recovery.

## Verification

Before committing production code:

```bash
make verify
```

Architecture or observable-behavior changes must update the active OpenSpec documents.

## Multi-symbol rules

- Never hard-code BTCUSDT as the production default.
- Every candle, gap, bootstrap cursor, subscription, and readiness state must carry an explicit stream identity.
- Adding ETHUSDT or another supported pair must require configuration and tests, not duplicated orchestration code.
- Do not share mutable per-symbol state through module globals or singleton managers.
- Failure or bootstrap progress for one stream must not overwrite another stream's durable state.
