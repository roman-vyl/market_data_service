# Sequential bounded backfill

## Decision

Version 1 does not implement a parallel REST scheduler.

Historical REST work is sequential by default. One bounded response window is fetched, validated, committed in one storage transaction, and completed before the next request begins.

## Why

The initial full minute history is large regardless of whether two requests run in parallel. The first implementation prioritizes:

- deterministic progress;
- simple rate-limit behavior;
- readable logs;
- one SQLite write transaction at a time;
- easy stop and resume;
- failure isolation between streams;
- small, testable modules.

Parallelism may be added later only after measurements show that sequential REST work is the limiting factor.

## Administrative runs are finite

Deep history loading must not require one uncontrolled process to run until every instrument is complete.

A backfill command receives a finite budget, expressed as a maximum number of bounded REST windows.

Conceptual commands:

```text
market-data backfill --ticker BTCUSDT.P --max-windows 100
market-data backfill --all --max-windows-per-stream 20
```

A command invocation:

1. resolves the selected configured streams;
2. visits them in deterministic configuration order;
3. processes at most the requested number of windows for each selected stream;
4. commits each window before continuing;
5. exits cleanly when its budget is exhausted;
6. preserves all completed progress for the next invocation.

## One stream

A one-stream run processes only the selected ticker/timeframe.

```text
BTCUSDT.P / 1m
  -> next missing or trailing window
  -> commit
  -> repeat up to max-windows
  -> exit
```

## All streams

An all-stream run visits every enabled stream sequentially.

With ten tickers and a budget of twenty windows per stream:

```text
stream 1 -> at most 20 windows
stream 2 -> at most 20 windows
...
stream 10 -> at most 20 windows
exit
```

The next command invocation repeats the same deterministic order and resumes each stream from durable candles and state.

## Resume source of truth

Schema v1 does not add a backfill-job table.

Resume planning uses:

- configured stream identity;
- `stream_state.latest_committed_open_time_ms`;
- a continuity audit when required.

Committed candles are never discarded merely because a command stopped or failed.

`latest_committed_open_time_ms` means only the latest candle successfully
persisted through canonical ingestion. It does not prove that every earlier
grid point is present. Backfill may finish with history loaded and audit still
pending; continuity proof belongs to the later audit/gap-repair workflow.

## Failure isolation

Recoverable REST/source failures move the affected stream to `degraded` and do not erase progress from another stream. Fatal invariant, configuration, schema, or storage-corruption failures move the affected stream to `failed`.

For an `--all` administrative run, the command may continue to later streams after recording a recoverable failure. The final command result must report every stream outcome rather than hiding partial failure.

## Normal service startup

The long-running service is not responsible for performing an unlimited deep-history bootstrap in one startup.

It may perform bounded startup catch-up for streams whose required history has already been established. Streams still undergoing deep bootstrap remain `bootstrapping` and are not ready for consumers.

## REST and WebSocket separation

REST provides:

- initial history;
- startup catch-up;
- gap repair;
- reconnect repair.

WebSocket provides confirmed realtime candle closes after historical readiness.

Both paths still use the same canonical candle validation and ingestion rules.
