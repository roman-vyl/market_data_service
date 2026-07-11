# Per-Stream Persisted State Machine

## Decision

Every enabled `StreamKey` owns one independent persisted lifecycle snapshot. BTC and ETH never share mutable lifecycle, audit, bootstrap, repair, WebSocket, or readiness state.

Persisted state explains and accelerates recovery, but canonical candles remain the factual source of truth. A previously persisted `ready` value is never trusted blindly after process restart.

## States

```text
uninitialized
bootstrapping
auditing
repairing
connecting
ready
degraded
failed
```

### `uninitialized`

The stream is registered but historical initialization has not started.

### `bootstrapping`

The service is loading the full available canonical `1m` history. Restart resumes from the actual latest committed candle and later performs a complete continuity audit.

### `auditing`

The service is proving continuity from the persisted earliest available candle through the latest fully closed candle.

### `repairing`

One or more gaps are being repaired through Bybit REST. Repair must return to `auditing`; it cannot jump directly to `ready` or `connecting`.

### `connecting`

History is continuous and the service is establishing realtime WebSocket delivery plus a short trailing REST check that closes the startup race.

### `ready`

The stream is complete, current, and acceptable for a future live consumer. This is the only ready lifecycle state.

### `degraded`

The stream is temporarily unsafe but automatic recovery is expected. Examples include WebSocket loss, stale tail, temporary REST failure, or newly detected discontinuity.

### `failed`

Automatic recovery is unsafe or impossible. Explicit recovery must first move the stream to `uninitialized` or `auditing`; `failed -> ready` is prohibited.

## Normal paths

New database:

```text
uninitialized
  -> bootstrapping
  -> auditing
  -> repairing -> auditing     when gaps exist
  -> connecting
  -> ready
```

Warm restart:

```text
persisted state
  -> reconcile against actual candles
  -> auditing
  -> repairing when required
  -> connecting
  -> ready
```

Bootstrap REST loss:

```text
bootstrapping
  -> degraded
  -> bootstrapping
```

Runtime loss:

```text
ready
  -> degraded
  -> auditing or connecting
  -> ready
```

## Persisted snapshot

`stream_state` stores only current operational facts:

- lifecycle state;
- earliest actually available candle;
- latest committed candle;
- last completed audit time;
- latest successful REST time;
- latest WebSocket message time;
- latest error code and detail;
- state change time;
- row update time.

It is not a gap journal, retry queue, bootstrap-window history, or event log.

## Restart semantics

- Crash in `bootstrapping`: resume from the actual maximum committed candle, then audit the full required range.
- Temporary REST/source failure during bootstrap: enter `degraded`; a later administrative run may return to `bootstrapping`.
- Crash in `auditing`: rerun the audit.
- Crash in `repairing`: rerun audit, derive actual remaining gaps, then repair.
- Crash in `connecting`: reconnect and run the trailing REST check again.
- Crash in `ready`: do not restore readiness immediately; prove continuity and freshness again.

## Readiness

Per-stream readiness is true only for `ready`.

Default aggregate readiness is strict:

```text
all enabled required streams are ready
```

An empty configured-stream set is not ready. BTC may remain individually ready while ETH bootstraps, but aggregate readiness remains false.

## Ownership

- `domain/stream_state.py` owns state names and legal transitions.
- `domain/readiness.py` owns pure readiness projection.
- Application use cases decide when a transition is justified.
- SQLite only persists the validated snapshot.
- No universal lifecycle manager may absorb bootstrap, audit, repair, reconnect, and storage logic.
