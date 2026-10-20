## 1. Supervision
- [ ] 1.1 `StalePolicy`: drop `intervals`, keep `grace_ms` (default 15000); settings `MDS_STALE_GRACE_MS` default 15000, remove `MDS_STALE_INTERVALS` (settings, `service.py` wiring, README)
- [ ] 1.2 Facts: add `last_checked_expected_open_ms` and `pending_expected_open_ms` (stored at signal time, kept through retries and `incomplete` windows, cleared on `restored`)
- [ ] 1.3 `detect_stale`: expected-bar rule (D1, D2); on `restored` commit the stored pending bar (never the bar expected at completion); `incomplete`/failure do not
- [ ] 1.4 Keep `realtime_live` semantics; heartbeat/transport activity no longer used for staleness

## 1a. Retry schedule for STALE (D6)
- [ ] 1a.1 Recovery worker: the schedule is chosen inside the existing worker by `signal.reason`; STALE uses one failure counter (60 s first, 360 s later), reset on `restored` and on real progress; other reasons run the current code path unchanged; no second worker
- [ ] 1a.2 Settings `MDS_STALE_RECOVERY_FIRST_RETRY_SECONDS` (60) and `MDS_STALE_RECOVERY_NEXT_RETRY_SECONDS` (360), validation, README; `MDS_REALTIME_RECOVERY_BASE/MAX_SECONDS` unchanged

## 2. Tests (deterministic fake clock)
- [ ] 2.1 WebSocket candle within grace: no signal, and at runtime level the stale worker enqueues nothing into the recovery queue for that stream
- [ ] 2.2 Silent WebSocket, 1 s ticks over 25 min on 5m: one signal per missed bar (regression of the 895 repeats)
- [ ] 2.3 Restored recovery with `attempted=0 committed=0` is not repeated for the same bar
- [ ] 2.4 `incomplete` and failed recovery do not mark checked; backoff only; retry keeps the original pending bar
- [ ] 2.4a Bar boundary crossed during recovery: E1 marked, E2 judged independently (covered: no signal; missing: one signal)
- [ ] 2.4b Long idle (many bars): one signal, one marked bar
- [ ] 2.5 Late WebSocket candle after REST recovery: duplicate, no extra signal
- [ ] 2.6 5m, 1h, 4h, 1d each on their own grid; stream without anchor: no signal
- [ ] 2.7 `realtime_live` and `ready` unchanged by stale recovery; runtime-level stale worker test updated
- [ ] 2.8 Update the existing supervisor test that used the old rule

- [ ] 2.9 STALE schedule: 60 s then 360 s repeating; new expected bar creates no second recovery and does not reset the delay; `restored` (also `attempted=0`) and real progress reset; recoverable failure and no-progress incomplete count; fatal not retried
- [ ] 2.9a Same window and marker without advance, even after a successful REST response, always falls under the schedule
- [ ] 2.9b Reconnect and sequence-gap recoveries keep 1 s doubling to 60 s with unchanged settings and defaults (existing worker tests stay valid unchanged)

## 3. Verification
- [ ] 3.1 `make verify` green; `openspec validate --all --strict` green
