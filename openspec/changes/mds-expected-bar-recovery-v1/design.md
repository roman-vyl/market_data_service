# Design

## Context (verified at 37c7fd5)
- `supervisor.py` `detect_stale`: for streams with `subscription_active`, not `recovery_pending`, no fatal error, anchor = `last_confirmed_observed_at_ms or last_transport_activity_ms`; stale when `now - anchor > step * intervals + grace_ms` (defaults 2 and 5 000 ms). It emits `RecoveryRequired(reason=STALE, suspected_start=last_successful_open_time_ms + step)`.
- `runtime/realtime.py` `_stale_worker` calls `detect_stale` every `realtime_stale_check_seconds` (1.0 s) and enqueues signals into the recovery worker, which keeps one pending recovery per stream and retries incomplete/recoverable results with backoff.
- `supervisor_state.py` `record_recovery_result` on `restored`: clears `recovery_pending`, sets `recovery_completed_at_ms` and raises `last_successful_open_time_ms` to `restored_through_open_time_ms`. It never touches `last_confirmed_observed_at_ms`, so the stale age does not reset.
- `recovery.py` `_execute_sync`: window = durable latest + step .. `align_to_grid(now)`; `restored_through = window.end - step`; `incomplete` with `recovery_target_advanced` when a new bar closed during recovery.
- `last_successful_open_time_ms` is advanced by WebSocket outcomes (committed, duplicate, corrected) and by restored recoveries, and seeded from the durable latest bar at admission.
- Supported grids: 5m, 1h, 4h, 1d (`domain/timeframes.py`, `align_to_grid`).

## Decision

### D1. Expected bar
For a stream with step `S` and grace `G` at time `now`:

`expected_open(now) = align_to_grid(now - G, S) - S`

That is the open time of the latest bar whose close (`open + S`) plus `G` has passed.

### D2. Stale rule (replaces the anchor/intervals rule)
A stream is stale, and `detect_stale` emits one `RecoveryRequired(STALE)`, only when all hold:
- `subscription_active`, not `recovery_pending`, no fatal error;
- `last_successful_open_time_ms` is not None and `< expected_open(now)`;
- `expected_open(now) > last_checked_expected_open_ms`.

`last_checked_expected_open_ms` is a new per-stream fact. The supervisor records `expected_open` on the facts when it emits the signal and commits it as checked when that recovery is reported `restored`. A failed or incomplete recovery does not commit it; the stream stays `recovery_pending` under the worker's existing backoff, so nothing new is emitted meanwhile.

The suspected start hint stays `last_successful_open_time_ms + S`.

### D3. Why this cannot loop
After a `restored` recovery the stream is checked for that expected bar and cannot be signalled again until `expected_open` advances, which takes one full step. With a silent WebSocket MDS runs at most one stale recovery per stream per bar, a short time after each close. The two conditions are redundant on purpose: `last_successful` normally reaches the expected bar after `restored`, and the checked marker is the hard guarantee if it does not (for example an exchange hole REST cannot fill).

### D4. Unchanged by design
`last_confirmed_observed_at_ms` and `recovery_completed_at_ms` keep their only remaining role: `realtime_live` requires a confirmed WebSocket candle after the recovery. Heartbeat and transport activity no longer decide staleness. Recovery planning, `max_windows`, the worker queue and backoff are untouched.

### D5. Settings
`MDS_STALE_GRACE_MS` (existing) becomes the grace `G`, default 15000. `MDS_STALE_INTERVALS` is removed together with `StalePolicy.intervals` and its README line. `realtime_stale_check_seconds` stays.

## Worked example, 5m
| Time | Old main | New |
|---|---|---|
| 12:05:00 | bar 12:00 closes | bar 12:00 closes |
| 12:05:01-12:05:05 | WebSocket delivers it: nothing to do | same, `last_successful` = 12:00, no check |
| 12:05:15 (WebSocket silent since the 12:00 candle) | not stale yet | expected 12:00 missing: one recovery |
| 12:05:16 | not stale yet | restored, marked checked for 12:00, silent until 12:10:15 |
| 12:10:05 | first stale recovery (10 min 5 s after the last candle), then a possible loop | still quiet |
| 12:10:15 (still silent) | possible loop continues | expected 12:05 missing: one recovery |

2026-10-20 wake at 06:55:24: expected = bar 06:50, `last_successful` = 06:35, one recovery restores through the latest closed bar, then silence until 07:00:15. The WebSocket bar 07:00 at 07:00:01 makes that check unnecessary.

## Alternatives rejected
- Lower `intervals`/`grace` only: the missing re-arm stays, the cycle gets faster.
- Re-arm from `recovery_completed_at_ms` (branch `claude/mds-stale-anchor-after-recovery-v1`): stops the loop but still waits two intervals. Kept as an archived working branch, not to be merged; its test that reproduces 895 recoveries in 25 minutes is ported.

## Risks
- REST load with a silent WebSocket: one recovery per stream per bar. 40 streams are mostly 5m: about 40 recoveries per 5 minutes. Recoveries are serialised by the operation gate, so the last stream of a burst can finish tens of seconds after the first.
- A WebSocket candle later than `G` (up to 77 s seen on 2026-10-19 at network stalls) triggers one extra REST recovery; the later WebSocket candle is a `duplicate`. Harmless; raise `MDS_STALE_GRACE_MS` if it is noisy.
- If REST genuinely lacks the expected bar, recovery ends `incomplete` and the existing no-progress backoff retries; unchanged behaviour.
- Until the Runtime-notification change lands, faster recovery does not reach Runtime.
