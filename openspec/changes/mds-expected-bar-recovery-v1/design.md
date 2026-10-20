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

`last_checked_expected_open_ms` and `pending_expected_open_ms` are new per-stream facts.

**Identity of the expected bar.** When `detect_stale` emits the signal it stores the bar that triggered it in `pending_expected_open_ms` (the value computed at that moment, not recomputed later). That identity stays with the stream through every retry and every `incomplete` continuation window of the same recovery. When the recovery is reported `restored`, exactly that stored bar is committed: `last_checked_expected_open_ms = max(last_checked_expected_open_ms, pending_expected_open_ms)` and the pending value is cleared. The expected bar at completion time is never used for marking. A failed or incomplete result does not commit anything and keeps the pending value; the stream stays `recovery_pending` under the worker's backoff, so nothing new is emitted meanwhile.

**Crossing a bar boundary during recovery.** If the next expected bar `E2` becomes due while the recovery for `E1` runs, `E1` is still the bar marked checked on completion. `E2` is then judged independently on the next tick: if the restored recovery already covered it (`last_successful_open_time_ms >= E2`, which is the normal case because the recovery window end is aligned to the latest closed bar at planning time and a bar closing mid-recovery makes the result `incomplete` with a next window), no signal is emitted; if it is still missing, exactly one signal is emitted for `E2`.

**After a long idle.** If MDS was down for many bars, one signal is emitted for the expected bar at that moment; the recovery window covers every missed bar through the existing bounded window logic, and only that one bar identity is marked. There is no signal per missed bar.

The suspected start hint stays `last_successful_open_time_ms + S`.

### D3. Why this cannot loop
After a `restored` recovery the stream is checked for that expected bar and cannot be signalled again until `expected_open` advances, which takes one full step. With a silent WebSocket MDS runs at most one stale recovery per stream per bar, a short time after each close. The two conditions are redundant on purpose: `last_successful` normally reaches the expected bar after `restored`, and the checked marker is the hard guarantee if it does not (for example an exchange hole REST cannot fill).

### D4. Unchanged by design
`last_confirmed_observed_at_ms` and `recovery_completed_at_ms` keep their only remaining role: `realtime_live` requires a confirmed WebSocket candle after the recovery. Heartbeat and transport activity no longer decide staleness. Recovery planning, `max_windows` and the worker queue are untouched; the retry schedule changes only as D6 says.

**Readiness is not changed by this change.** `ready` stays as defined by `runtime-health-and-readiness`: durable `ready`, active subscription, restored recovery, no pending recovery, no fatal error. It does not depend on `realtime_live` (that capability already states that `ready` may be true while `realtime_live` is false; for example `durable_state=ready realtime_status=subscribed data_ready=True realtime_live=False ready=True` in the 2026-10-20 07:00:03 logs). This change does not make a stream ready or not ready that was not before, and does not touch WebSocket readiness semantics. How a trading cycle may use a bar restored over REST is decided in the separate Runtime-notification change.

### D6. Retry limit after an unsuccessful recovery
The 15 s grace is only the time of the **first check of a new bar** in normal mode. It is not a retry interval. After an unsuccessful REST recovery the stream is rate-limited by its own schedule, independent of how often bars close:

- first retry: not earlier than 60 s after the failed attempt;
- second and every later retry: not earlier than 360 s after the previous attempt;
- a new expected bar never resets or shortens this schedule and never creates an independent recovery: while the stream is `recovery_pending` (already true today) `detect_stale` emits nothing, and the next successful attempt covers every bar missed meanwhile through its normal window;
- a `restored` recovery resets the schedule; afterwards the normal rule "bar close + grace" applies again.

What counts as unsuccessful (one shared counter per pending recovery):
- `recoverable failure` (REST error, timeout): counts;
- `incomplete` without progress (window and progress marker unchanged, so the required bar is still absent): counts;
- `incomplete` with progress (REST works, the window advanced, for example a bar closed during recovery or `max_windows` was hit): not a failure, continues immediately as today and resets the counter;
- `restored` including `attempted=0 committed=0`: success, not a failure;
- `fatal`: no retry, unchanged.

Settings: `MDS_REALTIME_RECOVERY_BASE_SECONDS` becomes the first retry delay (default 60, was 1) and `MDS_REALTIME_RECOVERY_MAX_SECONDS` the delay of every later retry (default 360, was 60); the doubling formula is removed. The existing check `max >= base` stays.

Scope: the schedule applies to every realtime recovery of the worker (stale, reconnect, sequence gap), because they share one worker and one REST budget. Historical backfill backoff is a separate worker and is untouched.

Not done: priority of 5m over 1h/4h/1d in the shared operation gate. Per-stream schedules cap the load, but a rare higher-timeframe request may still land on a 5m boundary. A strict guarantee would need a priority queue; not added now.

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
- If REST is down or lacks the expected bar, the stream retries after 60 s, then every 360 s (D6). Bars missed during that time arrive with the first successful attempt, up to about 6 minutes late; this is the accepted price of capping REST load.
- Reconnect and gap recoveries are slower to retry after a failure than before (60 s instead of 1-2 s); a successful first attempt is unaffected.
- Until the Runtime-notification change lands, faster recovery does not reach Runtime.
