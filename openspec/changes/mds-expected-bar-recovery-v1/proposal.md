# MDS expected-bar recovery v1

## Why
Staleness is decided by "no confirmed WebSocket candle for `step * 2 + 5 s`" (10 min 5 s for 5m). Two problems follow:

1. A missed bar is noticed 5-10 minutes late. On 2026-10-18 the WebSocket went silent at 17:45 and the first recovery ran at 17:55:10.
2. A recovery does not move the age reference, so the next 1 s tick calls the stream stale again. 2026-10-18 17:55-18:10: about 450 empty recoveries. 2026-10-20 06:55:31-07:00:03: 2141 `recovery result reason=stale` lines for one unchanged window with `attempted=0 committed=0`. Both cycles ended by themselves only when a confirmed WebSocket candle arrived.

Shortening the timeout alone would make the cycle more aggressive. The right trigger is the expected bar, not the length of the silence.

## What changes
- For every admitted stream MDS knows the last bar that should already exist on its own time grid: the latest bar whose close plus a grace period (default 15 s) has passed.
- If that bar has not been committed (by WebSocket or by REST), MDS starts the existing REST recovery for the stream, once.
- The right to run that check is tied to the specific expected bar. After a `restored` recovery the stream is not checked again until the next expected bar becomes due.
- Retry limit after an unsuccessful recovery: first retry not earlier than 60 s, later ones not earlier than 360 s; a new expected bar does not reset it; a `restored` recovery does. `MDS_REALTIME_RECOVERY_BASE_SECONDS` default 1 -> 60, `MDS_REALTIME_RECOVERY_MAX_SECONDS` default 60 -> 360, doubling removed.
- `MDS_STALE_GRACE_MS` default changes from 5000 to 15000; `MDS_STALE_INTERVALS` is removed.

## Unchanged
Recovery window planning, the recovery worker queue, `restored`/`incomplete`/failure classification, `realtime_live` (still needs a confirmed WebSocket candle after recovery), `ready`, reconnect/startup/sequence/rejected recovery triggers.

## Out of scope
- Telling Runtime about bars restored over REST: separate change. Until it lands, a faster recovery fills the database but Runtime still hears only about WebSocket bars.
- Reconnecting a WebSocket that is open but silent.
- The cause of WebSocket drops.
