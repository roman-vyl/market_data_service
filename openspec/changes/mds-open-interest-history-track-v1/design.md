# Design

## Context (verified at main 10a79de)
- Runtime history for candles is `HistoricalStreamReconciler`: `ResolveHistoricalLowerBound` then `RepairStreamGaps` over the whole
  window `[earliest_available, last closed bar]` (fresh preflight audit every pass). `BackfillStreamHistory` is used only by the
  administrative backfill command.
- Already track-neutral: `domain/gaps.iter_fetch_windows` (window size is a parameter), `domain/continuity.build_continuity_report`
  (takes a plain tuple of open times), `application/source_failure.classify_source_failure`.
- Bound to candles in two places only: where data comes from (`HistoricalCandleSource.fetch_closed_candles`, `list_candles`) and
  where progress is written (`stream_state`). The second is the dangerous one: `RepairStateRecorder`, `BackfillStreamHistory` and
  `HistoricalStreamReconciler` move the PUBLIC stream lifecycle (`auditing`, `repairing`, `connecting`). Calling them for OI as they
  are would move a live READY stream into REPAIRING.
- `HistoricalReconciliationWorker` owns queue, fairness and backoff, but its `on_complete` admits the stream to realtime and it writes
  blocking reasons into the runtime status that readiness reads.
- The schema is versioned (v2); an unknown version fails closed.

## Decisions

### D1. Configuration
Per instrument, optional boolean `open_interest` (default `false`). When `true` the OI track is enabled for that instrument's `5m`
stream; the instrument MUST include `5m` in `canonical_timeframes`, otherwise configuration loading fails. No other timeframe gets
an OI track. The config `schema_version` stays 1: the key is optional and additive. Turning the flag off stops OI work for that
stream and leaves stored OI rows untouched.

### D2. Storage, schema v3
Migration v2 -> v3 only creates two tables; no existing table or row changes.

```
open_interest (
  stream_id        INTEGER NOT NULL REFERENCES streams(id),
  open_time_ms     INTEGER NOT NULL CHECK (open_time_ms >= 0),
  open_interest    TEXT    NOT NULL,          -- canonical decimal text, domain-normalized
  source           TEXT    NOT NULL CHECK (source IN ('bybit_rest')),
  committed_at_ms  INTEGER NOT NULL,
  PRIMARY KEY (stream_id, open_time_ms)
) WITHOUT ROWID

track_progress (
  stream_id                         INTEGER NOT NULL REFERENCES streams(id),
  track                             TEXT    NOT NULL CHECK (track IN ('open_interest')),
  earliest_available_open_time_ms   INTEGER,
  discovery_next_open_time_ms       INTEGER,
  latest_committed_open_time_ms     INTEGER,
  last_audit_at_ms                  INTEGER,
  last_audit_gap_count              INTEGER,
  last_success_at_ms                INTEGER,
  last_error_code                   TEXT,
  last_error_detail                 TEXT,
  updated_at_ms                     INTEGER NOT NULL,
  PRIMARY KEY (stream_id, track),
  CHECK (earliest_available_open_time_ms IS NULL OR discovery_next_open_time_ms IS NULL)
)
```

`track_progress` holds internal progress facts only. It has no lifecycle state column and is not read by readiness, status or any
read endpoint in this change. Why not generalize `stream_state` to `(stream, track)` now: readiness, supervisor, realtime and `/v1`
read it, so generalizing it pulls step 2 into step 1. Step 2 may fold the two together when composite readiness exists.

Operational consequence: an image without this change refuses a v3 database (unsupported schema fails closed). Rolling back to an
older image after deploy needs the database backup taken before deploy.

### D3. Source
New port `OpenInterestSource.fetch_open_interest(stream, window, *, observed_at_ms) -> tuple[ObservedOpenInterest, ...]` and adapter
`BybitOpenInterestSource`: `GET /v5/market/open-interest`, `category=linear`, `symbol`, `intervalTime=5min`, `startTime`, `endTime`,
`limit=200`, on the existing `UrllibJsonHttpTransport` and retry of `rest_client.py`. Rows are parsed into Decimal, filtered to the
requested window, de-duplicated and sorted ascending. Only points of closed bars are accepted (`open_time_ms <=` last closed open
time at `observed_at_ms`), as for REST klines. A row whose timestamp is off the 5m grid is invalid.

`BybitRestCandleSource` and `HistoricalCandleSource` are not changed.

### D4. OI observation and acceptance
`ObservedOpenInterest(stream, open_time_ms, open_interest, observed_at_ms)`. Validation: aligned to the 5m grid, finite decimal,
`>= 0`, canonical text per the decimal policy. Acceptance per point, inside the window transaction:
- no row: insert, COMMITTED;
- same value: DUPLICATE;
- different value: replace, CORRECTED (REST is the only OI source in this change);
- invalid: REJECTED_INVALID, recorded in quarantine with reason `invalid_open_interest`, not stored.
`latest_committed_open_time_ms` of the track moves by max. This is OI-track acceptance only; it does not create or change candles.

### D5. Small parametrizations instead of copies
- **Lower bound.** The probe loop of `ResolveHistoricalLowerBound` runs against two injected pieces: a window probe ("earliest valid
  point of this track in this window, or none") and a progress store (resolved bound, discovery cursor). The candle instance keeps
  the candle source, `validate_observed_candle` and `stream_state`, with identical behavior; the OI instance uses the OI source and
  `track_progress`. Instrument launch time from cached metadata seeds both searches. OI probe windows are 200 points.
- **Audit.** `AuditStreamContinuity` takes an open-time reader. Candle default reads candles as today; the OI reader reads
  `open_interest` open times. `build_continuity_report` is shared.
- **Import.** New `ImportOpenInterestWindow`: fetch one OI window, accept every point (D4), one transaction per window, rows outside
  the stream/window go to quarantine as `unexpected_historical_open_interest`.
- **Repair.** `RepairStreamGaps` takes the window importer and a progress recorder through small protocols. The candle instance keeps
  `ImportHistoricalWindow` and `RepairStateRecorder` (unchanged behavior and lifecycle). The OI instance uses
  `ImportOpenInterestWindow` and a recorder that writes `track_progress` only and never touches the stream lifecycle. Max window 200.
- **No OI backfill class.** As for candles in runtime, history is loaded by repair over the full window from the lower bound; the
  fresh preflight audit each pass is what makes it resumable after restart.
- **Reconciler.** New small `OpenInterestTrackReconciler.execute(stream, window)`: OI lower bound -> OI repair -> outcome
  (`complete`, `incomplete`, `recoverable_failure`, `fatal_failure`). No `prepare_for_bootstrap`, no `mark_connecting`, no
  hand-off to realtime.

### D6. Runtime: automatic background track
- After candle startup, `RuntimeService` starts one more task in its TaskGroup: an OI track worker for flagged streams. OI work never
  runs before or inside candle startup, so candle startup, admission and readiness keep their current timing.
- The worker reuses `HistoricalReconciliationWorker` queue/backoff logic with two injected hooks: what happens on completion and how
  progress is reported. The candle instance keeps today's hooks (admit to realtime, runtime status blocking reasons). The OI instance
  re-enqueues a completed stream after `MDS_OI_TRACK_REFRESH_SECONDS` (default 300) to keep the tail, and reports only to logs and
  `track_progress`, never to the runtime status that readiness reads.
- Every OI pass takes the same `operation_gate` as candle history and realtime REST recovery, so REST-authoritative work stays
  serialized. Per-pass budget `MDS_OI_TRACK_WINDOWS_PER_PASS` (default 2, like the candle settings), so a realtime recovery waits at
  most one short OI pass.
- Fixed target per cycle: the window end is the last closed bar when the cycle starts, as for candles.
- Failures: recoverable -> existing backoff; fatal -> the OI track stops for that stream until restart, the error is stored in
  `track_progress` and logged. Any exception of the OI worker is caught inside the worker: it must never end the service TaskGroup
  and never change any stream's lifecycle or readiness.

### D7. Proof of completeness (automatic, no manual commands)
- Log lines per pass: `oi track pass begin|end stream=... lower_bound=... window=... remaining_gaps=... attempted=... committed=...
  duplicates=... corrected=... rejected=... error=...`.
- After every completed cycle the worker itself writes one summary line per stream: `oi track complete stream=... lower_bound=...
  latest=... expected_points=... stored_points=... gaps=0`, and stores the time and result of that audit in `track_progress`
  (`last_audit_at_ms`, gap count). No administrative command is needed or added.
- Target: lower bound resolved and gaps = 0 on all flagged coins; for BTC the audit starts at 2020-07-20, so March-July 2020 is
  not an OI gap.

### D8. Volume
About 105 000 5m points per year. BTC from 2020-07-20 is about 660 000 points, about 3 300 requests of 200; all 11 coins on the
order of 25 000 requests on first start, then one tail pass per stream every 5 minutes. Wall time depends on Bybit latency and is
measured on the first run; the budget per pass keeps realtime recovery responsive meanwhile.

## Checks on Bybit before code (read-only, owner's Mac; Bybit is blocked from the cloud)
1. A window before OI history starts returns an empty list (not an error), so discovery can advance.
2. `startTime`/`endTime` windows of 200 points return all points without a cursor.
3. Timestamps are on the 5m grid, and which bar the point at timestamp T describes (needed for step 2 joins).
4. Whether the current, not yet closed interval is returned.
