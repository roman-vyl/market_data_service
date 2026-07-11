# Old BBB Data Engine — File-by-File Reuse Audit

## Status

Step 1 of the agreed pre-implementation plan is complete for the supplied repository snapshot.

Snapshot scope audited:

```text
data_engine/config.py
data_engine/contracts/*
data_engine/engine/*
data_engine/fetcher/*
data_engine/store/*
data_engine/service/cli.py
relevant tests and historical data-engine plans
```

This audit distinguishes:

- semantics to preserve;
- code that can be ported with limited changes;
- ideas to retain but APIs to redesign;
- code that must not be copied;
- parity tests required before the new implementation is accepted.

## Executive conclusions

The old Data Engine is not throwaway code. It already established several strong contracts:

1. half-open time windows `[start_ms, end_ms)`;
2. one canonical timeframe registry;
3. integer UTC millisecond timestamps;
4. deterministic timeframe-grid mathematics;
5. a single pure gap-detection algorithm;
6. bounded REST fetch windows;
7. Bybit `launchTime` discovery with local caching;
8. sorted and window-filtered Bybit responses;
9. non-recursive preflight/repair/postflight flow;
10. postflight continuity verification;
11. resumable trailing backfill from the last stored candle;
12. failure diagnostics stored outside ordinary candles;
13. explicit schema-health checks;
14. no strategy or VectorBT dependency inside the data layer.

These semantics should be preserved.

The following old choices must not be carried into the new service:

1. candle prices and volume stored as binary floating point;
2. identity limited to `(symbol, timeframe, open_time_ms)`;
3. transport-specific constants hardcoded globally to Bybit linear;
4. one `Db` class owning schema, reads, writes, health, metadata, and diagnostics;
5. unconditional upsert that cannot distinguish duplicate from correction;
6. `written_rows == len(input_rows)` even when rows were duplicates;
7. validation only after rows were already stored;
8. repair orchestration and historical policy inside a large CLI module;
9. silent suppression of diagnostic-write failures;
10. no atomic candle + event + stream-state transaction;
11. no persisted per-stream bootstrap state;
12. no first-class multi-symbol/instrument identity;
13. no distinction between bulk historical ingestion and realtime publication.

---

# 1. Configuration

## `data_engine/config.py`

### `Settings`

**Old responsibility**

- Reads database path and log level through `pydantic-settings`.
- Uses an environment prefix.
- Rejects invalid log levels early.
- Explicitly avoids global singleton configuration.

**Decision: PORT SEMANTICS, REDESIGN API**

Preserve:

- one settings object built at the entrypoint;
- explicit dependency passing;
- fail-fast validation;
- environment-prefix discipline;
- `Path` for filesystem locations.

Change:

- settings must include a path to the market configuration file;
- market list must live in a structured TOML file, not an environment JSON blob;
- Bybit endpoint/environment, HTTP limits, rate limiting, startup policy, API host/port, and database path become separate typed settings groups;
- no default that accidentally points to production data without an explicit deployment profile.

**Required parity tests**

- default path normalization;
- environment override;
- case-insensitive log-level normalization;
- invalid level rejected;
- no configuration singleton import side effects.

**New tests beyond parity**

- missing market configuration rejected;
- duplicate instrument identities rejected;
- duplicate stream identities rejected;
- unsupported venue/category/kind/timeframe rejected;
- at least one enabled stream required;
- BTCUSDT and ETHUSDT can coexist independently.

---

# 2. Domain contracts

## `data_engine/contracts/candle.py`

### `Candle`

**Old responsibility**

Immutable slotted dataclass containing:

```text
symbol
timeframe
open_time_ms
open/high/low/close/volume as float
```

**Decision: PRESERVE IMMUTABILITY, REPLACE CONTRACT**

Preserve:

- immutable value-object semantics;
- slotted/frozen representation where practical;
- `open_time_ms` as integer UTC milliseconds;
- OHLCV as the core candle payload.

Replace:

- `symbol + timeframe` with a `StreamKey`;
- binary floats with exact normalized decimals;
- add explicit source/provenance at the observation boundary;
- distinguish `ObservedCandle` from `CanonicalCandle`;
- avoid storing transport payload inside the canonical object.

Target conceptual split:

```text
InstrumentKey
  venue
  market_category
  symbol

StreamKey
  instrument
  timeframe

ObservedCandle
  stream
  open_time_ms
  open/high/low/close/volume
  confirmed
  observed_at_ms
  source

CanonicalCandle
  stream
  open_time_ms
  normalized OHLCV
  committed_at_ms
```

**Do not port**

- `float` fields;
- implicit Bybit-linear assumptions;
- inability to distinguish confirmed and partial observations.

**Required parity tests**

- immutable equality semantics;
- exact preservation of symbol, timeframe, timestamp and OHLCV values for ordinary fixtures.

**New tests beyond parity**

- decimal normalization;
- no false correction from alternate decimal spelling (`1.0` vs `1.000`);
- stream identity includes venue and category;
- unconfirmed candle cannot become canonical.

## `data_engine/contracts/time_window.py`

### `TimeWindow`

**Old responsibility**

Immutable half-open interval `[start_ms, end_ms)` with `start < end`.

**Decision: PORT SEMANTICS ALMOST UNCHANGED**

This is one of the strongest old contracts.

Preserve:

- half-open semantics;
- integer millisecond boundaries;
- immutable value object;
- strict non-empty invariant.

Possible change:

- rename to `TimeRange` only if the project consistently uses that term; otherwise keep `TimeWindow` to preserve clarity and parity.
- grid alignment is not a constructor invariant because the same type may represent transport or metadata windows. Grid-required use cases validate explicitly.

**Required parity tests**

- valid interval accepted;
- equal or reversed boundaries rejected;
- end is exclusive in all repository and fetch operations.

## `data_engine/contracts/gap.py`

### `Gap`

**Old responsibility**

Immutable missing half-open interval with `start < end`.

**Decision: PORT SEMANTICS ALMOST UNCHANGED**

Preserve:

- half-open interval;
- no symbol or timeframe inside the mathematical gap object;
- immutable contract.

Change:

- application records that persist a gap will combine `StreamKey + Gap` and lifecycle/status metadata;
- the pure `Gap` remains independent from persistence.

**Required parity tests**

- constructor invariant;
- one-candle gap representation;
- contiguous absent timestamps collapse into one gap.

## `data_engine/contracts/fetch_request.py`

### `FetchRequest`

**Old responsibility**

Combines `symbol`, `timeframe`, and a `TimeWindow`.

**Decision: PORT IDEA, REPLACE IDENTITY**

Target:

```text
CandleFetchRequest
  stream: StreamKey
  window: TimeWindow
  limit: optional explicit transport bound
```

Preserve:

- explicit bounded request object;
- no hidden global current symbol/timeframe.

Change:

- full stream identity;
- optionally require closed-window policy at the application boundary rather than transport adapter;
- exchange limit remains adapter-owned but planning can receive it as a capability.

**Required parity tests**

- window and stream passed unchanged to adapter;
- independent requests for BTC and ETH never share mutable state.

## `data_engine/contracts/fix_report.py`

### `FixReport`

**Old responsibility**

Aggregated result of a repair pass, including gaps before/after, row counts, invalid rows, freshness, diagnostics and status.

**Decision: RETAIN CONCEPT, SPLIT INTO APPLICATION RESULTS**

The report was useful, but one broad DTO should not become the central service model.

Preserve concepts:

- preflight and postflight gap visibility;
- fetched/accepted/rejected counts;
- freshness/continuity outcome;
- structured status rather than only exceptions;
- diagnostics available to operators.

Redesign into smaller results:

```text
ContinuityAuditResult
RepairPlan
RepairRunResult
IngestionBatchResult
StreamReadinessSnapshot
```

Avoid:

- mutable `list` fields inside a frozen dataclass;
- string diagnostics as the only machine-readable failure representation;
- a single status attempting to summarize network, validation, continuity and readiness.

**Required parity tests**

- old status scenarios map to explicit new outcomes:
  - complete;
  - incomplete;
  - invalid observation;
  - hard failure.

---

# 3. Timeframe registry and grid mathematics

## `data_engine/contracts/timeframes.py`

### `TimeframeSpec`
### `TIMEFRAME_SPECS`
### `validate_timeframe`
### `timeframe_ms`
### `bybit_interval`
### `pandas_freq_alias`

**Old responsibility**

Single source of truth mapping internal timeframe ids to duration, Bybit interval and pandas frequency.

**Decision: PORT CORE DESIGN, EXPAND AND RE-LAYER**

This is a high-value old design and must not be reinvented independently in multiple adapters.

Preserve:

- one registry;
- canonical internal ids;
- one duration mapping;
- strict validation;
- no duplicate timeframe maps elsewhere.

Required change:

- add mandatory `1m`:

```text
id = 1m
duration_ms = 60_000
Bybit interval = 1
```

Layering change:

- `id` and `duration_ms` belong in domain;
- Bybit interval belongs in the Bybit adapter capability map;
- pandas alias is research-facing and should not be a core service dependency unless exported as plain metadata.

Reason:

The old registry mixed domain duration with two consumer-specific representations. The “single source of truth” principle remains, but adapter metadata must not force pandas or Bybit concepts into the domain model.

**Required parity tests**

- old 5m, 15m, 1h, 4h, 1d durations preserved;
- old Bybit mappings preserved in the Bybit adapter;
- unsupported id rejected with stable diagnostics.

**New tests beyond parity**

- `1m == 60_000`, Bybit interval `1`;
- no second timeframe-duration mapping exists in source tree;
- every configured stream timeframe is supported by the selected venue adapter.

## `data_engine/engine/time_grid.py`

### `tf_ms`

**Decision: REMOVE WRAPPER OR KEEP ONLY AS DOMAIN ALIAS**

The old wrapper simply delegates to `timeframe_ms`. It has no independent value. Do not create duplicate public functions unless compatibility requires it.

### `align_to_grid`

**Decision: PORT SEMANTICS**

Preserve floor-to-grid behavior.

### `ceil_to_grid`

**Decision: PORT SEMANTICS**

Preserve exact behavior: aligned timestamps remain unchanged, unaligned timestamps move to the next boundary.

### `next_close_ms`

**Decision: PORT WITH CLEARER NAME/CONTRACT**

The function returns the close boundary of the bar containing the supplied timestamp. The name can be misread as “next closed candle after now”. Preserve behavior but document precisely, or rename to `containing_bar_close_ms`.

### `last_closed_open_time_ms`

**Decision: PORT SEMANTICS, ADD CLOCK-BOUNDARY TESTS**

Old behavior:

```text
align_to_grid(now) - step
```

This intentionally excludes the bar opening exactly at the current boundary because it has only just begun. This is correct for closed-candle processing.

Preserve this contract.

**Required parity tests**

- floor alignment;
- ceil alignment;
- exact boundary behavior;
- just-before and just-after boundary behavior;
- 1m boundary behavior;
- negative timestamps either explicitly rejected or deliberately supported; do not leave accidental Python-floor semantics undocumented.

---

# 4. Gap detection and fetch planning

## `data_engine/engine/gaps.py`

### `find_gaps_linear`

**Old responsibility**

Given timestamps, a step and a half-open window:

- constructs the expected grid;
- ignores duplicates;
- accepts unsorted timestamps;
- ignores timestamps outside the audited window;
- returns collapsed contiguous gaps.

**Decision: PORT SEMANTICS; ALGORITHM MAY BE OPTIMIZED**

This is the single most important pure algorithm to retain.

Preserve exact behavior:

- empty data means one full-window gap;
- complete grid means no gaps;
- leading/internal/trailing gaps detected;
- adjacent missing points collapsed;
- duplicate and unsorted inputs tolerated;
- out-of-window values ignored;
- `step_ms <= 0` rejected.

Potential scalability change:

The old implementation builds a `set` of all actual timestamps and iterates the full expected minute range. This is acceptable for moderate windows but a full multi-year 1m audit creates millions of expected iterations and a large set.

The new implementation may use a sorted streaming scan or SQL-side ordered iterator, but it must be behaviorally identical.

Do not add database access to the pure gap algorithm.

**Required parity tests**

Port all old `tests/test_gaps.py` cases unchanged in meaning.

Add:

- misaligned actual timestamps;
- very large 1m window performance guard;
- iterator input or chunked audit if selected;
- multi-symbol isolation at the application layer;
- no pre-history gap before the persisted required-history start.

## `data_engine/engine/dim.py`

### `iter_fetch_windows`

**Old responsibility**

Splits an aligned gap into half-open windows containing at most `max_candles` bars.

**Decision: PORT SEMANTICS ALMOST UNCHANGED**

Preserve:

- positive step and limit validation;
- aligned boundaries required;
- deterministic contiguous coverage;
- no overlap;
- no holes;
- exact max-size behavior;
- final short window.

API change:

- return an iterator instead of materializing a list for very deep minute history;
- use a generic name such as `iter_bounded_windows` or `plan_fetch_windows`;
- exchange limit supplied by adapter capabilities or application policy.

**Required parity tests**

- one exact-size window;
- one-over-limit produces two windows;
- last partial window;
- complete union equals original gap;
- no overlap;
- invalid alignment/step/limit rejected;
- multi-million minute interval does not build all windows in memory.

### `_count_invalid_ohlc`

**Old responsibility**

Counts invalid rows after reading stored data.

Checks:

- prices must be positive;
- volume non-negative;
- high >= low/open/close;
- low <= open/close.

**Decision: PRESERVE RULES, REJECT IMPLEMENTATION LOCATION AND TIMING**

Strong rule to preserve: these OHLC constraints are valid.

Change completely:

- validate each observation before persistence;
- return typed failures, not only a count;
- exact decimals, not floats;
- check finite/parseable values before comparison;
- validate grid alignment, configured stream, confirmed status and timestamp range too;
- optionally perform storage-audit validation separately as a corruption check.

### `_put_quarantine_safe`

**Old responsibility**

Best-effort append-only diagnostic write; swallows every exception.

**Decision: RETAIN DURABLE DIAGNOSTIC IDEA, REJECT SILENT FAILURE**

Preserve:

- ingestion/repair failures should be queryable after the process exits;
- diagnostics should not pollute the canonical candle table.

Reject:

- `except Exception: return` without logging or metrics;
- free-form payload as the only structure;
- diagnostics written in unrelated independent transactions when atomic linkage is needed.

New model:

- typed ingestion diagnostics or repair-run records;
- diagnostic write failures are logged and reflected in run status;
- raw payload retention bounded and deliberate.

### `fix_candles`

**Old responsibility**

Non-recursive repair pipeline:

1. validate timeframe/window/limit;
2. read current range;
3. find gaps;
4. split each gap;
5. fetch;
6. filter unexpected rows;
7. upsert;
8. reread range;
9. re-audit gaps and OHLC;
10. check expected latest candle;
11. return a structured report.

**Decision: PRESERVE WORKFLOW, DECOMPOSE IMPLEMENTATION**

This old workflow is good and should remain visible in the new architecture.

Preserve:

- explicit supplied window;
- no recursion;
- preflight and postflight;
- same gap algorithm for historical and reconnect repair;
- filtering/validation of rows against requested stream and window;
- hard failure differentiated from incomplete repair;
- expected-latest/freshness check;
- repair continues across independent windows where policy permits.

Redesign into use cases:

```text
AuditStreamContinuity
PlanGapRepair
FetchCandleWindow
IngestObservedCandleBatch
RepairStreamGaps
EvaluateStreamReadiness
```

Problems in old implementation to avoid:

- untyped `db` and `fetcher` dependencies;
- one function owns too many responsibilities;
- validation occurs after upsert;
- upsert silently overwrites corrections;
- row count semantics are inaccurate;
- status strings combine unrelated dimensions;
- no transaction joining candle and durable event;
- symbol/timeframe identity incomplete;
- no per-stream persisted repair state;
- no bounded shared scheduler for multiple instruments.

**Required parity tests**

Carry forward old DIM scenarios:

- no gaps => no fetch;
- one gap repaired;
- empty fetch => incomplete;
- fetch error => hard failure + diagnostic;
- invalid OHLC reported;
- postflight gap remains => incomplete;
- function is non-recursive;
- core does not import entrypoint/CLI;
- large gap split into multiple calls;
- unexpected rows rejected and reported.

Add:

- invalid rows never reach canonical storage;
- exact duplicate is no-op;
- changed row is correction, not ordinary upsert;
- batch transaction rollback;
- BTC repair failure does not mutate ETH state;
- restart resumes persisted repair/bootstrap state;
- bulk bootstrap does not create realtime notification per candle.

---

# 5. Fetcher abstraction and Bybit REST

## `data_engine/fetcher/base.py`

### `IFetcher`

**Old responsibility**

Protocol exposing one `fetch_candles(request)` method.

**Decision: PORT PORT-BASED DESIGN, EXPAND CAPABILITIES**

Preserve:

- application code depends on an interface, not `pybit`;
- request/response use domain-neutral contracts.

Expand into focused ports:

```text
CandleHistorySource.fetch_candles
InstrumentMetadataSource.get_instrument
RealtimeCandleSource (later phase)
```

Do not make one giant exchange client port.

Add adapter capability metadata:

- supported categories;
- supported native timeframes;
- maximum candles per call;
- pagination semantics.

## `data_engine/fetcher/bybit_rest.py`

### Constants `BYBIT_CATEGORY`, `BYBIT_KLINE_LIMIT`

**Decision: PRESERVE VALUES AS ADAPTER DEFAULTS, NOT GLOBAL DOMAIN CONSTANTS**

Old values:

- category `linear`;
- limit `200`.

The new service is explicitly Bybit-linear-perpetual initially, but multi-symbol configuration means category belongs to the instrument/adapter request context.

The exact current Bybit request limit must be verified against official docs at implementation time; tests should use adapter capability rather than spread the number through application code.

### `BybitHTTPError`

**Decision: KEEP TYPED ADAPTER ERROR IDEA, REDESIGN ERROR CLASSIFICATION**

Need separate error categories:

- transient transport;
- rate limited;
- exchange server error;
- invalid request/permanent response;
- malformed payload;
- instrument not found.

### `_default_client`

**Decision: DO NOT PORT AS DOMAIN/APPLICATION BEHAVIOR**

Concrete client construction belongs in entrypoint composition. Adapter constructors may provide a convenience factory, but tests and application use dependency injection.

### `_is_retryable_exception`

**Old behavior**

Retries:

- status 429;
- status 5xx;
- `ConnectionError`, `TimeoutError`, `OSError`.

**Decision: PRESERVE BASE POLICY, MAKE EXPLICIT AND TESTED**

Good baseline. Improve:

- distinguish Bybit retCode from HTTP status code;
- respect rate-limit headers where available;
- bounded jitter/backoff;
- expose retry exhaustion diagnostics;
- shared venue-level rate limiter for BTC/ETH fairness.

### `_default_retrying`

**Old behavior**

Five attempts, exponential wait 1–16 seconds, re-raise.

**Decision: RETAIN AS REFERENCE, MAKE CONFIGURABLE**

Do not hardcode as universal policy. Keep bounded retries and exponential backoff.

### `_ensure_ok`

**Old behavior**

Accepts `retCode` 0/"0", otherwise raises.

**Decision: PORT SEMANTICS, IMPROVE PAYLOAD VALIDATION**

Preserve Bybit envelope validation. Add validation for missing/malformed `result.list` and typed code classification.

### `BybitREST.fetch_candles`

**Old behavior worth preserving**

- rejects windows larger than adapter limit;
- maps internal timeframe to Bybit interval;
- sends category, symbol, interval, start, end and explicit limit;
- converts half-open end to inclusive transport end with `end_ms - 1`;
- retries request;
- filters response rows back to requested half-open window;
- sorts ascending because Bybit may return reverse order.

**Decision: PORT BEHAVIOR, REWRITE NUMERIC PARSING AND IDENTITY**

This is high-value behavior.

Required changes:

- parse numeric strings as exact decimals;
- emit `ObservedCandle`, not canonical candles;
- include full `StreamKey`;
- reject malformed row shapes explicitly;
- carry observation source metadata;
- no hardcoded category disconnected from configured instrument;
- validate unexpected timestamps and duplicates as diagnostics;
- do not decide duplicate/correction here.

**Required parity tests**

Port old tests for:

- payload mapping;
- ascending order;
- transient retries;
- retryable retCode;
- linear category;
- timeframe mapping;
- `end = window.end - 1`;
- explicit limit;
- window filtering;
- oversized request rejected.

Add:

- 1m interval maps to `1`;
- decimal lexical normalization;
- malformed row rejected;
- duplicate rows returned deterministically for later classification or deduplicated with explicit diagnostic policy;
- ETH request does not inherit BTC symbol;
- non-retryable exchange response fails once;
- rate-limiter fairness.

### `fetch_launch_time_ms`

**Old behavior**

Calls `get_instruments_info(category=linear, symbol=...)`, validates envelope, requires at least one row, parses `launchTime`.

**Decision: PORT BEHAVIOR INTO INSTRUMENT METADATA ADAPTER**

Preserve:

- use Bybit instrument metadata as the initial lower-bound hint;
- typed failure when instrument is absent;
- retry policy.

Change:

- return full instrument metadata, not only launch time;
- validate requested symbol exactly matches returned symbol;
- capture base coin, quote coin, settle coin, contract type, status, tick size, quantity step where available;
- `launchTime` is not the canonical earliest candle.

## `data_engine/fetcher/depth_resolver.py`

### `resolve_launch_time_ms`

**Old responsibility**

Read cached launch time from DB, fetch and cache only when absent.

**Decision: PORT CACHE-ASIDE SEMANTICS, EXPAND IDENTITY AND METADATA**

This is another strong old solution.

Preserve:

```text
read durable metadata
if present: avoid network call
if absent: fetch, validate, persist, return
```

Change:

- cache keyed by full `InstrumentKey`;
- store fetched timestamp and metadata version/source;
- separate `instrument_launch_time_ms` from `earliest_available_candle_open_time_ms` per stream;
- allow explicit refresh policy rather than treating metadata as immutable forever.

**Required parity tests**

- cache hit avoids network;
- cache miss fetches and stores;
- Bybit linear category supplied;
- launch time parsed;
- retryable metadata error retried;
- no rows => typed error.

Add:

- BTC and ETH caches independent;
- launch-time refresh does not overwrite observed earliest candle;
- metadata mismatch rejected.

---

# 6. Storage schema and database wrapper

## `data_engine/store/ddl.py`

### `EXPECTED_TABLES`
### `DDL_STATEMENTS`
### `SCHEMA_VERSION_INSERT`

**Old responsibility**

Defines schema in code with four tables:

- `schema_meta`;
- `candles`;
- `meta`;
- `quarantine`.

**Decision: RETAIN EXPLICIT MIGRATION OWNERSHIP, REPLACE SCHEMA**

Preserve:

- schema is explicit and versioned;
- startup does not assume a compatible existing DB;
- health validates expected schema;
- creation is idempotent.

Reject:

- one static DDL tuple as the long-term migration mechanism;
- `REAL` OHLCV;
- key limited to symbol/timeframe/time;
- `meta` keyed only by symbol;
- no foreign keys between instruments, streams and state;
- no event log or bootstrap state.

Step 3 will define the exact new schema. Preliminary required concepts remain:

```text
schema_migrations
instruments
streams
candles
stream_state
bootstrap_runs or bootstrap_state
gap_records
market_events
ingestion_diagnostics
```

## `data_engine/store/db.py`

### `Db.__init__`

**Old behavior**

- opens connection;
- enables WAL;
- sets 30s busy timeout;
- sets synchronous NORMAL;
- does not auto-create schema.

**Decision: PRESERVE PRAGMA BASELINE AND EXPLICIT MIGRATION POLICY, REDESIGN CONNECTION OWNERSHIP**

WAL, busy timeout, synchronous NORMAL are good proven defaults for review in Step 3.

Change:

- explicit connection factory/unit of work;
- enable and verify foreign keys;
- define thread/process ownership;
- no long-lived publicly exposed raw connection;
- migrations are separate from repositories.

### `apply_ddl`

**Decision: REPLACE WITH MIGRATION RUNNER**

Preserve idempotent startup behavior, but use ordered migrations and a clear failure mode.

### `health`

**Old behavior**

Read-only schema check; does not silently repair an existing broken DB.

**Decision: PORT SEMANTICS**

This is important and matches the newly agreed cold-start behavior.

Preserve:

- missing new database may be initialized by startup;
- existing incompatible database is not silently destroyed or rewritten;
- health check is read-only;
- schema version mismatch is explicit.

Improve:

- distinguish missing DB, migration pending, incompatible schema, corruption, inaccessible path;
- health is not readiness;
- avoid full row counts over multi-million candle tables in routine health checks.

### `upsert`

**Old behavior**

Bulk `executemany`, updates values on key conflict, returns number of input rows.

**Decision: DO NOT PORT API OR SEMANTICS**

Good part:

- batch write inside one transaction;
- SQL conflict handling can be efficient.

Fatal problems for new requirements:

- exact duplicates and corrections indistinguishable;
- corrections silently overwrite old values;
- returned count is not actual inserted/updated count;
- no validation;
- no event/state atomicity;
- float equality;
- no source/provenance.

Replacement:

```text
AtomicCandleCommitUnitOfWork
  classify existing row
  insert new / no-op duplicate / update correction
  append correct event policy
  advance stream state
  commit once
```

Bulk bootstrap may use optimized staging/batches, but must preserve classification and recoverability contracts.

### `range_get`

**Decision: PORT QUERY SEMANTICS, REPLACE DTO/KEY**

Preserve:

- half-open range;
- ascending order;
- stream-filtered query.

Change:

- full `StreamKey`;
- exact decimal decoding;
- pagination/streaming for multi-million rows;
- API/export may need chunked reads rather than one giant list.

### `count_candles`

**Decision: RETAIN AS AUDIT TOOL, NOT PRIMARY COMPLETENESS PROOF**

Counts are useful but insufficient: equal count can hide one missing and one misaligned/duplicate timestamp. Continuity audit remains authoritative.

### `max_open_time_ms`, `min_open_time_ms`, `candle_summary`

**Decision: PORT SEMANTICS**

Useful for bootstrap/resume/status, keyed by full stream identity.

Do not treat min/max alone as proof that no internal gaps exist.

### `set_launch_time_ms`, `get_launch_time_ms`

**Decision: REPLACE WITH INSTRUMENT METADATA REPOSITORY**

Preserve cache-aside behavior, expand identity and metadata fields.

### `put_quarantine`

**Decision: REPLACE WITH TYPED DIAGNOSTIC REPOSITORY**

Preserve durable operator visibility, redesign structure and failure semantics.

### `_all_expected_tables_exist`, `_schema_version_value`

**Decision: PORT INTENT INTO MIGRATION/SCHEMA INSPECTOR**

Required behavior remains, implementation changes with migrations.

**Required storage parity tests**

- explicit schema initialization;
- health does not mutate schema;
- missing expected table => mismatch;
- half-open ascending range query;
- min/max/summary;
- metadata cache read/write;
- batch transaction rollback;
- WAL and configured pragmas verified.

**New tests beyond parity**

- duplicate versus correction classification;
- atomic candle + state + event commit;
- no event after rollback;
- decimals round trip exactly;
- BTC/ETH stream keys do not collide;
- existing incompatible DB is preserved and service stays unready;
- large historical range can be streamed/chunked.

---

# 7. CLI and orchestration

## `data_engine/service/cli.py`

This is the largest old Data Engine file at roughly 393 lines and already shows why the new project needs strict file responsibility rules.

### Printing helpers

**Decision: DO NOT PORT TO CORE**

Formatting belongs in entrypoint adapters. Structured application results remain independent.

### `_make_fetcher`, `_resolve_launch_time`, `_now_ms`

**Decision: REPLACE WITH COMPOSITION ROOT AND PORTS**

Good old idea: dependencies are patchable in tests.

New service should inject:

- clock;
- metadata source;
- candle source;
- repositories/unit of work.

### `_parse_tf`

**Decision: DOMAIN VALIDATION MOVES OUT OF CLI**

CLI/API adapters convert input and call domain parser.

### `_discover_first_available_open_time_ms`

**Old behavior**

Starting from aligned launch-time candidate, scans bounded Bybit windows forward until the first non-empty response and returns the minimum open time.

**Decision: PORT ALGORITHM INTO A DEDICATED APPLICATION USE CASE**

This is a particularly important good solution that was initially easy to overlook.

Preserve:

- `launchTime` is only the initial candidate;
- empty leading windows are allowed before any history has been found;
- scan advances by bounded exchange windows;
- first actual candle, not launch time, establishes available history.

Change:

- full stream identity;
- persist discovery state for resume;
- distinguish no history yet from permanent instrument/config error;
- exact 1m support;
- do not rescan from launch time after restart;
- protect with shared rate limiter and fair multi-symbol scheduling.

### `status`

**Decision: RETAIN OPERATOR CAPABILITY, REIMPLEMENT THROUGH APPLICATION QUERIES**

Useful semantics:

- new DB can be initialized deliberately;
- existing schema mismatch shown, not auto-fixed;
- per-symbol/timeframe span available.

New service exposes health/readiness/API and optional admin CLI, with per-stream status.

### `backfill` and `_run_backfill`

**Good old behavior**

- resolves/caches launch time;
- aligns candidate start;
- calculates latest closed candle;
- resumes from `max_open_time + step`;
- allows empty leading chunks before first actual candle;
- treats empty chunks after history has begun as error;
- bounds requests;
- completion check uses full effective history window, not only new tail;
- second run is idempotent at the storage-key level.

**Decision: PRESERVE THESE SEMANTICS, REPLACE ORCHESTRATION**

This is valuable behavior and should become the new per-stream bootstrap state machine.

Important caveat:

The old completion check compares expected and actual counts. This can report complete despite malformed/misaligned rows if storage permits them. New completion requires continuity audit.

### `fix`

**Good old behavior**

- on non-empty DB, starts audit at actual minimum stored candle;
- on empty DB, discovers first actual available candle;
- builds full historical window;
- delegates to common repair function;
- does not implement a second gap algorithm.

**Decision: PRESERVE POLICY IN DEDICATED USE CASES**

Cold start and restart should use the same underlying discovery/backfill/audit/repair components, but not remain inside CLI.

**Old CLI tests worth preserving as acceptance scenarios**

- unsupported timeframe rejected;
- non-1h timeframe works;
- empty DB backfill;
- broken existing DB is not auto-fixed;
- resume from last stored candle;
- second run idempotent;
- full-history completion check;
- count mismatch => incomplete;
- no history found => error;
- leading empty chunks allowed;
- empty chunk after data started => error;
- chunking respects exchange limit;
- discovery returns effective first candle;
- fix delegates full historical window;
- non-ok repair exits nonzero;
- schema mismatch prevents network call.

---

# 8. Historical plans and architecture rules

## `docs/data engine/00_master_plan.md`

**High-value decisions to retain**

- Data Engine is a clean data layer, not a strategy layer.
- One config source and explicit dependency passing.
- One database path.
- One gap algorithm reused by repair and future realtime watchdog.
- One timestamp representation: `open_time_ms` integer UTC milliseconds.
- No upward imports across layers.
- Repair is non-recursive: preflight -> fix -> postflight.
- Same candle writer for backfill, repair and realtime.
- Realtime must not create a second candle-write path.
- Strategy/research dependencies do not enter core.

**Old decisions intentionally superseded**

- 1m was deferred; now 1m is mandatory canonical history.
- multi-symbol was deferred; now multi-symbol is foundational.
- realtime was a later frozen phase; the standalone service is explicitly built for it, though still phased after storage/REST.
- CLI as sole mutating path; new service has a long-running runtime, while admin CLI remains optional.

## `docs/data engine/02_historical_backfill.md`

**Retain**

- explicit bounded fetch request;
- launch-time-based depth discovery;
- full-history backfill;
- resumable operation;
- exchange-window chunking;
- no research dependency.

## `docs/data engine/03_dim_repair.md`

**Retain strongly**

- empty DB represented as a full-window gap rather than a separate repair algorithm;
- one gap detector;
- historical policy outside core repair function;
- postflight audit;
- no recursive self-repair;
- DB, fetcher, gap math and orchestration responsibilities separated.

For the new service, bulk bootstrap may still be a distinct application workflow for performance/event semantics, but it must use the same canonical validation/classification/commit primitives rather than a second storage path.

## `docs/data engine/06_realtime.md`

Though only a phase card, it contains two critical rules to preserve:

1. watchdog catch-up reuses the same gap/repair path;
2. WebSocket writes through the same canonical writer as REST/backfill.

These are now normative requirements of the new service.

---

# 9. Concrete port/reject matrix

| Old artifact | Decision | Notes |
|---|---|---|
| `Settings` validation style | Port semantics | Expand typed settings and TOML markets |
| immutable dataclasses | Port | Replace candle identity and numeric types |
| `TimeWindow` | Port nearly unchanged | Keep half-open interval |
| `Gap` | Port nearly unchanged | Persisted gap record wraps stream identity |
| timeframe registry concept | Port | Add 1m; separate domain from adapter metadata |
| `align_to_grid` | Port | Add boundary tests |
| `ceil_to_grid` | Port | Preserve aligned input behavior |
| `last_closed_open_time_ms` | Port | Critical closed-candle semantic |
| `find_gaps_linear` behavior | Port | May optimize algorithm for 1m scale |
| `iter_fetch_windows` behavior | Port | Return iterator, adapter capability limit |
| OHLC rules | Port | Validate before write with Decimal |
| `fix_candles` workflow | Port architecture | Decompose into use cases |
| `IFetcher` interface idea | Port | Split metadata/history/realtime ports |
| Bybit half-open-to-inclusive `end-1` | Port | Adapter-specific behavior |
| Bybit ascending sort | Port | Required normalization |
| Bybit response window filter | Port | Required defensive behavior |
| transient retry baseline | Port concept | Add typed codes, rate limiter and jitter |
| `launchTime` metadata fetch | Port | Return richer instrument metadata |
| cache-aside launch time | Port | Full InstrumentKey and refresh policy |
| first-available-candle scan | Port | Persist per-stream discovery progress |
| WAL / busy timeout / NORMAL | Keep as baseline | Finalize in SQLite design step |
| explicit schema health check | Port | Separate liveness/readiness |
| static DDL tuple | Reject | Use migrations |
| `REAL` OHLCV | Reject | Exact Decimal policy |
| old candle primary key | Reject | Full stream identity |
| unconditional upsert | Reject | Duplicate/correction classification |
| one broad `Db` class | Reject | Repositories + unit of work |
| silent quarantine failure | Reject | Typed diagnostics, visible failures |
| 393-line CLI orchestration | Reject structure | Split use cases and entrypoint formatting |
| count-only completeness | Reject as authority | Continuity audit is authoritative |
| one event per historical row | Not present; forbid | Bulk/realtime event policy handled separately |

---

# 10. Parity fixture set to carry into the new repository

The new repository should recreate the following fixed behavior fixtures before Phase 1/2 are considered stable.

## Time/grid fixtures

- all old supported durations;
- mandatory 1m duration;
- floor alignment;
- ceil alignment;
- exact boundary latest-closed behavior;
- half-open ranges.

## Gap fixtures

- empty full gap;
- complete range;
- leading/internal/trailing gaps;
- multiple gaps;
- adjacent missing points collapsed;
- duplicates ignored;
- unsorted inputs accepted;
- out-of-window timestamps ignored.

## Bybit adapter fixtures

- reverse response sorted ascending;
- request end translated to `end - 1`;
- row window filter;
- explicit limit;
- retry transient network/429/5xx;
- non-retryable failure not retried;
- launch time metadata parsing;
- first available candle later than launch time;
- leading empty windows before first data.

## Bootstrap/repair fixtures

- new DB full history;
- existing empty DB;
- resume from max open time;
- idempotent repeat;
- empty chunk before first candle allowed;
- empty chunk after history starts is failure/incomplete;
- postflight audit required;
- unexpected stream/time rows rejected;
- schema mismatch prevents network activity;
- one gap algorithm for bootstrap and reconnect repair.

## Storage fixtures

- schema initialization explicit;
- existing incompatible DB preserved;
- WAL-related settings inspected;
- half-open ascending range read;
- transaction rollback;
- exact decimal round trip;
- duplicate no-op;
- correction visible;
- atomic event/state/candle commit;
- BTC and ETH isolation.

---

# 11. Step 1 completion decision

Step 1 is complete for the supplied snapshot.

The old code is sufficiently understood to proceed to Step 2: finalize exact instrument and stream semantics.

No production code should yet be ported. During implementation, each ported behavior must cite this audit and be covered by the listed parity tests.
