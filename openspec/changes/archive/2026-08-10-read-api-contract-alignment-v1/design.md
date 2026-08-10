## Context

Four public read/planning HTTP endpoints exist, routed through a hand-rolled
`http.server`-based router (`adapters/http/request_router.py`), not a
framework like FastAPI:

- `GET /v1/candles` — `ConsumerReadHttpHandler` → `GetCandleRange`
- `POST /v1/historical-candles` — `HistoricalReadHttpHandler` →
  `GetHistoricalCandleRange`
- `GET /v1/streams/{ticker}/{timeframe}/bounds` — `HistoryPlanningHttpHandler`
  → `GetStreamBounds`
- `POST /v1/streams/{ticker}/{timeframe}/continuity-audits` —
  `HistoryPlanningHttpHandler` → `AuditStreamRange` / `AuditStreamContinuity`

All four are governed by the single `market-data-read-contracts` canonical
capability (`openspec/specs/market-data-read-contracts/spec.md`), introduced
by the as-built baseline replacement (`c9d9ca0`/`f1f6fe2`). That baseline
was already present on `main` before this change started — it landed via
`origin/claude/canonical-specs-baseline-replace-p01w0o`, which was verified
and merged to `main` in preparation for this change, not as part of it. It
already states the target contract this change needs: `{error, detail}` envelope; `422` for validation/malformed-body/
range/alignment/bounds failures; `404 configured_stream_not_found` for an
unknown stream; `409 stream_not_ready`/`coverage_stale` for
readiness/provenance conflicts; `500 continuity_invariant_broken` for a
broken grid invariant; `422 invalid_request` for a malformed hash,
distinct from `409 coverage_stale` for a well-formed-but-stale one; a
single `from_ms`/`to_ms` continuity-audit request shape; `market_data_hash`
as a hash string on continuous coverage and `null` on gaps; and OpenAPI
coverage of all four routes.

Two independent exception-to-HTTP-response mapping functions exist in the
running code with different envelope shapes and different error codes for
equivalent conditions:

- `adapters/http/consumer_read/exception_mapping.py::map_exception` —
  used by `ConsumerReadHttpHandler` and `HistoricalReadHttpHandler`.
  Produces `{"error": <code>, "detail": <str>}`. `InvalidRange.code` is
  `"invalid_range"` (`application/consumer_read/errors.py`), not the
  capability's canonical `invalid_request`.
- `HistoryPlanningHttpHandler._map_exception` — used by the bounds and
  continuity-audits handlers. Produces `{"error": <code>, "message": <str>}`
  for `UnknownStreamError`/`ValueError`, or bare `{"error": <code>}` for
  `LookupError`/generic exceptions. `UnknownStreamError`'s HTTP code is
  `"stream_not_found"`, not the capability's canonical
  `configured_stream_not_found`.

`canonical_market_data_hash` (`application/consumer_read/provenance.py`) is
the single hash-computation function shared by all three hash-producing/
consuming call sites (candles read, historical read, continuity audit);
this change does not touch it. It always produces a 64-character lowercase
hexadecimal SHA-256 digest, which is the format `expected_market_data_hash`
validation needs to check against.

The maintained OpenAPI document is hand-written Python
(`adapters/http/consumer_read/openapi.py::openapi_document()`), served at
`GET /openapi.json`. It is the only OpenAPI artifact in the repository. It
has no entry for `POST /v1/historical-candles`, and its continuity-audits
request schema declares `start_time_ms`/`end_time_ms` with
`additionalProperties: false` — the opposite of both the capability's
canonical shape and the shape the running handler actually accepts as an
alias.

### Supersession note

An earlier draft of this proposal was built against the canonical spec
baseline that predates `c9d9ca0` (six separate legacy capabilities,
including standalone `consumer-read-api-v1` and
`historical-backtest-read-contract-v1` specs, plus a hand-built new
`history-planning-read-api-v1` capability for the bounds/continuity-audits
routes). That baseline is no longer current: `c9d9ca0` replaced those six
capabilities with nine as-built ones, folding all four read/planning
routes into the single `market-data-read-contracts` capability and
settling on `422` (not the earlier draft's `400`/`422` split) for
malformed/validation/range/alignment/bounds failures, and `422
invalid_request` (not the earlier draft's proposed `invalid_hash_format`)
for a malformed hash. This design supersedes that draft in full; nothing
from it is carried forward except the underlying inventory of runtime
discrepancies, which is restated above against the current baseline.

## Goals / Non-Goals

**Goals:**

- Bring the running handlers into conformance with the error-code and
  envelope contract `market-data-read-contracts` already states, by
  pinning the exact code names and adding scenarios the current spec text
  left implicit or endpoint-scoped.
- Preserve the existing canonical mapping exactly as already decided in
  the baseline: `422` for malformed/validation/range/alignment/bounds
  failures (no `400` status anywhere in this capability), `422
  invalid_request` for a malformed hash, `409 coverage_stale` for a
  well-formed-but-stale one.
- Make the single `from_ms`/`to_ms` continuity-audit request shape
  enforceable (not just stated) by adding an explicit rejection scenario
  for the legacy shape.
- Make the `POST /v1/historical-candles` and continuity-audits OpenAPI gaps
  enforceable by adding explicit schema-parity scenarios.

**Non-Goals:**

- No change to storage, backfill, recovery, or readiness algorithms.
- No change to how `canonical_market_data_hash` is computed.
- No pagination, cursoring, streaming, or new query/body parameters beyond
  what already exists.
- No new capability directory and no restoration of the removed
  `consumer-read-api-v1`/`historical-backtest-read-contract-v1`
  capabilities. All changes land as a `MODIFIED` delta to the existing
  `market-data-read-contracts` capability.
- No `400` status code anywhere in this capability. The baseline
  deliberately collapses malformed-request and range/alignment/bounds
  failures onto `422`; this change does not reopen that decision.
- No architectural refactor of the router/handler module boundaries beyond
  what unifying the error envelope and hash validation requires.
- This is a proposal-only OpenSpec change: no source, test, or OpenAPI
  document edits are made here.

## Decisions

### 1. Modify `market-data-read-contracts` in place; no new/restored capability

The task explicitly rules out recreating `consumer-read-api-v1` or
`historical-backtest-read-contract-v1` (both deliberately removed by the
baseline replacement) and rules out a duplicate capability for the
bounds/continuity-audits routes (which the baseline already folded into
`market-data-read-contracts`, unlike the pre-baseline state where no live
spec covered them at all). All deltas in this change therefore target the
single existing `market-data-read-contracts/spec.md` file with `MODIFIED
Requirements` blocks.

### 2. Keep the baseline's `422`-only mapping; do not reintroduce `400`

The baseline's "Public read errors use one stable envelope" requirement
already states: "Validation, malformed-body, invalid-range, alignment, and
bounds failures SHALL use HTTP `422` with a typed error code." This
collapses what an earlier draft treated as a `400`/`422` split (malformed
vs. semantic-only) onto a single `422` status for every non-lookup,
non-readiness, non-invariant client-request problem. This change keeps
that decision as given and only adds the missing code-name pins
(`invalid_request` for the malformed-request case specifically, as
distinct from `range_not_aligned` and `range_out_of_bounds`, which the
baseline already names) rather than re-litigating status codes.

### 3. `configured_stream_not_found` replaces `stream_not_found` on bounds/continuity-audits

The baseline's stable-envelope requirement already says "Configured-stream
lookup failures SHALL use HTTP `404` with error code
`configured_stream_not_found`" without scoping that sentence to any one
endpoint — by the capability's own Purpose statement ("canonical candle
consumption, history planning/provenance, and hash-bound historical
reads"), this already covers all four routes. The running
`HistoryPlanningHttpHandler` currently emits `stream_not_found` instead.
This change adds an explicit cross-endpoint scenario removing any doubt
that the single code applies uniformly, since the ambiguity — not the
mapping decision itself — was the actual gap.

### 4. Malformed-hash format check runs before the stale-coverage comparison

`canonical_market_data_hash` always produces a fixed-length lowercase hex
digest, so its shape is checkable without a storage read. Running the
format check first, and only running the recomputed-hash comparison
(and hence only reaching `coverage_stale`) once the format is known-good,
is what makes the two outcomes distinguishable — the baseline already
mandates the outcome ("malformed hashes SHALL be rejected as ... `422
invalid_request` rather than treated as a stale-but-valid provenance
value"), but did not previously state the ordering. This change makes the
ordering explicit since it is the mechanism that produces the required
distinction; the underlying single-branch string comparison in
`GetHistoricalCandleRange.execute` cannot otherwise tell the two cases
apart.

### 5. Retire `start_time_ms`/`end_time_ms` with an explicit rejection scenario, not just an absence of support

The baseline's "Explicit continuity audit has one canonical request shape"
requirement already forbids a second request contract, but has no scenario
describing what happens when a client sends the legacy shape anyway — the
running handler currently accepts it silently. This change adds a scenario
pinning the outcome to `422 invalid_request` in the common envelope, so the
requirement is testable rather than only aspirational. Three test
scenarios in `tests/test_research_history_integration_http.py` currently
exercise the legacy shape as a success path and will need to move to
`from_ms`/`to_ms` in the implementation change (see `tasks.md`).

## Risks / Trade-offs

- **Retiring `start_time_ms`/`end_time_ms`** is a breaking change for any
  caller currently using that shape. If an external consumer is known to
  depend on the legacy names, this decision should be revisited before
  implementation — but the baseline capability spec already forbids the
  legacy shape as a parallel contract, so this change only makes an
  already-decided removal enforceable, it does not newly decide to remove
  it.
- **Changing `InvalidRange`'s code from `invalid_range` to
  `invalid_request`** and **`UnknownStreamError`'s code from
  `stream_not_found` to `configured_stream_not_found`** are wire-visible
  changes for any consumer coded against today's running (non-compliant)
  codes. Both are corrections toward the already-stated baseline contract,
  not new decisions.
- **Adding hash-format validation** introduces one new, narrow validation
  branch ahead of the existing stale-hash comparison. This is pure input
  validation, not a change to hash computation or stale-coverage business
  logic, and only rejects inputs that could never have matched anyway.
