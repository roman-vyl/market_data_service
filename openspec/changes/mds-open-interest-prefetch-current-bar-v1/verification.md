# Post-deployment verification — 2026-11-06

## Observation window and provenance

Selected window: 2026-11-03 19:15:00 through 2026-11-04 19:15:00 UTC (22:15–22:15 Moscow), after current-bar prefetch was deployed. Sources are saved timestamped logs from a97fd0e and 408a1f5 and a read-only query of production SQLite. This window predates the lifecycle sweep optimization in PR #28.

The container replacement between those log sources leaves about 95 seconds without log lines near 17:26 UTC. This interval contains no missing expected prefetch point: all 576 identities on the 24-hour grid are logged and present in SQLite. General warning counters below cover available logs; they cannot exclude unlogged events in that interval.

## Prefetch and publication

- BTCUSDT.P:5m and SOLUSDT.P:5m: 288 expected points each, all 576 prefetched and stored. Every stored committed_at_ms is earlier than its bar close.
- 531/576 points (92.2%) were prefetched 20–35 seconds after open. The slowest was 177.3 seconds, still before close.
- Attempt counts: 536 points on attempt 1, 32 on attempt 2, 6 on attempt 3, 2 on attempt 4.
- No bar closed without its prefetched point in this sample.
- Five `bar not published` lines, all with open_interest_stored=True and recovery_pending=True. This is 0.208/hour versus the design baseline of 59/26 = 2.269/hour, a 90.8% reduction. These lines do not establish subsequent Runtime delivery.
- ERROR lines: zero. There were 53 event-loop lag warnings (maximum 6.08 seconds), eight WS disconnects including seven ping timeouts. Count timeout episodes from disconnect lines, not the duplicate transport-error text.
- For context, the previously audited approximately 62-hour baseline had 253 lag warnings (about 4.08/hour) and 62 disconnects (1/hour); this sample has 2.21 lag/hour and 0.333 disconnect/hour. Different network conditions and intervening SQLite changes prevent attributing these differences to prefetch. No direct end-to-end WS or Runtime latency measurement was added.

## Late historical value comparison

Four read-only GET requests to Bybit `/v5/market/open-interest` completed at 2026-11-06 14:10:44–14:10:46 UTC. Parameters: category=linear, intervalTime=5min, symbol=BTCUSDT or SOLUSDT. For each symbol, the two inclusive timestamp ranges were:

| startTime | endTime | limit | returned |
| --- | --- | --- | --- |
| 1793733300000 | 1793793000000 | 200 | 200 |
| 1793793300000 | 1793819400000 | 88 | 88 |

All responses had retCode=0. Compare by stream plus timestamp, using Python Decimal equality rather than floating point.

| Stream | Compared | Equal | Missing | Different |
| --- | --- | --- | --- | --- |
| BTCUSDT.P:5m | 288 | 288 | 0 | 0 |
| SOLUSDT.P:5m | 288 | 288 | 0 | 0 |
| Total | 576 | 576 | 0 | 0 |

This confirms the sampled prefetched values equal the later historical values; it does not prove immutability for every future Bybit point.

Local raw artifacts (temporary, not repository dependencies):
- `/tmp/oi-prefetch-history-responses.json`, SHA-256 `1ba61857c5a770eb05ff527f416f96327264c0422f92560f3d6a6884a1a9b8b2`.
- `/tmp/oi-prefetch-history-comparison.json`, SHA-256 `8da6406b1b1c7a9d466b8d206a0a493d406c9a7eafcbe03374232ffa3dd760f5`.

## Commit-time request check and remaining boundaries

Production HTTP requests are not individually logged, so absence of a network call at commit is established through the existing code path and regression test, not inferred from silence in production logs. AppendLiveOpenInterest.execute returns immediately when the point exists, before invoking the importer. All 576 sampled points existed before close. `test_real_parser_prefetch_stores_the_running_bar_and_commit_makes_no_request` exercises this path with the actual Bybit parser and SQLite and asserts that the transport call count does not increase at commit.

The requested post-deployment verification is complete for the sampled prefetch behavior. Residual event-loop pauses, readiness-held notifications and their eventual delivery remain separate diagnostics. No container, database, image pin or runtime configuration was changed during this check.

## Repository validation

OpenSpec strict validation, Ruff, mypy and diff whitespace checks passed. The full make verify run had 428 passing tests and one known intermittent ConnectionResetError in test_malformed_success_body_raises_delivery_error. That exact HTTP test passed on isolated rerun, together with the commit-time no-request regression test.
