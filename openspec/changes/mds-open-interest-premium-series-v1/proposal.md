# MDS open interest and premium index series v1

## Status
DRAFT for the owner's decision. No code until the owner's word.

## Why
Vote W8 variant 6 (EMA200) has ten voters that read market data other than candles: nine read Bybit open interest (OI,
15-minute points: W8_30, W8_14, W8r, W8p, O1, kI, W1r, W1d, W2d) and one reads the Bybit premium index (5-minute kline: PS_R).
MDS stores and serves only trade candles today, so the Engine cannot compute them, neither in research nor live.

The Engine already reads its candles from MDS itself (`strategy_engine/adapters/market_data_service/client.py`) for live entry,
range evaluation and batches. Research Service and Runtime never hand candles to the Engine. So once MDS serves the two series,
the Engine can read them the same way, and Research Service and Runtime need no change.

## What changes
1. **Configuration.** An instrument MAY declare `market_series = ["open_interest_15m", "premium_index_5m"]` next to
   `canonical_timeframes`. Default: none. Only BTC, ETH and SOL need them now.
2. **Storage.** New canonical tables for series points, one row per `(series stream, time)`, decimal text values, source and
   commit time; one `stream_state` row per series stream (same lifecycle as candle streams: bootstrapping, ready, degraded,
   failed). Schema v3 with migration from v2; candle tables unchanged.
3. **History.** Full available history from Bybit REST:
   - OI: `GET /v5/market/open-interest` (`intervalTime=15min`, 200 points per page, cursor paging);
   - premium: `GET /v5/market/premium-index-price-kline` (`interval=5`, 1000 bars per page), stored as close (open, high, low
     kept for audit).
   Same lower-bound discovery, bounded windows, resumable progress and gap audit as candles.
4. **Live.** REST poll after each bucket closes (OI every 15 min, premium every 5 min, plus grace). No WebSocket: Bybit has no
   premium-kline stream, and the WebSocket ticker OI is a running snapshot, not the 15-minute point research used.
5. **Read API.** `GET /v1/series?ticker=&series=open_interest_15m|premium_index_5m&from_ms=&to_ms=` (ready-only, half-open
   grid, exact decimals, provenance hash) and `GET /v1/series-streams/{ticker}/{series}/bounds`; OpenAPI updated.
6. **Timing for live bars.** For an instrument with series, the committed 5m bar notification to Runtime is sent only once the
   series point(s) that bar may use (as-of rule of the Engine OpenSpec `w8-open-interest-premium-v1`) are committed, waiting at
   most `MDS_SERIES_NOTIFY_WAIT_SECONDS` (default 20). After the wait the notification goes out anyway and the log says which
   series is late; the Engine then fails closed only for strategies that read that series.

## Unchanged
Candle ingestion, candle read contracts, notifications for instruments without series, repair of candle streams.

## Out of scope
- Engine side: reading the series from MDS, as-of alignment and the voters (Engine thread, OpenSpec `w8-open-interest-premium-v1`).
- Funding rate and long/short ratio (research found them noise).
- bbb_stack: `markets.toml` lines for BTC/ETH/SOL and the image pin (separate PR on the owner's word).
