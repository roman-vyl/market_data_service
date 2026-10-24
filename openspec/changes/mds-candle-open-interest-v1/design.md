# Design

## D1. OI is a field of the candle row
`open_interest_value TEXT NULL` on `candles`. The candle row is created by the existing ingestion exactly as today; OI is
written into the existing row by one use case `AttachOpenInterest(stream, window)`:
fetch Bybit 5-minute OI for the window -> validate (timestamp on the 5m grid, decimal >= 0) -> in one transaction write the
values into existing candle rows and move `oi_latest_open_time_ms` over the contiguous filled prefix. OHLCV is never touched;
a point for a bar that has no candle row yet is not stored (it is fetched again later). The same use case serves history,
gap repair and live, so there is one path for OI.

## D2. Why not fetch OI inside the kline WebSocket path
The Bybit kline (REST and WebSocket) carries no OI; OI is a separate Bybit request. Putting that request inside the WebSocket
handler or the REST kline window would change their timing and failure behaviour, which the owner forbids. So candle commit
stays as is and OI is attached to the committed row.

## D3. Completeness and the notification
Today a committed realtime bar (or a restored recovery episode) enqueues the notification at once. For an OI instrument the
enqueue moves to the moment `AttachOpenInterest` has written that bar's OI. The payload, the single-in-flight sender and the
ordering stay the same. Candle commits keep their timing; only the "bar complete" signal of OI instruments waits for the OI
of the same bar. Runtime and the Engine then always read a bar together with its own OI, never a bar with an older OI.

## D4. History
`oi_latest_open_time_ms` starts empty. An OI-history pass (same budgeting and fairness as candle reconciliation, never in the
same transaction as a candle window) fills from the first 5m OI point Bybit has to the newest committed bar. A gap audit finds
bars with candle but without OI and repairs them through D1. Readiness of the candle stream does not depend on OI history; the
readiness document shows OI coverage separately.

## D5. When Bybit does not give the OI (owner choice)
- A (recommended): after the timeout the notification goes out with that bar's OI empty (null), and the bar is marked
  `oi_missing` in the log and readiness document. Consumers see null, never an older value. Gap repair fills it later.
- B: the notification waits until the OI arrives, however long. A Bybit OI outage then stops all bar notifications of that
  coin (MDS does not know which strategies use OI).

## D6. Timing measurement before code (task 0)
How many seconds after the 5m close Bybit publishes the 5-minute OI point is not known. One short Mac session polls
`open-interest` for BTC/ETH/SOL for 1 hour (36 bars) and records the delay. The timeout in D5 is set from it (for example
p99 delay + 10 s).

## D7. Hash
`market_data_hash` stays over OHLCV (unchanged contract, old experiments still read). `open_interest_hash` over
`(open_time_ms, open_interest)` is returned next to it.
