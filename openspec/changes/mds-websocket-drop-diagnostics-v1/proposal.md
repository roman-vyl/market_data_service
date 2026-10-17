# MDS websocket drop diagnostics v1

## Why
With 7-8 coins and parallel 5m backfills the public websocket dropped every 7-25 minutes and BTC/ETH left `ready` for 16-70 s. The close code was never logged, so the cause is unproven. The leading hypothesis is a blocked event loop (synchronous SQLite work) that misses the websocket keepalive pong within 20 s.

## What changes
- Log websocket connect, close code/reason, transport failure and reconnect exhaustion.
- Fall back to the locally sent close frame when no frame was received, so the code is never lost.
- Add an event-loop lag monitor that warns when the loop was blocked longer than a threshold.
- Raise the websocket keepalive pong timeout from 20 s to a configurable 60 s (`MDS_WS_PING_TIMEOUT_SECONDS`), well inside Bybit's 10 minute idle limit.

## Out of scope
Moving DB work off the event loop and a rule for malformed exchange candles: separate changes once the logs show the cause.
