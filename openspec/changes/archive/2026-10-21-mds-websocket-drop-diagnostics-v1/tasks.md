## 1. Implementation
- [x] 1.1 Log connection lifecycle events with close code and reason
- [x] 1.2 Close-code fallback to the sent frame in the transport
- [x] 1.3 Event-loop lag monitor and settings `MDS_EVENT_LOOP_LAG_WARN_SECONDS`
- [x] 1.4 Configurable keepalive pong timeout `MDS_WS_PING_TIMEOUT_SECONDS` (default 60)
- [x] 1.5 Tests; `make verify` green
