# MDS startup survives a transient Bybit failure v1

## Status
DRAFT for the owner's decision. No code until the owner's word.

## Why
On 2026-10-20 10:21:30Z MDS on the Mac crashed at start: one Bybit `instruments-info` call hit a TLS timeout, the process
exited and Docker restarted it. Two causes in the code:

1. `UrllibJsonHttpTransport.get_json` wraps only `HTTPError`, `URLError` and `TimeoutError` into `BybitHttpError`. A TLS or
   socket error raised while the response body is read (`ssl.SSLError`, `ConnectionResetError`,
   `http.client.RemoteDisconnected`, `http.client.IncompleteRead`) escapes unwrapped, so the client's 4-attempt retry does not
   apply: one failure is final.
2. `entrypoints/serve.py` verifies every configured instrument against `instruments-info` before the HTTP server starts. Any
   exception there ends the process, even a recoverable one after all retries.

## What changes
1. The transport turns every network or TLS error (any `OSError` and `http.client.HTTPException`) into `BybitHttpError`, which is
   already recoverable and retried with backoff.
2. Startup verification retries a recoverable failure with backoff (5 s, doubling, at most 60 s) for up to
   `MDS_STARTUP_METADATA_MAX_WAIT_SECONDS` (default 600), logging each failure on one line. A metadata mismatch (wrong symbol,
   not trading) still stops the start at once, as today.
3. If the wait runs out, the process exits with a clear log line (Docker restarts it as today).

## Unchanged
Metadata rules, retry counts for candles, readiness and health semantics.
