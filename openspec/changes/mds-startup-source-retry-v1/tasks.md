## 1. Transport
- [ ] 1.1 Wrap `OSError` (incl. `ssl.SSLError`, `ConnectionResetError`) and `http.client.HTTPException` raised by `urlopen` or the body read into `BybitHttpError`
- [ ] 1.2 Tests: each error type becomes `BybitHttpError` and is retried by the REST client

## 2. Startup
- [ ] 2.1 Startup metadata verification retries recoverable failures with backoff up to `MDS_STARTUP_METADATA_MAX_WAIT_SECONDS`; mismatch is still immediate
- [ ] 2.2 Setting, validation, README
- [ ] 2.3 Tests with fake clock: one TLS failure then success starts normally; mismatch stops at once; endless failure exits after the limit
