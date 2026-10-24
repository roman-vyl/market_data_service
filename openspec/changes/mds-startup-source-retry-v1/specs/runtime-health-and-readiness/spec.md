## ADDED Requirements

### Requirement: Startup tolerates transient exchange failures
Startup instrument metadata verification SHALL retry recoverable source failures (network, TLS, HTTP 5xx, rate limit) with
bounded backoff for a configured maximum wait before giving up. A metadata mismatch SHALL still stop startup immediately.

#### Scenario: One TLS timeout at start
- **WHEN** the first `instruments-info` request for an instrument fails with a TLS timeout and the next one succeeds
- **THEN** MDS starts normally and logs one recoverable failure.

#### Scenario: Wrong symbol
- **WHEN** Bybit reports that a configured instrument is not a trading USDT linear perpetual
- **THEN** startup stops at once with the mismatch, without retrying.
