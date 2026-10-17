## 1. Settings

- [ ] 1.1 `RuntimeSettings.config_watch_seconds: float = 0`, read from
  `MDS_CONFIG_WATCH_SECONDS`, with CLI `--config-watch-seconds`. Negative values are
  rejected.

## 2. Watcher

- [ ] 2.1 `runtime/config_watch.py`, `ConfigWatcher(path, running_config, interval, stop_event, logger)`:
  - polls a SHA-256 of the file's bytes;
  - on a change, loads the file with `load_market_config`;
  - compares the enabled coverage (ticker, exchange symbol, enabled, timeframes, history
    policy) with the running config;
  - decides one of `restart`, `unchanged` or `invalid`, with one log line per decision.
- [ ] 2.2 On `restart` the watcher records the reason and sets `stop_event`. On `invalid`
  it remembers the bad hash and stays quiet until the hash changes again.
- [ ] 2.3 `serve`: when `config_watch_seconds > 0`, run the watcher in the same task group
  as realtime and the historical worker. After a stop caused by the watcher, return exit
  code `75`; every other stop returns as today.

## 3. Tests

- [ ] 3.1 A changed coverage (one ticker flipped to `enabled = false`) leads to `restart`,
  `stop_event` is set, and the INFO line names the removed streams.
- [ ] 3.2 A comment-only edit leads to `unchanged`, with no stop.
- [ ] 3.3 Invalid TOML, an unknown timeframe or a duplicate ticker leads to `invalid`, with
  no stop and one ERROR line. A second check of the same bad content logs nothing. A later
  valid edit is acted on.
- [ ] 3.4 A missing file during watch counts as `invalid` and does not stop MDS.
- [ ] 3.5 `serve` returns `75` after a watcher stop and `0` after a signal stop
  (fake runtime).
- [ ] 3.6 `MDS_CONFIG_WATCH_SECONDS` unset or `0`: no watcher task is created.

## 4. Docs

- [ ] 4.1 README "Docker config behavior": the watch setting, the exit code `75`, and that
  an applied edit means about a minute of `connecting`.

## 5. Acceptance on the Mac (after the bbb_stack mount change, only when the owner allows an MDS restart)

- [ ] 5.1 With the pinned image and `MDS_CONFIG_WATCH_SECONDS=10`, add a comment line to
  `${BBB_DATA_ROOT}/mds/markets.toml`: one `unchanged` INFO line and no restart.
- [ ] 5.2 Break the file (invalid TOML): one ERROR line, no restart, BTC/ETH stay `ready`.
  Then restore the file.
- [ ] 5.3 A real coverage change chosen by the owner leads to one INFO line, exit `75`, an
  automatic restart, and BTC/ETH `ready` again within 5 minutes.
