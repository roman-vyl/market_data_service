## 1. MDS settings and watcher (this repo)

- [ ] 1.1 `RuntimeSettings.config_watch_seconds: float = 0`, read from
  `MDS_CONFIG_WATCH_SECONDS`, with CLI `--config-watch-seconds`. Negative values are
  rejected.
- [ ] 1.2 `runtime/config_watch.py`, `ConfigWatcher(path, running_config, interval, stop_event, logger)`:
  - polls a SHA-256 of the file's bytes;
  - on a change, loads it with `load_market_config`;
  - compares the enabled coverage with the running config;
  - decides one of `restart`, `unchanged` or `invalid`, with one log line per decision.
- [ ] 1.3 On `restart` the watcher records the reason and sets `stop_event`. On `invalid`
  it remembers the bad hash and stays quiet until the hash changes.
- [ ] 1.4 `serve`: when `config_watch_seconds > 0`, run the watcher in the runtime task
  group. After a stop caused by the watcher, return `75`; every other stop returns as
  today.
- [ ] 1.5 README "Docker config behavior":
  - the external config file;
  - the watch setting;
  - exit code `75`;
  - an applied edit means about a minute of `connecting`.

## 2. Tests (this repo)

- [ ] 2.1 A ticker flipped to `enabled = false` leads to `restart`, `stop_event` is set,
  and the INFO line names the removed streams.
- [ ] 2.2 A comment-only edit leads to `unchanged`, with no stop.
- [ ] 2.3 Invalid TOML, an unknown timeframe or a duplicate ticker leads to `invalid`,
  with no stop and one ERROR line. Unchanged bad content logs nothing more. A later valid
  edit is acted on.
- [ ] 2.4 A missing file during watch counts as `invalid`, with no stop.
- [ ] 2.5 `serve` returns `75` after a watcher stop and `0` after a signal stop
  (fake runtime).
- [ ] 2.6 With the setting unset or `0`, no watcher task is created.

## 3. Image and stack wiring (bbb_stack, after 1–2 are merged)

- [ ] 3.1 Pin the new MDS image in `deploy/images.env`.
- [ ] 3.2 Compose, `market-data-service`, in `docker-compose.yml` and in the
  `docker-compose.deploy.yml` `volumes: !override`:
  - mount `${BBB_DATA_ROOT}/mds` → `/app/config-host` read-only, keeping the market-data
    mount;
  - set `MDS_MARKETS_CONFIG_PATH: "/app/config-host/markets.toml"`;
  - set `MDS_CONFIG_WATCH_SECONDS: "10"`.

  `docker compose config --quiet` passes in base and deploy mode.
- [ ] 3.3 `bbb-live-stack` spec, Data Mount Layout: the MDS config line becomes
  `${BBB_DATA_ROOT}/mds` → `/app/config-host` read-only.

## 4. Host file (owner's Mac)

- [ ] 4.1 `${BBB_DATA_ROOT}/mds/markets.toml`, byte-identical to the pinned image's
  `/app/config/markets.toml`.
- [ ] 4.2 `${BBB_DATA_ROOT}/mds/README.md` covers:
  - what the file controls;
  - how to enable or disable a ticker;
  - that disabling keeps stored history;
  - that MDS restarts itself on a valid edit (about a minute of `connecting`);
  - where to see errors (`docker logs bbb_stack-market-data-service-1`).

## 5. Apply and acceptance (only when the owner allows an MDS restart)

- [ ] 5.1 Recreate only `market-data-service`. `docker inspect` shows `/app/config-host`
  with `RW: false`, and the startup log shows the same enabled streams as before.
  BTC/ETH reach `ready` within 5 minutes.
- [ ] 5.2 Add a comment line to the host file: one `unchanged` line, no restart.
- [ ] 5.3 Break the file (invalid TOML): one ERROR line, no restart, BTC/ETH stay
  `ready`. Then restore the file.
- [ ] 5.4 A coverage change chosen by the owner: one INFO line, exit `75`, an automatic
  restart, and BTC/ETH `ready` again within 5 minutes.
