## Why

The live stack is moving the instrument list out of the image into a host file the owner
edits by hand (bbb_stack `bbb-mds-host-config-mount-v1`:
`${BBB_DATA_ROOT}/mds/markets.toml`, mounted read-only at `/app/config-host`).

Today MDS reads `markets.toml` only at startup, so an edit needs a manual restart. The
owner wants MDS to pick up an edit by itself, with no deploys, no pins and no commands.

## What Changes

- A new setting `MDS_CONFIG_WATCH_SECONDS` (CLI `--config-watch-seconds`):
  - `0` (default) keeps today's behaviour;
  - a positive value makes `serve` check the config file at that interval.
- When the file's content changes, MDS loads and validates it with the same rules as at
  startup (`load_market_config`). Then:
  - **Valid, and the enabled coverage differs** from the running one (instruments,
    exchange symbols, enabled flags, timeframes, history policy): MDS logs one INFO line
    naming the added, removed and changed streams. It then stops the same way it stops on
    SIGTERM and exits with a dedicated code `75` ("config changed, restart me").
    `restart: unless-stopped` in the stack brings it back with the new config.
  - **Valid, but no coverage change** (comments, formatting): MDS logs one INFO line and
    keeps running.
  - **Invalid** (unreadable, bad TOML, failed validation): MDS logs one ERROR line with
    the loader message, keeps running on the current config, and does not check that
    content again until the file changes once more.
- Exchange metadata verification still happens at the next startup, as today. A ticker
  Bybit rejects fails there, exactly as it would with the image config.

## Non-Goals

- No hot add or remove of streams inside a running process. Every applied change is a
  process restart; streams are `connecting` for about a minute.
- No change to the config schema, validation rules, storage or read API.
- No compose change here: that is the bbb_stack change.
- No WebSocket drop fix: separate work.
