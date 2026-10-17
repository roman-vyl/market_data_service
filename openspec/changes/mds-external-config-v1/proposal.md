## Why

The MDS instrument list (`config/markets.toml`) is baked into the image. In the live
stack's deploy mode the repo-checkout config mount is dropped, so changing the list takes
four steps:
- a PR to this repo;
- an image build;
- a pin in bbb_stack;
- recreating the container.

On 2026-10-17 that loop ran three times in one afternoon.

Other services already keep their operator-owned state under `BBB_DATA_ROOT` on the host,
for example the Strategy Runtime catalog at `${BBB_DATA_ROOT}/strategy-runtime/specs`. The
owner wants the same for MDS: a config file under `BBB_DATA_ROOT` that they edit by hand,
which MDS reads and picks up by itself. No deploys and no pins.

## What Changes

- **External config file.** In the live stack MDS reads its market configuration from the
  host file `${BBB_DATA_ROOT}/mds/markets.toml`, mounted read-only (directory mount
  `${BBB_DATA_ROOT}/mds` → `/app/config-host`), through the existing
  `MDS_MARKETS_CONFIG_PATH`. The file is seeded once with the config of the currently
  pinned image (BTC/ETH enabled, nine tickers with `enabled = false`); a `README.md` sits
  next to it. The baked `config/markets.toml` stays as the default for tests and
  standalone runs.
- **Pickup without deploy.** A new setting `MDS_CONFIG_WATCH_SECONDS` (CLI
  `--config-watch-seconds`):
  - `0` (default) keeps today's behaviour;
  - a positive value makes `serve` check the file at that interval and validate any
    changed content with the startup rules.

  Outcomes of a check:
  - **Valid, and the enabled coverage changed:** MDS logs the added, removed and changed
    streams, stops through the same graceful path as SIGTERM and exits with code `75`.
    `restart: unless-stopped` brings it back with the new config.
  - **Valid, no coverage change** (comments, formatting): MDS keeps running.
  - **Invalid** (unreadable, missing, bad TOML, failed validation): MDS logs one ERROR and
    keeps running on the current config.
- **Stack wiring.** A small bbb_stack compose PR, listed as a task here:
  - the `/app/config-host` mount in base and deploy mode;
  - `MDS_MARKETS_CONFIG_PATH=/app/config-host/markets.toml`;
  - `MDS_CONFIG_WATCH_SECONDS=10`;
  - the matching line in the `bbb-live-stack` spec's Data Mount Layout.

## Non-Goals

- No hot add or remove of streams inside a running process. Every applied change is a
  process restart: streams are `connecting` for about a minute (measured 2026-10-17:
  about 48 s to BTC 5m `ready`).
- No change to the config schema, validation rules, storage or read API.
- No change to other services.
- No fix of the WebSocket drops seen while nine tickers backfill; that is separate.
- No MDS restart while the owner's smoke test runs. Apply waits for the owner's go.
