# Proposal: MDS Container Production Readiness v1

## Why

MDS already has a useful standalone Docker baseline. The image launches
`market-data-service serve` with an exec-form `CMD`, stores SQLite at
`/data/market.sqlite3`, packages a default market config, and Compose preserves
the external `${BBB_DATA_ROOT}/market-data:/data` bind mount. The runtime
already exposes `GET /health` and owns SIGINT/SIGTERM shutdown behavior through
the canonical `runtime-health-and-readiness` capability. `.dockerignore` and
the Dockerfile's explicit `COPY` set already exclude many development files.

The remaining gap is a container/deployment contract. The image still runs as
root, normal operation is not verified against a read-only application
filesystem, Compose publishes HTTP on every host interface, the image has no
Docker healthcheck, and no container smoke test proves that restart and
recreation preserve the external SQLite database. Build hygiene is configured
but not verified against the resulting image.

This proposal defines the smallest standalone-container hardening needed to
close those gaps. It is proposal-only; it does not apply the implementation.

## What Changes

- Add a new `container-production-runtime` capability that owns only the MDS
  container and standalone deployment contract.
- Run the image's application process as a stable non-root user while keeping
  `/data` writable for SQLite.
- Make normal service operation independent of writes to packaged application
  and config paths; keep `/data` as the only writable persistent mount.
- Preserve direct exec/PID1 launch so Docker SIGTERM and SIGINT reach the
  shutdown path already specified by `runtime-health-and-readiness`.
- Define an image-level Docker healthcheck against the existing `GET /health`
  endpoint on the effective `MDS_HTTP_PORT` (default `8080`), without creating
  or reinterpreting a health/readiness API.
- Publish standalone Compose HTTP only as `127.0.0.1:8080:8080`.
- Preserve `${BBB_DATA_ROOT}/market-data:/data` exactly as the external
  persistence contract.
- Add container smoke coverage for restart and recreation against the same
  host directory, including monotonic preservation of real candle/progress
  evidence and return to healthy state.
- Define a 20-second standalone Compose stop grace period and verify signal
  shutdown against that production policy.
- Verify the documented one-time ownership migration for an existing external
  SQLite directory without world-writable production permissions.
- Verify both build context and built-image hygiene so repository databases,
  Git metadata, caches, virtual environments, archives, backups, and other
  runtime-unnecessary artifacts do not enter the image.

## Capabilities

### New Capabilities

- `container-production-runtime`: non-root execution, application/config
  filesystem immutability, external `/data` persistence, direct PID1 signal
  delivery, Docker healthcheck reuse, localhost-only standalone publishing,
  restart/recreate persistence, and image/build hygiene.

### Modified Capabilities

- None. In particular, `runtime-health-and-readiness` remains the canonical
  owner of process health, readiness, and graceful shutdown behavior. This
  change only requires the container boundary to expose `/health` to Docker and
  deliver Docker signals to that existing runtime path.

## Non-Goals

- No observability, metrics, tracing, or OpenTelemetry work.
- No SQLite backup, checkpoint, synchronous, WAL, or other database tuning.
- No fault injection and no recovery/readiness redesign.
- No general multi-service Compose and no Dockerization of other services.
- No new health/readiness endpoint, response shape, or semantic shortcut.
- No committed-bar webhook configuration, default, validation, composition,
  delivery, or shutdown behavior change.
- No ingestion, lifecycle, market configuration, or public API redesign.

## Impact

The later implementation is expected to stay within `Dockerfile`,
`docker-compose.yml`, `.dockerignore`, container-focused tests/scripts, and
operator documentation. Runtime entrypoints, settings, health/readiness
handlers, storage semantics, and webhook code should remain unchanged unless a
test exposes a strictly container-blocking defect that requires a separately
approved specification update.
