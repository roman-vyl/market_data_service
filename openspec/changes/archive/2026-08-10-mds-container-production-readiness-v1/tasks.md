# Tasks: MDS Container Production Readiness v1

## 1. Image runtime boundary

- [x] 1.1 Add a dedicated stable non-root UID/GID to the Docker image and run
      `market-data-service serve` as that identity.
- [x] 1.2 Keep packaged application/config paths root-owned and non-writable by
      the runtime user; make `/data` writable by the runtime UID/GID.
- [x] 1.3 Preserve the exec-form service command as PID1 without adding a
      shell or supervisor entrypoint.
- [x] 1.4 Add an image-level Docker healthcheck that calls only the existing
      `http://127.0.0.1:8080/health` endpoint with bounded timeout/retries and
      no new runtime dependency solely for probing.

## 2. Standalone Compose contract

- [x] 2.1 Change the published port to exactly
      `127.0.0.1:8080:8080`.
- [x] 2.2 Preserve `${BBB_DATA_ROOT}/market-data:/data` exactly and retain the
      existing read-only operator config mount.
- [x] 2.3 Run the standalone service with a read-only root filesystem and
      `/data` as its only writable persistent mount.
- [x] 2.4 Document the stable runtime UID/GID and the requirement that the
      host-side `${BBB_DATA_ROOT}/market-data` directory be writable by it.

## 3. Build and image hygiene

- [x] 3.1 Complete `.dockerignore` coverage for repository databases and
      sidecars, Git data, virtual environments, Python/tool caches, build
      outputs, archives, snapshots/backups, and host-local metadata.
- [x] 3.2 Add a built-image inspection test proving excluded repository and
      development artifacts are absent while the installed package and
      intended default config remain present.
- [x] 3.3 Add static/container assertions that the final image user is
      non-root, application/config paths are not runtime-writable, `/data` is
      writable, and the service command remains direct PID1.

## 4. Container lifecycle smoke coverage

- [x] 4.1 Start Compose against a temporary `BBB_DATA_ROOT`, wait for Docker
      health, and assert the host-visible SQLite database and non-root process.
- [x] 4.2 Restart the same container, wait for health to return, and prove
      stable SQLite schema/stream evidence is preserved.
- [x] 4.3 Remove and recreate the container without deleting the host data
      directory, wait for health to return, and prove the same SQLite evidence
      is preserved.
- [x] 4.4 Verify Docker SIGTERM and SIGINT reach the existing runtime shutdown
      path, with no shell/supervisor interception and no duplicate internal
      graceful-shutdown contract.

## 5. Regression and documentation verification

- [x] 5.1 Keep runtime entrypoint/settings and committed-bar webhook behavior
      unchanged; run their existing regression tests as part of verification.
- [x] 5.2 Update only standalone-container operator documentation and the
      container entries in the acceptance matrix where necessary.
- [x] 5.3 Run the container smoke suite and `make verify` for the later apply
      change.

## 6. Correction pass

- [x] 6.1 Make the image healthcheck use effective `MDS_HTTP_PORT` with an
      `8080` default and prove a non-default container port works.
- [x] 6.2 Replace schema/stream-only restart evidence with real candle rows and
      monotonic per-stream durable progress checks across restart/recreate.
- [x] 6.3 Set standalone Compose `stop_grace_period: 20s` and exercise graceful
      stop using that configured policy without a test-only timeout override.
- [x] 6.4 Cover one-time migration of an existing external SQLite directory to
      UID/GID `10001:10001` using `0750` directories and `0640` files, including
      WAL/SHM writes by the non-root service.
- [x] 6.5 Run deterministic container smoke, `make verify`, strict OpenSpec
      validation, and the separate bounded real-Bybit regression against the
      existing external MDS data root.
