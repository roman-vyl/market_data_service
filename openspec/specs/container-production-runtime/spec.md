# container-production-runtime Specification

## Purpose
Defines the durable production-container and standalone deployment contract for
MDS, including non-root execution, filesystem and persistence boundaries,
Docker health, signal delivery, localhost exposure, restart/recreate
persistence, and image hygiene.
## Requirements
### Requirement: Production container runs as non-root

The production MDS image SHALL run `market-data-service serve` as a dedicated
runtime user with a stable non-zero numeric UID and GID.

#### Scenario: Service process has an unprivileged identity

- **WHEN** the production image or standalone Compose service starts MDS
- **THEN** the service process UID and GID are non-zero
- **AND** the process can read the installed application and selected market
  config
- **AND** the process can create and update SQLite files under `/data`

### Requirement: Application and config require no runtime writes

Normal operation of the production container SHALL NOT require writes to the
installed application, `/app`, or packaged/mounted config. `/data` SHALL remain
the only writable persistent mount in the standalone deployment.

#### Scenario: Service operates with a read-only application filesystem

- **WHEN** the standalone service is started with a read-only container root
  filesystem
- **THEN** startup, HTTP health serving, normal runtime work, and shutdown can
  proceed without writes to application or config paths
- **AND** the selected market config remains readable and non-writable by the
  runtime user
- **AND** SQLite database, WAL, and SHM files are written only below `/data`

#### Scenario: Existing external data contract is preserved

- **WHEN** the standalone Compose configuration is inspected
- **THEN** `${BBB_DATA_ROOT}/market-data` is mounted at `/data`
- **AND** `/data` is writable by the documented runtime UID/GID
- **AND** no second writable persistent mount is introduced

#### Scenario: Existing data directory is migrated without world-write access

- **WHEN** an existing external data directory and SQLite files are migrated to
  the documented runtime UID/GID before the hardened container starts
- **THEN** directories do not require permissions broader than `0750`
- **AND** database files do not require permissions broader than `0640`
- **AND** the non-root service can open the existing database and create or
  update its SQLite WAL and SHM sidecars under `/data`

### Requirement: Container preserves direct PID1 signal delivery

The production container SHALL launch the service directly as PID1 using exec
semantics. Docker SIGTERM and SIGINT SHALL reach the existing shutdown path
owned by the canonical `runtime-health-and-readiness` capability without a
shell, supervisor, or sidecar intercepting them.

#### Scenario: Service owns PID1

- **WHEN** the final image command and a running production container are
  inspected
- **THEN** `market-data-service serve` is the direct container process
- **AND** no shell wrapper or supervisor owns PID1 ahead of the service

#### Scenario: Docker signals reach the canonical shutdown path

- **WHEN** Docker sends SIGTERM or SIGINT to the running container
- **THEN** the signal is delivered to the service PID1 process
- **AND** the service exits through the shutdown behavior specified by
  `runtime-health-and-readiness`
- **AND** standalone Compose allows the configured `20s` production grace
  period before forced termination

### Requirement: Docker healthcheck reuses process health

The production image SHALL define a Docker healthcheck that requests the
existing container-local `GET /health` endpoint on the effective
`MDS_HTTP_PORT`, defaulting to `8080` when the variable is absent. It SHALL NOT
call `/readiness`, introduce another endpoint, or change process-health
semantics.

#### Scenario: Existing health response drives Docker health

- **WHEN** `GET http://127.0.0.1:<effective-MDS_HTTP_PORT>/health` returns HTTP
  200 within the configured probe timeout
- **THEN** the Docker healthcheck succeeds
- **AND** a connection failure, timeout, or non-200 response makes the probe
  fail
- **AND** the result is independent of `GET /readiness`

### Requirement: Standalone HTTP publishing is localhost-only

The standalone Compose service SHALL publish MDS HTTP only on the host loopback
interface.

#### Scenario: Compose exposes no all-interface HTTP binding

- **WHEN** `docker-compose.yml` is inspected
- **THEN** the MDS port mapping is exactly `127.0.0.1:8080:8080`
- **AND** no MDS HTTP port is published on `0.0.0.0` or all host interfaces

### Requirement: Restart and recreation preserve external SQLite

Restarting or recreating the standalone container against the same
`${BBB_DATA_ROOT}/market-data` directory SHALL preserve its SQLite database and
allow process health to return without bypassing existing runtime
reconciliation/readiness rules.

#### Scenario: Container restart preserves data and restores health

- **WHEN** a running standalone service has durable SQLite state under `/data`
- **AND** the container is restarted without deleting the host data directory
- **THEN** the same host-visible database remains present
- **AND** every previously recorded candle remains present with unchanged
  canonical values
- **AND** per-stream candle counts and durable progress are not lower than
  before restart, while normal additional progress is allowed
- **AND** Docker health eventually returns to healthy through `GET /health`

#### Scenario: Container recreation preserves data and restores health

- **WHEN** the standalone container is removed and recreated without deleting
  `${BBB_DATA_ROOT}/market-data`
- **THEN** the recreated container opens the same host-visible database
- **AND** every previously recorded candle remains present with unchanged
  canonical values
- **AND** per-stream candle counts and durable progress are not lower than
  before recreation, while normal additional progress is allowed
- **AND** Docker health eventually returns to healthy through `GET /health`
- **AND** readiness remains governed by existing restart reconciliation rules

### Requirement: Build context and image contain only runtime inputs

The production build SHALL prevent repository-local and development artifacts
that are unnecessary at runtime from entering the Docker build context or final
image.

#### Scenario: Development artifacts are absent from the built image

- **WHEN** the production image is built from the repository
- **THEN** the image contains no repository Git metadata
- **AND** it contains no repository SQLite database, WAL/SHM sidecar, or local
  `data/` content
- **AND** it contains no local virtual environment, Python/tool cache, build
  output, archive, snapshot, backup, or host OS metadata
- **AND** it still contains the installed MDS package and intended built-in
  market config required for standalone startup
