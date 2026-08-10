# Design: MDS Container Production Readiness v1

## Context

The production-readiness work is limited to the standalone MDS container. The
existing runtime contracts remain authoritative for process health, readiness,
startup reconciliation, and graceful shutdown. This design adds a deployment
boundary around those behaviors; it does not move them into Docker-specific
code.

## Current-State Findings

Already present:

- `Dockerfile` uses `CMD ["market-data-service", "serve"]`, so the service is
  launched directly without a shell wrapper.
- The image uses `/data/market.sqlite3` and
  `/app/config/markets.toml`, creates `/data`, and declares it as a volume.
- `docker-compose.yml` preserves
  `${BBB_DATA_ROOT}/market-data:/data` and mounts the operator config read-only.
- `serve.py` installs SIGINT/SIGTERM handlers; the canonical
  `runtime-health-and-readiness` capability already specifies graceful
  shutdown and restart reconciliation.
- `GET /health` already represents process health independently of
  `GET /readiness`.
- Webhook settings remain optional, disabled by default, and environment
  driven; they do not require filesystem writes.
- The Dockerfile copies an explicit runtime file set, while `.dockerignore`
  already excludes Git metadata, local data/SQLite files, `.venv`, common
  caches, ZIP files, snapshots, backups, and host OS metadata.

Still missing:

- The image does not create or select a non-root runtime user.
- Image ownership and standalone Compose do not enforce a read-only
  application/config filesystem.
- The bind-mounted host data directory has no documented non-root ownership
  prerequisite.
- Compose publishes `8080:8080` on all host interfaces.
- The image defines no Docker healthcheck.
- There is no Docker-level restart/recreate persistence and signal-delivery
  smoke coverage.
- There is no automated check of the final image contents, and ignore coverage
  does not yet explicitly cover every common archive/build artifact class.

## Decisions

### Stable non-root identity and filesystem ownership

The image will create a dedicated user and group with a stable numeric UID/GID,
selected and documented by the implementation, and will set that user before
the final command. Packaged code, installed package files, and built-in config
will remain owned by root and readable but not writable by the runtime user.
`/data` will be owned by or writable for that runtime identity.

A bind mount hides image-layer ownership. Operator documentation and smoke-test
setup must therefore create `${BBB_DATA_ROOT}/market-data` with permissions
that allow the documented runtime UID/GID to write. The container will not
start as root merely to chown a host directory; that would weaken the non-root
and direct-PID1 contracts.

Standalone Compose will set the container root filesystem read-only and expose
only `/data` as writable persistent storage. No writable application/config
mount or scratch volume will be added. `PYTHONDONTWRITEBYTECODE=1`, stdout/stderr
logging, and the existing file-based config allow normal operation without
writes elsewhere.

### Direct PID1 and signal delivery

The existing exec-form service command will remain the final container process.
No entrypoint wrapper or supervisor is needed for the proposed hardening.
Container tests will prove that Docker SIGTERM and SIGINT are delivered to that
process and it exits through the existing runtime shutdown path.

The shutdown sequence itself is not specified here. Cancellation boundaries,
resource closure, committed transaction durability, and restart reconciliation
remain owned by `runtime-health-and-readiness` and the relevant storage/runtime
capabilities. The container delta only prevents Docker packaging from
interposing on that behavior.

### Existing `/health` as Docker healthcheck

The healthcheck will be defined in the image so it also applies outside
Compose. It will request `http://127.0.0.1:8080/health` using Python's standard
library or another tool already present in the image. HTTP 200 is success;
connection failure, timeout, or non-200 is failure.

The check will not call `/readiness`, inspect SQLite, contact Bybit, or add a
new endpoint. Healthcheck interval, timeout, start period, and retry values will
be bounded operational settings, not new application semantics.

### Standalone Compose boundary

The only published port will be:

```yaml
ports:
  - "127.0.0.1:8080:8080"
```

The existing persistence mapping remains exactly:

```yaml
volumes:
  - ${BBB_DATA_ROOT}/market-data:/data
```

The read-only operator config mount remains supported. No downstream service,
shared SQLite consumer, or second Compose service is introduced.

### Restart and recreate verification

A Docker smoke test will use a temporary `BBB_DATA_ROOT` whose `market-data`
directory is writable by the runtime UID/GID. It will:

1. build and start the standalone service;
2. wait for Docker health to become healthy;
3. verify the process UID is non-zero and the SQLite file is host-visible;
4. record stable SQLite evidence such as schema version and configured stream
   rows after a clean stop boundary;
5. run `docker compose restart`, wait for health, and compare the evidence;
6. remove/recreate only the container, preserving the host directory, then
   wait for health and compare the evidence again;
7. exercise SIGTERM and SIGINT delivery without redefining runtime shutdown
   assertions already owned by the canonical runtime capability.

The smoke may use the existing bounded startup settings. It must not infer
readiness from Docker health or weaken normal restart reconciliation.

### Build-context and image hygiene

The explicit Dockerfile `COPY` allowlist remains the primary image boundary.
`.dockerignore` will be completed for common repository database sidecars,
virtual environments, Python/tool caches, build outputs, archives, snapshots,
backup directories, and host metadata so accidental future broad copies do not
expand the context silently.

Verification will inspect the built image as well as the ignore rules. Python
distribution metadata created by package installation and the intended
built-in config are valid runtime content; repository-local metadata and
development artifacts are not.

## Scope Guardrails

No runtime settings, health/readiness payload, lifecycle transition, SQLite
tuning, webhook contract, notification behavior, or service topology changes
are part of this design. Existing webhook tests should continue to pass, but
webhook behavior is not a requirement of `container-production-runtime`.
