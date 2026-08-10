from pathlib import Path

ROOT = Path(__file__).parents[1]


def _text(name: str) -> str:
    return (ROOT / name).read_text(encoding="utf-8")


def test_runtime_image_is_non_root_direct_pid1_and_healthchecked() -> None:
    dockerfile = _text("Dockerfile")
    runtime = dockerfile.split("FROM python:3.12-slim AS runtime", maxsplit=1)[1]

    assert "USER 10001:10001" in runtime
    assert 'CMD ["market-data-service", "serve"]' in runtime
    assert "ENTRYPOINT" not in runtime
    assert "HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3" in runtime
    assert "os.environ.get('MDS_HTTP_PORT', '8080')" in runtime
    assert "http://127.0.0.1:{port}/health" in runtime
    assert "/readiness" not in runtime


def test_runtime_image_keeps_only_installed_package_and_config() -> None:
    dockerfile = _text("Dockerfile")
    runtime = dockerfile.split("FROM python:3.12-slim AS runtime", maxsplit=1)[1]

    assert "COPY --from=builder /wheels /wheels" in runtime
    assert "COPY --chown=0:0 config ./config" in runtime
    assert "COPY src" not in runtime
    assert "COPY pyproject.toml" not in runtime
    assert "chmod -R a-w /app" in runtime
    assert "chown 10001:10001 /data" in runtime


def test_compose_is_read_only_localhost_only_and_preserves_mounts() -> None:
    compose = _text("docker-compose.yml")
    lines = {line.strip() for line in compose.splitlines()}

    assert "read_only: true" in lines
    assert "stop_grace_period: 20s" in lines
    assert '- "127.0.0.1:8080:8080"' in lines
    assert '- "8080:8080"' not in lines
    assert "- ./config/markets.toml:/app/config/markets.toml:ro" in lines
    assert "- ${BBB_DATA_ROOT}/market-data:/data" in lines


def test_build_context_is_an_explicit_allowlist() -> None:
    patterns = [
        line
        for line in _text(".dockerignore").splitlines()
        if line and not line.startswith("#")
    ]

    assert patterns == [
        "**",
        "!pyproject.toml",
        "!README.md",
        "!src/",
        "!src/**",
        "!config/",
        "!config/**",
    ]
