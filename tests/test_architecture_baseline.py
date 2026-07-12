from __future__ import annotations

import ast
from pathlib import Path

PACKAGE_ROOT = Path(__file__).parents[1] / "src" / "market_data_service"


def _internal_imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    return {name for name in imports if name.startswith("market_data_service")}


def test_domain_does_not_import_outer_layers() -> None:
    forbidden = (
        "market_data_service.application",
        "market_data_service.ports",
        "market_data_service.adapters",
        "market_data_service.entrypoints",
    )
    for path in (PACKAGE_ROOT / "domain").rglob("*.py"):
        for imported in _internal_imports(path):
            assert not imported.startswith(forbidden), f"{path} imports forbidden {imported}"


def test_application_does_not_import_concrete_adapters_or_entrypoints() -> None:
    forbidden = (
        "market_data_service.adapters",
        "market_data_service.entrypoints",
    )
    for path in (PACKAGE_ROOT / "application").rglob("*.py"):
        for imported in _internal_imports(path):
            assert not imported.startswith(forbidden), f"{path} imports forbidden {imported}"


def test_python_modules_remain_laconic() -> None:
    """Catch accidental manager-style growth before files become hard to split."""

    limit = 220
    for path in PACKAGE_ROOT.rglob("*.py"):
        line_count = len(path.read_text(encoding="utf-8").splitlines())
        assert line_count <= limit, f"{path} has {line_count} lines; split responsibilities"


def test_no_generic_manager_or_utils_modules() -> None:
    forbidden_stems = {"manager", "managers", "utils", "helpers"}
    for path in PACKAGE_ROOT.rglob("*.py"):
        assert path.stem not in forbidden_stems, f"generic dumping-ground module: {path}"


def test_realtime_modules_preserve_responsibility_boundaries() -> None:
    transport = PACKAGE_ROOT / "adapters" / "bybit" / "websocket" / "transport.py"
    protocol = PACKAGE_ROOT / "adapters" / "bybit" / "websocket" / "protocol.py"
    connector = PACKAGE_ROOT / "application" / "realtime" / "connector.py"
    handler = PACKAGE_ROOT / "application" / "realtime" / "handler.py"

    forbidden_transport = (
        "market_data_service.adapters.sqlite",
        "market_data_service.application.ingest",
        "market_data_service.application.repair",
        "market_data_service.application.backfill",
    )
    for imported in _internal_imports(transport):
        assert not imported.startswith(forbidden_transport)

    forbidden_protocol = (
        "market_data_service.adapters.sqlite",
        "market_data_service.application.ingest",
        "market_data_service.application.repair",
        "market_data_service.application.backfill",
    )
    for imported in _internal_imports(protocol):
        assert not imported.startswith(forbidden_protocol)

    for imported in _internal_imports(connector):
        assert not imported.startswith("market_data_service.adapters")
        assert not imported.startswith("market_data_service.application.repair")
        assert not imported.startswith("market_data_service.application.backfill")

    for imported in _internal_imports(handler):
        assert not imported.startswith("market_data_service.adapters")
