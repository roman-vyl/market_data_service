from pathlib import Path

ADR_DIR = Path("docs/adr")

EXPECTED = {
    "001-standalone-service-repository.md",
    "002-sqlite-single-owner-storage.md",
    "003-full-available-1m-history.md",
    "004-canonical-ticker-mapping.md",
    "005-one-canonical-ingestion-path.md",
    "006-exact-decimal-persistence.md",
    "007-minimal-schema-v1.md",
    "008-readiness-first-consumers.md",
    "009-per-stream-state-machine.md",
    "010-sequential-bounded-backfill.md",
    "011-layered-architecture.md",
}


def test_all_accepted_adrs_exist() -> None:
    actual = {path.name for path in ADR_DIR.glob("[0-9][0-9][0-9]-*.md")}
    assert actual == EXPECTED


def test_adrs_are_short_and_have_required_sections() -> None:
    for path in ADR_DIR.glob("[0-9][0-9][0-9]-*.md"):
        text = path.read_text(encoding="utf-8")
        assert "**Status:** Accepted" in text
        assert "## Context" in text
        assert "## Decision" in text
        assert "## Consequences" in text
        assert "## Rejected alternatives" in text
        assert len(text.splitlines()) <= 50, path
