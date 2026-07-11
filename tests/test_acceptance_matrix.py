import re
from pathlib import Path

MATRIX_PATH = Path("docs/acceptance-test-matrix.md")
REQUIRED_PREFIXES = {
    "CFG",
    "VAL",
    "DB",
    "ING",
    "BST",
    "GAP",
    "STM",
    "MUL",
    "CON",
    "RST",
    "WSS",
    "API",
    "RUN",
}


def test_acceptance_matrix_has_unique_scenario_ids() -> None:
    text = MATRIX_PATH.read_text(encoding="utf-8")
    scenario_ids = re.findall(r"\| ([A-Z]{2,3}-\d{2}) \|", text)

    assert scenario_ids
    assert len(scenario_ids) == len(set(scenario_ids))


def test_acceptance_matrix_covers_all_required_areas() -> None:
    text = MATRIX_PATH.read_text(encoding="utf-8")
    scenario_ids = re.findall(r"\| ([A-Z]{2,3})-\d{2} \|", text)

    assert set(scenario_ids) >= REQUIRED_PREFIXES


def test_acceptance_matrix_names_first_real_milestones() -> None:
    text = MATRIX_PATH.read_text(encoding="utf-8")

    assert "First executable integration milestone" in text
    assert "First Bybit smoke milestone" in text
