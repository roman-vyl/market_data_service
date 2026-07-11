from pathlib import Path


def test_schema_v1_has_no_event_or_consumer_cursor_tables() -> None:
    schema = Path("src/market_data_service/adapters/sqlite/schema_v1.sql").read_text()
    lowered = schema.lower()
    assert "market_events" not in lowered
    assert "consumer_offsets" not in lowered
    assert "consumer_cursors" not in lowered


def test_normative_contract_uses_readiness_and_consumer_owned_cursor() -> None:
    contract = Path("docs/consumer-readiness-contract.md").read_text()
    assert "MUST NOT make trading" in contract
    assert "last_processed_open_time_ms" in contract
    assert "market event log" in contract
    assert "ordered range reads" in contract


def test_open_spec_does_not_require_event_cursor_api() -> None:
    spec = Path(
        "openspec/changes/market-data-service-v1/specs/market-data-service-v1/spec.md"
    ).read_text()
    tasks = Path("openspec/changes/market-data-service-v1/tasks.md").read_text()
    assert "event cursor endpoint" not in tasks.lower()
    assert "server-owned consumer offsets" in spec
