from market_data_service.application.backfill import plan_sequential_backfill
from market_data_service.domain.backfill import BackfillBudget, BackfillRequest, BackfillSelection
from market_data_service.domain.identity import InstrumentKey, StreamKey


def _stream(ticker: str) -> StreamKey:
    return StreamKey(InstrumentKey(ticker), "1m")


def test_one_stream_backfill_is_bounded_and_selected() -> None:
    btc = _stream("BTCUSDT.P")
    eth = _stream("ETHUSDT.P")
    request = BackfillRequest(
        selection=BackfillSelection.ONE_STREAM,
        stream=btc,
        budget=BackfillBudget(max_windows=100),
    )

    plan = plan_sequential_backfill(request, [btc, eth])

    assert plan.streams == (btc,)
    assert plan.max_windows_per_stream == 100


def test_all_streams_preserve_configuration_order() -> None:
    btc = _stream("BTCUSDT.P")
    eth = _stream("ETHUSDT.P")
    request = BackfillRequest(
        selection=BackfillSelection.ALL_STREAMS,
        budget=BackfillBudget(max_windows=20),
    )

    plan = plan_sequential_backfill(request, [btc, eth])

    assert plan.streams == (btc, eth)
    assert plan.max_windows_per_stream == 20


def test_duplicate_configured_streams_are_deduplicated_stably() -> None:
    btc = _stream("BTCUSDT.P")
    eth = _stream("ETHUSDT.P")
    request = BackfillRequest(
        selection=BackfillSelection.ALL_STREAMS,
        budget=BackfillBudget(max_windows=1),
    )

    plan = plan_sequential_backfill(request, [btc, btc, eth])

    assert plan.streams == (btc, eth)


def test_one_stream_must_be_configured() -> None:
    btc = _stream("BTCUSDT.P")
    eth = _stream("ETHUSDT.P")
    request = BackfillRequest(
        selection=BackfillSelection.ONE_STREAM,
        stream=eth,
        budget=BackfillBudget(max_windows=1),
    )

    try:
        plan_sequential_backfill(request, [btc])
    except ValueError as error:
        assert "not configured" in str(error)
    else:
        raise AssertionError("expected unconfigured stream rejection")


def test_backfill_budget_must_be_positive() -> None:
    try:
        BackfillBudget(max_windows=0)
    except ValueError as error:
        assert "positive" in str(error)
    else:
        raise AssertionError("expected invalid budget rejection")
