"""Sequential bounded full-history bootstrap for configured streams."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Literal

from market_data_service.application.full_bootstrap import (
    BootstrapFullStreamHistory,
    FullHistoryBootstrapRequest,
    FullHistoryBootstrapResult,
)
from market_data_service.application.source_failure import classify_source_failure
from market_data_service.domain.identity import StreamKey
from market_data_service.domain.instruments import InstrumentCoverage


@dataclass(frozen=True, slots=True)
class MultiStreamBackfillRequest:
    max_windows_per_stream: int

    def __post_init__(self) -> None:
        if self.max_windows_per_stream <= 0:
            raise ValueError("max_windows_per_stream must be positive")


@dataclass(frozen=True, slots=True)
class MultiStreamBackfillOutcome:
    coverage: InstrumentCoverage
    stream: StreamKey
    result: FullHistoryBootstrapResult | None
    error_code: str | None = None
    error_detail: str | None = None
    failure_disposition: Literal["recoverable", "fatal"] | None = None


@dataclass(frozen=True, slots=True)
class MultiStreamBackfillResult:
    outcomes: tuple[MultiStreamBackfillOutcome, ...]
    status: Literal["complete", "incomplete", "failed"]

    @property
    def attempted_streams(self) -> int:
        return len(self.outcomes)

    @property
    def has_errors(self) -> bool:
        return any(outcome.error_code is not None for outcome in self.outcomes)


class BackfillAllConfiguredStreams:
    """Verify instruments, then bootstrap every enabled configured stream."""

    def __init__(
        self,
        metadata_verifier: Callable[[InstrumentCoverage], object],
        bootstrap_factory: Callable[[InstrumentCoverage, StreamKey], BootstrapFullStreamHistory],
    ) -> None:
        self._metadata_verifier = metadata_verifier
        self._bootstrap_factory = bootstrap_factory

    def execute(
        self,
        coverages: Sequence[InstrumentCoverage],
        request: MultiStreamBackfillRequest,
    ) -> MultiStreamBackfillResult:
        enabled = tuple(coverage for coverage in coverages if coverage.enabled)
        if not enabled:
            raise ValueError("no enabled instruments configured")

        outcomes: list[MultiStreamBackfillOutcome] = []
        overall: Literal["complete", "incomplete", "failed"] = "complete"
        for coverage in enabled:
            try:
                self._metadata_verifier(coverage)
            except Exception as exc:
                decision = classify_source_failure(exc)
                for stream in coverage.stream_keys:
                    outcomes.append(
                        MultiStreamBackfillOutcome(
                            coverage=coverage,
                            stream=stream,
                            result=None,
                            error_code=decision.code,
                            error_detail=decision.detail,
                            failure_disposition=decision.disposition.value,
                        )
                    )
                if decision.disposition.value == "fatal":
                    overall = "failed"
                    break
                overall = "incomplete"
                continue

            for stream in coverage.stream_keys:
                result = self._bootstrap_factory(coverage, stream).execute(
                    FullHistoryBootstrapRequest(
                        stream=stream,
                        max_windows=request.max_windows_per_stream,
                    )
                )
                outcomes.append(
                    MultiStreamBackfillOutcome(
                        coverage=coverage,
                        stream=stream,
                        result=result,
                        error_code=result.error_code,
                        error_detail=result.error_detail,
                        failure_disposition=result.failure_disposition,
                    )
                )
                if result.failure_disposition == "fatal":
                    overall = "failed"
                    return MultiStreamBackfillResult(tuple(outcomes), overall)
                if result.error_code is not None or not result.reached_target:
                    overall = "incomplete"

        return MultiStreamBackfillResult(outcomes=tuple(outcomes), status=overall)
