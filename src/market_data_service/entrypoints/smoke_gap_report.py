"""Console report formatting for the gap-repair smoke runner."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from market_data_service.entrypoints.smoke_gap_repair import SmokeGapRepairResult


def print_smoke_gap_repair_result(result: SmokeGapRepairResult) -> None:
    repair_post = result.repair.post_repair_audit
    print(f"database={result.database_path}")
    print(f"stream={result.stream.canonical_id}")
    print(f"window=[{result.window.start_ms}, {result.window.end_ms})")
    print(f"deleted_open_time_ms={result.deleted_open_time_ms}")
    print(
        "initial_audit "
        f"continuity={str(result.initial_audit.is_continuous).lower()} "
        f"gaps={len(result.initial_audit.gaps)}"
    )
    print(
        "gap_audit "
        f"continuity={str(result.gap_audit.is_continuous).lower()} "
        f"gaps={len(result.gap_audit.gaps)}"
    )
    print(
        "repair "
        f"status={result.repair.status.value} "
        f"attempted_windows={result.repair.attempted_windows} "
        f"completed_windows={result.repair.completed_windows} "
        f"post_continuity={str(repair_post.is_continuous if repair_post else False).lower()}"
    )
    print(
        "repeat_repair "
        f"status={result.repeated_repair.status.value} "
        f"attempted_windows={result.repeated_repair.attempted_windows}"
    )
    print(f"smoke_result={'PASS' if result.ok else 'FAIL'}")
