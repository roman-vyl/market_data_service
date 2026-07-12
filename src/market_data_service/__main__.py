from __future__ import annotations

import argparse
import sys

from market_data_service import __version__
from market_data_service.entrypoints.audit_continuity import main as audit_continuity_main
from market_data_service.entrypoints.backfill import main as backfill_main
from market_data_service.entrypoints.serve import main as serve_main
from market_data_service.entrypoints.smoke_all_backfill import main as smoke_all_backfill_main
from market_data_service.entrypoints.smoke_audit_continuity import (
    main as smoke_audit_continuity_main,
)
from market_data_service.entrypoints.smoke_backfill import main as smoke_backfill_main
from market_data_service.entrypoints.smoke_full_bootstrap import (
    main as smoke_full_bootstrap_main,
)
from market_data_service.entrypoints.smoke_gap_repair import main as smoke_gap_repair_main
from market_data_service.entrypoints.smoke_rest import main as smoke_rest_main
from market_data_service.entrypoints.smoke_websocket import main as smoke_websocket_main


def main(argv: list[str] | None = None) -> int:
    args_in = sys.argv[1:] if argv is None else argv
    if args_in[:1] == ["audit-continuity"]:
        return audit_continuity_main(args_in[1:])
    if args_in[:1] == ["backfill"]:
        return backfill_main(args_in[1:])
    if args_in[:1] == ["smoke-all-backfill"]:
        return smoke_all_backfill_main(args_in[1:])
    if args_in[:1] == ["smoke-backfill"]:
        return smoke_backfill_main(args_in[1:])
    if args_in[:1] == ["smoke-full-bootstrap"]:
        return smoke_full_bootstrap_main(args_in[1:])
    if args_in[:1] == ["smoke-audit-continuity"]:
        return smoke_audit_continuity_main(args_in[1:])
    if args_in[:1] == ["smoke-gap-repair"]:
        return smoke_gap_repair_main(args_in[1:])
    if args_in[:1] == ["smoke-rest"]:
        return smoke_rest_main(args_in[1:])
    if args_in[:1] == ["smoke-websocket"]:
        return smoke_websocket_main(args_in[1:])
    if args_in[:1] == ["serve"]:
        return serve_main(args_in[1:])

    parser = argparse.ArgumentParser(prog="market-data-service")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("audit-continuity", help="audit canonical candle continuity")
    subparsers.add_parser("backfill", help="run bounded historical REST backfill")
    subparsers.add_parser(
        "smoke-audit-continuity",
        help="run real REST backfill plus continuity smoke",
    )
    subparsers.add_parser(
        "smoke-all-backfill", help="run real two-stream bounded resume smoke"
    )
    subparsers.add_parser("smoke-backfill", help="run real bounded backfill smoke")
    subparsers.add_parser(
        "smoke-full-bootstrap",
        help="run real full-history bootstrap restart/resume smoke",
    )
    subparsers.add_parser(
        "smoke-gap-repair",
        help="run real bounded backfill plus production gap-repair smoke",
    )
    subparsers.add_parser("smoke-rest", help="run the local Bybit REST smoke test")
    subparsers.add_parser(
        "smoke-websocket", help="run bounded real Bybit WebSocket ingestion smoke"
    )
    subparsers.add_parser("serve", help="run the long-lived market-data runtime")
    parser.parse_args(args_in)
    print(f"market-data-service {__version__}: architecture baseline initialized")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
