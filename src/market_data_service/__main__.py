from __future__ import annotations

import argparse
import sys

from market_data_service import __version__
from market_data_service.entrypoints.backfill import main as backfill_main
from market_data_service.entrypoints.smoke_backfill import main as smoke_backfill_main
from market_data_service.entrypoints.smoke_rest import main as smoke_rest_main


def main(argv: list[str] | None = None) -> int:
    args_in = sys.argv[1:] if argv is None else argv
    if args_in[:1] == ["backfill"]:
        return backfill_main(args_in[1:])
    if args_in[:1] == ["smoke-backfill"]:
        return smoke_backfill_main(args_in[1:])
    if args_in[:1] == ["smoke-rest"]:
        return smoke_rest_main(args_in[1:])

    parser = argparse.ArgumentParser(prog="market-data-service")
    subparsers = parser.add_subparsers(dest="command")
    subparsers.add_parser("backfill", help="run bounded historical REST backfill")
    subparsers.add_parser("smoke-backfill", help="run real bounded backfill smoke")
    subparsers.add_parser("smoke-rest", help="run the local Bybit REST smoke test")
    parser.parse_args(args_in)
    print(f"market-data-service {__version__}: architecture baseline initialized")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
