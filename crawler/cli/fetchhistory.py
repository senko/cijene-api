#!/usr/bin/env python3
"""
Fetch historical price data by iterating through past dates.
"""
import sys
from argparse import ArgumentParser
from datetime import date, timedelta
from pathlib import Path

from crawler.crawl import crawl, get_chains
from crawler.cli.crawl import parse_date, setup_logging

DEFAULT_START_DATE = date(2025, 5, 2)


def main():
    parser = ArgumentParser(
        description="Fetch historical price data from law effective date to end date",
    )
    parser.add_argument(
        "output_path",
        nargs="?",
        type=Path,
        default=None,
        help="Directory where data will be stored (required)",
    )
    parser.add_argument(
        "-s", "--start-date",
        type=parse_date,
        default=None,
        help="Start date (YYYY-MM-DD), defaults to law effective date",
    )
    parser.add_argument(
        "-e", "--end-date",
        type=parse_date,
        default=None,
        help="End date (YYYY-MM-DD), defaults to today",
    )
    parser.add_argument(
        "-c", "--chain",
        help="Comma-separated list of retail chains to fetch (defaults to all)",
    )
    parser.add_argument(
        "-v", "--verbose",
        choices=["debug", "info", "warning", "error", "critical"],
        default="warning",
        help="Set logging verbosity level",
    )

    args = parser.parse_args()
    setup_logging(args.verbose)

    if args.output_path is None:
        parser.error("output_path is required; use -h/--help for more info")

    if args.output_path.is_file():
        parser.error(f"Output path '{args.output_path}' is a file.")

    if not args.output_path.exists():
        args.output_path.mkdir(parents=True, exist_ok=True)
        print(f"Created directory: {args.output_path}")

    chains_to_fetch = None
    if args.chain:
        chains_to_fetch = [chain.strip() for chain in args.chain.split(",")]
        available = get_chains()
        for chain_name in chains_to_fetch:
            if chain_name not in available:
                parser.error(
                    f"Unknown chain '{chain_name}'. Available chains: {', '.join(available)}"
                )

    start = args.start_date or DEFAULT_START_DATE
    end = args.end_date or date.today()
    if start > end:
        parser.error("start-date must be on or before end-date")

    current = start
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        if (args.output_path / date_str).exists() or (args.output_path / f"{date_str}.zip").exists():
            print(f"Skipping {date_str}, already exists")
        else:
            print(f"Fetching price data for {date_str} ...", flush=True)
            try:
                crawl(args.output_path, current, chains_to_fetch)
            except Exception as err:
                print(f"Error fetching {date_str}: {err}", file=sys.stderr)
        current += timedelta(days=1)
    return 0


if __name__ == "__main__":
    sys.exit(main())