#!/usr/bin/env python3
import argparse
from datetime import datetime
import logging
import sys
from pathlib import Path

from crawler.crawl import crawl, get_chains
from crawler.store.base import add_file_logging

logger = logging.getLogger(__name__)


def parse_date(date_str):
    """Parse a date string in YYYY-MM-DD format."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError("Date must be in YYYY-MM-DD format")


def setup_logging(log_level):
    """Configure logging for the crawler package."""
    import codecs

    stderr = codecs.getwriter("utf-8")(sys.stderr.buffer, errors="replace")
    level_map = {
        "debug": logging.DEBUG,
        "info": logging.INFO,
        "warning": logging.WARNING,
        "error": logging.ERROR,
        "critical": logging.CRITICAL,
    }

    level = level_map.get(log_level.lower(), logging.WARNING)
    logging.basicConfig(
        level=level,
        format="%(asctime)s:%(name)s:%(levelname)s:%(message)s",
        stream=stderr,
    )

    # Only enable logs from the crawler package
    for handler in logging.root.handlers:
        handler.addFilter(lambda record: record.name.startswith("crawler"))

    # Set other loggers to a higher level to suppress their messages
    for logger_name in logging.root.manager.loggerDict:
        if not logger_name.startswith("crawler"):
            logging.getLogger(logger_name).setLevel(logging.ERROR)


def main():
    parser = argparse.ArgumentParser(
        description="Crawl retail chains for product pricing data",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "output_path",
        nargs="?",
        type=Path,
        default=None,
        help="Output directory path where data will be stored.\n(Required unless -l/--list is used)",
    )
    parser.add_argument(
        "-d",
        "--date",
        type=parse_date,
        help="Date for which to crawl (format: YYYY-MM-DD, defaults to today)",
    )
    parser.add_argument(
        "-c",
        "--chain",
        help="Comma-separated list of retail chains to crawl (defaults to all)",
    )
    parser.add_argument(
        "-l",
        "--list",
        action="store_true",
        help="List supported retail chains and exit (output_path is not required)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        choices=["debug", "info", "warning", "error", "critical"],
        default="warning",
        help="Set verbosity level (default: warning)",
    )
    parser.add_argument(
        "-s",
        "--sql",
        action="store_true",
        help="Process data and save to DB\n",
    )

    args = parser.parse_args()

    # Set up logging
    setup_logging(args.verbose)
    add_file_logging(args.output_path)

    if args.list:
        print("Supported retail chains:")
        for chain_name in get_chains():
            print(f"  - {chain_name}")
        return 0

    if args.output_path is None:
        parser.error("output_path is required; use -h/--help for more info")

    if args.output_path.is_file():
        parser.error(f"Output path '{args.output_path}' is a file.")

    if not args.output_path.exists():
        args.output_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directory: {args.output_path}")

    chains_to_crawl = None
    if args.chain:
        chains_to_crawl = [chain.strip() for chain in args.chain.split(",")]
        available_chains = get_chains()
        for chain_name in chains_to_crawl:
            if chain_name not in available_chains:
                parser.error(
                    f"Unknown chain '{chain_name}'. Available chains: {', '.join(available_chains)}"
                )

    # Run the crawler
    try:
        # Ensure date is None if not provided, so crawl() uses its default
        crawl_date = args.date  # parse_date already handles empty string to None

        chains_txt = (
            ", ".join(chains_to_crawl) if chains_to_crawl else "all retail chains"
        )
        date_txt = args.date.strftime("%Y-%m-%d") if args.date else "today"
        logger.info(f"Fetching price data from {chains_txt} for {date_txt} ...")

        zip_path = crawl(
            args.output_path, crawl_date, chains_to_crawl, process_db=args.sql
        )
        logger.info(f"Archive created: {zip_path}")
        return 0
    except Exception as e:
        print(f"Error during crawling: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
