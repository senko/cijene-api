#!/usr/bin/env python3
"""
Delete old prices from the database.

Deletes the per-store prices that are older than the given number of days.
Each chain's latest data stays, also when it is older than that, so the API
still shows chains that stopped publishing data. Computed chain prices and
chain stats stay.

Run it once a day at a time with little API traffic, for example:

    uv run -m service.db.clean_prices --keep-days=30
"""

import argparse
import asyncio
import logging
from datetime import date, timedelta
from time import time

from service.config import settings

logger = logging.getLogger("clean_prices")

db = settings.get_db()

MIN_KEEP_DAYS = 7
MAX_KEEP_DAYS = 360


def keep_days(value: str) -> int:
    """
    Parse and check the --keep-days argument.

    Args:
        value: The argument value.

    Returns:
        The number of days.
    """
    try:
        days = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a whole number: {value!r}")
    if not MIN_KEEP_DAYS <= days <= MAX_KEEP_DAYS:
        raise argparse.ArgumentTypeError(
            f"must be from {MIN_KEEP_DAYS} to {MAX_KEEP_DAYS}, got {days}"
        )
    return days


async def warn_stale_chains(before: date) -> None:
    """
    Log a warning for each chain whose latest data is from before the cutoff.

    Args:
        before: The cutoff date.
    """
    for stats in await db.list_latest_chain_stats():
        if stats.price_date < before:
            logger.warning(
                f"{stats.chain_code}: latest data is from {stats.price_date}, "
                f"before the cutoff {before}; keeping it"
            )


async def clean_prices(days: int, dry_run: bool) -> None:
    """
    Delete (or with dry_run, count) the prices older than the given days.

    Args:
        days: Number of days of prices to keep, counted back from today.
        dry_run: Only count and log the rows to delete.
    """
    before = date.today() - timedelta(days=days)
    await warn_stale_chains(before)

    if dry_run:
        logger.info(f"Dry run: counting prices before {before} (keeping {days} days)")
        counts = await db.count_prices_before(before)
        for price_date, count in counts.items():
            logger.info(f"{price_date}: would delete {count:,} rows")
        logger.info(
            f"Would delete {sum(counts.values()):,} rows, number of dates: {len(counts)}"
        )
        return

    logger.info(f"Deleting prices before {before} (keeping {days} days)")
    t0 = time()
    deleted = await db.delete_prices_before(before)
    if not deleted:
        logger.info("Nothing to delete")
        return
    logger.info(
        f"Deleted {sum(deleted.values()):,} rows in {int(time() - t0)} seconds, "
        f"number of dates: {len(deleted)}"
    )


async def main():
    """
    Command-line entry point.
    """
    parser = argparse.ArgumentParser(
        description="Delete old prices from the database",
    )
    parser.add_argument(
        "--keep-days",
        type=keep_days,
        required=True,
        metavar="N",
        help=(
            f"Keep the prices of the last N days ({MIN_KEEP_DAYS} to "
            f"{MAX_KEEP_DAYS}); delete older ones"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only count and show the rows to delete",
    )
    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s:%(name)s:%(levelname)s:%(message)s",
    )

    await db.connect()

    try:
        await clean_prices(args.keep_days, args.dry_run)
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
