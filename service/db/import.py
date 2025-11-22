#!/usr/bin/env python3
import argparse
import asyncio
import logging
from pathlib import Path

from service.config import settings
from service.db.importer.archive_handler import import_archive, import_directory

db = settings.get_db()


async def main():
    """
    Import price data from directories or zip archives.

    This script expects the directories to be named in the format YYYY-MM-DD,
    containing subdirectories for each retail chain. Each chain directory
    should contain CSV files named `stores.csv`, `products.csv`, and `prices.csv`.
    The CSV files should follow the structure documented in
    `crawler/store/archive_info.txt`.

    Zip archives should be named YYYY-MM-DD.zip and contain the same resources
    as directories described above.

    Database connection settings are loaded from the service configuration, see
    `service/config.py` for details.
    """
    parser = argparse.ArgumentParser(
        description=main.__doc__,
        formatter_class=argparse.RawTextHelpFormatter,
    )
    parser.add_argument(
        "paths",
        type=Path,
        help="One or more directories or zip archives containing price data",
        nargs="+",
    )
    parser.add_argument(
        "-s",
        "--skip-stats",
        action="store_true",
        help="Skip computing chain stats",
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

    compute_stats_flag = not args.skip_stats

    await db.connect()

    try:
        await db.create_tables()

        for path in args.paths:
            if path.is_dir():
                await import_directory(path, compute_stats_flag)
            elif path.suffix.lower() == ".zip":
                await import_archive(path, compute_stats_flag)
            else:
                logging.error(
                    f"Path `{path}` is neither a directory nor a zip archive."
                )
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(main())
