import asyncio
import logging
import zipfile
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from time import time

from service.config import settings
from service.db.stats import compute_stats
from .chain_importer import process_chain_products_only, process_chain_stores_and_prices

logger = logging.getLogger("importer.archive_handler")

db = settings.get_db()


def parse_date_from_path(path: Path) -> datetime:
    """
    Parse date from path name in YYYY-MM-DD format.

    Args:
        path: Path with date in name.

    Returns:
        Parsed datetime object.

    Raises:
        ValueError: If date format is invalid.
    """
    date_str = path.stem if path.is_file() else path.name
    return datetime.strptime(date_str, "%Y-%m-%d")


async def import_archive(path: Path, compute_stats_flag: bool = True) -> None:
    """Import data from all chain directories in the given zip archive."""
    try:
        price_date = parse_date_from_path(path)
    except ValueError:
        logger.error(f"`{path.stem}` is not a valid date in YYYY-MM-DD format")
        return

    try:
        with TemporaryDirectory() as temp_dir:  # type: ignore
            logger.debug(f"Extracting archive {path} to {temp_dir}")
            with zipfile.ZipFile(path, "r") as zip_ref:
                zip_ref.extractall(temp_dir)
            await _import(Path(temp_dir), price_date, compute_stats_flag)
    except zipfile.BadZipFile:
        logger.error(f"Invalid or corrupted zip file: {path}")
        return
    except FileNotFoundError:
        logger.error(f"Archive file not found: {path}")
        return
    except PermissionError:
        logger.error(f"Permission denied accessing archive: {path}")
        return
    except Exception as e:
        logger.error(f"Unexpected error processing archive {path}: {e}")
        return


async def import_directory(path: Path, compute_stats_flag: bool = True) -> None:
    """Import data from all chain directories in the given directory."""
    if not path.is_dir():
        logger.error(f"`{path}` does not exist or is not a directory")
        return

    try:
        price_date = parse_date_from_path(path)
    except ValueError:
        logger.error(
            f"Directory `{path.name}` is not a valid date in YYYY-MM-DD format"
        )
        return

    await _import(path, price_date, compute_stats_flag)


async def _import(
    path: Path, price_date: datetime, compute_stats_flag: bool = True
) -> None:
    """
    Import data from chain directories in the given path.

    Args:
        path: Path containing chain directories.
        price_date: Date for which the prices are valid.
        compute_stats_flag: Whether to compute statistics after import.
    """
    chain_dirs = [d for d in path.iterdir() if d.is_dir()]
    if not chain_dirs:
        logger.warning(f"No chain directories found in {path}")
        return

    logger.debug(f"Importing {len(chain_dirs)} chains from {path}")

    t0 = time()

    barcodes = await db.get_product_barcodes()

    # Phase 1: Sequential EAN processing to avoid deadlocks
    logger.debug("Phase 1: Processing EAN codes sequentially")
    await _process_eans_sequentially(chain_dirs, price_date, barcodes)

    # Phase 2: Parallel processing of stores and prices
    logger.debug("Phase 2: Processing stores and prices in parallel")
    await _process_stores_and_prices_parallel(chain_dirs, price_date, barcodes)

    dt = int(time() - t0)
    logger.info(f"Imported {len(chain_dirs)} chains in {dt} seconds")

    if compute_stats_flag:
        await compute_stats(price_date)
    else:
        logger.debug(f"Skipping statistics computation for {price_date:%Y-%m-%d}")


async def _process_eans_sequentially(
    chain_dirs: list[Path], price_date: datetime, barcodes: dict[str, int]
) -> None:
    """
    Process EAN codes sequentially to avoid database deadlocks.

    Args:
        chain_dirs: List of chain directories to process.
        price_date: Date for which the prices are valid.
        barcodes: Dictionary of existing EAN codes and their product IDs.
    """
    for chain_dir in chain_dirs:
        await process_chain_products_only(price_date, chain_dir, barcodes)


async def _process_stores_and_prices_parallel(
    chain_dirs: list[Path], price_date: datetime, barcodes: dict[str, int]
) -> None:
    """
    Process stores and prices in parallel since they don't share resources.

    Args:
        chain_dirs: List of chain directories to process.
        price_date: Date for which the prices are valid.
        barcodes: Dictionary of existing EAN codes and their product IDs.
    """
    tasks = []
    for chain_dir in chain_dirs:
        task = process_chain_stores_and_prices(price_date, chain_dir, barcodes)
        tasks.append(task)

    await asyncio.gather(*tasks)
