import logging
from datetime import date
from pathlib import Path
from typing import Dict, NamedTuple

from service.config import settings
from service.db.models import Chain
from .processors import process_stores, process_products, process_prices

logger = logging.getLogger("importer.chain_importer")

db = settings.get_db()


class ChainFiles(NamedTuple):
    """Container for chain CSV file paths."""

    stores: Path
    products: Path
    prices: Path


def build_chain_csv_file_paths(chain_dir: Path) -> ChainFiles:
    """
    Build the CSV file paths for a chain directory.

    Args:
        chain_dir: Path to the chain directory.

    Returns:
        ChainFiles containing the constructed paths to the CSV files.
    """
    return ChainFiles(
        stores=chain_dir / "stores.csv",
        products=chain_dir / "products.csv",
        prices=chain_dir / "prices.csv",
    )


def get_chain_files_if_all_exist(chain_dir: Path) -> ChainFiles | None:
    """
    Get chain CSV file paths if all required files exist in the directory.

    Args:
        chain_dir: Path to the chain directory.

    Returns:
        ChainFiles if all required files exist, None otherwise.
    """
    code = chain_dir.name
    files = build_chain_csv_file_paths(chain_dir)

    for file_path in files:
        if not file_path.exists():
            logger.warning(f"No {file_path.name} found for chain {code}")
            return None

    return files


async def register_chain_in_database(chain_dir: Path) -> int:
    """
    Register a chain in the database and return its ID.

    Args:
        chain_dir: Path to the chain directory.

    Returns:
        The database ID of the registered chain.
    """
    code = chain_dir.name
    chain = Chain(code=code)
    return await db.add_chain(chain)


async def process_chain_products_only(
    price_date: date,
    chain_dir: Path,
    barcodes: Dict[str, int],
) -> None:
    """
    Process only the products/EAN codes for a chain to avoid deadlocks.

    Args:
        price_date: The date for which the prices are valid.
        chain_dir: Path to the directory containing the chain's CSV files.
        barcodes: Dictionary mapping EAN codes to global product IDs.
    """
    files = get_chain_files_if_all_exist(chain_dir)
    if files is None:
        return

    code = chain_dir.name
    logger.debug(f"Processing products for chain: {code}")

    chain_id = await register_chain_in_database(chain_dir)

    # Only process products to add EAN codes sequentially
    await process_products(files.products, chain_id, code, barcodes)


async def process_chain_stores_and_prices(
    price_date: date,
    chain_dir: Path,
    barcodes: Dict[str, int],
) -> None:
    """
    Process stores and prices for a chain (EAN codes should already be processed).

    Args:
        price_date: The date for which the prices are valid.
        chain_dir: Path to the directory containing the chain's CSV files.
        barcodes: Dictionary mapping EAN codes to global product IDs.
    """
    files = get_chain_files_if_all_exist(chain_dir)
    if files is None:
        return

    code = chain_dir.name
    logger.debug(f"Processing stores and prices for chain: {code}")

    chain_id = await register_chain_in_database(chain_dir)

    # Process stores and prices (products should already be processed)
    store_map = await process_stores(files.stores, chain_id)
    chain_product_map = await db.get_chain_product_map(chain_id)

    n_new_prices = await process_prices(
        price_date,
        files.prices,
        chain_id,
        store_map,
        chain_product_map,
    )

    logger.info(f"Imported {n_new_prices} new prices for {code}")
