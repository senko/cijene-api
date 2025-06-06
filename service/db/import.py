#!/usr/bin/env python3
from decimal import Decimal
import logging
import asyncio
import argparse
from datetime import datetime, date
from pathlib import Path
from typing import Any, List, Dict
from csv import DictReader
from time import time

from service.config import settings
from service.db.models import Chain, Store, ChainProduct, Price

logger = logging.getLogger("importer")

db = settings.get_db()


async def read_csv(file_path: Path) -> List[Dict[str, str]]:
    """
    Read a CSV file and return a list of dictionaries.

    Args:
        file_path: Path to the CSV file.

    Returns:
        List of dictionaries where each dictionary represents a row in the CSV.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            reader = DictReader(f)  # type: ignore
            return [row for row in reader]
    except Exception as e:
        logger.error(f"Error reading {file_path}: {e}")
        return []


async def process_stores(stores_path: Path, chain_id: int) -> dict[str, int]:
    """
    Process stores CSV and import to database.

    Args:
        stores_path: Path to the stores CSV file.
        chain_id: ID of the chain to which these stores belong.

    Returns:
        A dictionary mapping store codes to their database IDs.
    """
    logger.debug(f"Importing stores from {stores_path}")

    stores_data = await read_csv(stores_path)
    store_map = {}

    for store_row in stores_data:
        store = Store(
            chain_id=chain_id,
            code=store_row["store_id"],
            type=store_row.get("type"),
            address=store_row.get("address"),
            city=store_row.get("city"),
            zipcode=store_row.get("zipcode"),
        )

        store_id = await db.add_store(store)
        store_map[store.code] = store_id

    logger.debug(f"Processed {len(stores_data)} stores")
    return store_map


async def process_products(
    products_path: Path,
    chain_id: int,
    chain_code: str,
    barcodes: dict[str, int],
) -> Dict[str, int]:
    """
    Process products CSV and import to database.

    As a side effect, this function will also add any newly
    created EAN codes to the provided `barcodes` dictionary.

    Args:
        products_path: Path to the products CSV file.
        chain_id: ID of the chain to which these products belong.
        chain_code: Code of the retail chain.
        barcodes: Dictionary mapping EAN codes to global product IDs.

    Returns:
        A dictionary mapping product codes to their database IDs for the chain.
    """
    logger.debug(f"Processing products from {products_path}")

    products_data = await read_csv(products_path)
    chain_product_map = await db.get_chain_product_map(chain_id)

    # Ideally the CSV would already have valid barcodes, but some older
    # archives contain invalid ones so we need to clean them up.
    def clean_barcode(data: dict[str, Any]) -> dict:
        barcode = data.get("barcode", "").strip()

        if ":" in barcode:
            return data

        if len(barcode) >= 8 and barcode.isdigit():
            return data

        product_id = data.get("product_id", "")
        if not product_id:
            logger.warning(f"Product has no barcode: {data}")
            return data

        # Construct a chain-specific barcode
        data["barcode"] = f"{chain_code}:{product_id}"
        return data

    new_products = [
        clean_barcode(p)
        for p in products_data
        if p["product_id"] not in chain_product_map
    ]

    if not new_products:
        return chain_product_map

    logger.debug(
        f"Found {len(new_products)} new products out of {len(products_data)} total"
    )

    n_new_barcodes = 0
    for product in new_products:
        barcode = product["barcode"]
        if barcode in barcodes:
            continue

        global_product_id = await db.add_ean(barcode)
        barcodes[barcode] = global_product_id
        n_new_barcodes += 1

    if n_new_barcodes:
        logger.debug(f"Added {n_new_barcodes} new barcodes to global products")

    products_to_create = []
    for product in new_products:
        barcode = product["barcode"]
        code = product["product_id"]
        global_product_id = barcodes[barcode]

        products_to_create.append(
            ChainProduct(
                chain_id=chain_id,
                product_id=global_product_id,
                code=code,
                name=product["name"],
                brand=(product["brand"] or "").strip() or None,
                category=(product["category"] or "").strip() or None,
                unit=(product["unit"] or "").strip() or None,
                quantity=(product["quantity"] or "").strip() or None,
            )
        )

    n_inserts = await db.add_many_chain_products(products_to_create)
    if n_inserts != len(new_products):
        logger.warning(
            f"Expected to insert {len(new_products)} products, but inserted {n_inserts}."
        )
    logger.debug(f"Imported {len(new_products)} new chain products")

    chain_product_map = await db.get_chain_product_map(chain_id)
    return chain_product_map


async def process_prices(
    price_date: date,
    prices_path: Path,
    chain_id: int,
    store_map: dict[str, int],
    chain_product_map: dict[str, int],
) -> int:
    """
    Process prices CSV and import to database.

    Args:
        price_date: The date for which the prices are valid.
        prices_path: Path to the prices CSV file.
        chain_id: ID of the chain to which these prices belong.
        store_map: Dictionary mapping store codes to their database IDs.
        chain_product_map: Dictionary mapping product codes to their database IDs.

    Returns:
        The number of prices successfully inserted into the database.
    """
    logger.debug(f"Reading prices from {prices_path}")

    prices_data = await read_csv(prices_path)

    # Create price objects
    prices_to_create = []

    logger.debug(f"Found {len(prices_data)} price entries, preparing to import")

    def clean_price(value: str) -> Decimal | None:
        if value is None:
            return None
        value = value.strip()
        if value == "":
            return None
        dval = Decimal(value)
        if dval == 0:
            return None
        return dval

    for price_row in prices_data:
        store_id = store_map[price_row["store_id"]]
        product_id = chain_product_map.get(price_row["product_id"])
        if product_id is None:
            # Price for a product that wasn't added, perhaps because the
            # barcode is invalid
            logger.warning(
                f"Skipping price for unknown product {price_row['product_id']}"
            )
            continue

        prices_to_create.append(
            Price(
                chain_product_id=product_id,
                store_id=store_id,
                price_date=price_date,
                regular_price=Decimal(price_row["price"]),
                special_price=clean_price(price_row.get("special_price") or ""),
                unit_price=clean_price(price_row["unit_price"]),
                best_price_30=clean_price(price_row["best_price_30"]),
                anchor_price=clean_price(price_row["anchor_price"]),
            )
        )

    logger.debug(f"Importing {len(prices_to_create)} prices")
    n_inserted = await db.add_many_prices(prices_to_create)
    return n_inserted


async def process_chain(
    price_date: date,
    chain_dir: Path,
    barcodes: dict[str, int],
) -> None:
    """
    Process a single retail chain and import its data.

    The expected directory structure and CSV columns are documented in
    `crawler/store/archive_info.txt`.

    Note: updates the `barcodes` dictionary with any new EAN codes found
    (see the `process_products` function).

    Args:
        price_date: The date for which the prices are valid.
        chain_dir: Path to the directory containing the chain's CSV files.
        barcodes: Dictionary mapping EAN codes to global product IDs.

    """
    code = chain_dir.name

    stores_path = chain_dir / "stores.csv"
    if not stores_path.exists():
        logger.warning(f"No stores.csv found for chain {code}")
        return

    products_path = chain_dir / "products.csv"
    if not products_path.exists():
        logger.warning(f"No products.csv found for chain {code}")
        return

    prices_path = chain_dir / "prices.csv"
    if not prices_path.exists():
        logger.warning(f"No prices.csv found for chain {code}")
        return

    logger.debug(f"Processing chain: {code}")

    chain = Chain(code=code)
    chain_id = await db.add_chain(chain)

    store_map = await process_stores(stores_path, chain_id)
    chain_product_map = await process_products(products_path, chain_id, code, barcodes)

    n_new_prices = await process_prices(
        price_date,
        prices_path,
        chain_id,
        store_map,
        chain_product_map,
    )

    logger.info(f"Imported {n_new_prices} new prices for {code}")


async def import_directory(path: Path) -> None:
    """Import data from all chain directories in the given directory."""
    if not path.is_dir():
        logger.error(f"`{path}` does not exist or is not a directory")
        return

    try:
        price_date = datetime.strptime(path.name, "%Y-%m-%d")
    except ValueError:
        logger.error(
            f"Directory `{path.name}` is not a valid date in YYYY-MM-DD format"
        )
        return

    chain_dirs = [d.resolve() for d in path.iterdir() if d.is_dir()]
    if not chain_dirs:
        logger.warning(f"No chain directories found in {path}")
        return

    logger.debug(f"Importing {len(chain_dirs)} chains from {path}")

    t0 = time()

    await db.connect()

    try:
        await db.create_tables()

        barcodes = await db.get_product_barcodes()
        for chain_dir in chain_dirs:
            await process_chain(price_date, chain_dir, barcodes)

        logger.debug(f"Computing average chain prices for {price_date:%Y-%m-%d}")
        await db.compute_chain_prices(price_date)

        logger.debug(f"Computing chain summaries for {price_date:%Y-%m-%d}")
        await db.compute_chain_summaries(price_date)
    finally:
        await db.close()

    t1 = time()
    dt = int(t1 - t0)
    logger.info(f"Imported {len(chain_dirs)} chains in {dt} seconds")


async def main():
    """
    Import price data from a directory.

    This script expects the directory to be named in the format YYYY-MM-DD,
    containing subdirectories for each retail chain. Each chain directory
    should contain CSV files named `stores.csv`, `products.csv`, and `prices.csv`.
    The CSV files should follow the structure documented in
    `crawler/store/archive_info.txt`.

    Database connection settings are loaded from the service configuration, see
    `service/config.py` for details.
    """
    parser = argparse.ArgumentParser(description=main.__doc__)
    parser.add_argument("dir", type=Path, help="Directory containing price data")
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

    await import_directory(args.dir)


if __name__ == "__main__":
    asyncio.run(main())
