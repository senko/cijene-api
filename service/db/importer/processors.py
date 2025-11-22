import logging
from datetime import date
from pathlib import Path
from typing import Any, Dict

from service.config import settings
from service.db.models import ChainProduct, Store
from .csv_reader import read_csv

logger = logging.getLogger("importer.processors")

db = settings.get_db()


async def process_stores(stores_path: Path, chain_id: int) -> Dict[str, int]:
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

    # Prepare all stores for bulk insertion
    stores_to_create = []
    for store_row in stores_data:
        store = Store(
            chain_id=chain_id,
            code=store_row["store_id"],
            type=store_row.get("type"),
            address=store_row.get("address"),
            city=store_row.get("city"),
            zipcode=store_row.get("zipcode"),
        )
        stores_to_create.append(store)

    # Insert all stores in bulk
    store_map = await db.add_many_stores(stores_to_create)

    logger.debug(f"Processed {len(stores_data)} stores")
    return store_map


def validate_and_fix_barcode(data: Dict[str, Any], chain_code: str) -> Dict[str, Any]:
    """
    Validate barcode data and generate a chain-specific barcode if needed.

    Args:
        data: Product data dictionary.
        chain_code: Code of the retail chain.

    Returns:
        Updated product data dictionary with valid barcode.
    """
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


def _filter_new_products(
    products_data: list[Dict[str, Any]],
    chain_product_map: Dict[str, int],
    chain_code: str,
) -> list[Dict[str, Any]]:
    """
    Filter products that don't exist in the chain and clean their barcodes.

    Args:
        products_data: Raw product data from CSV.
        chain_product_map: Existing product codes mapped to their IDs.
        chain_code: Code of the retail chain.

    Returns:
        List of new products with cleaned barcodes.
    """
    return [
        validate_and_fix_barcode(p, chain_code)
        for p in products_data
        if p["product_id"] not in chain_product_map
    ]


def _extract_missing_barcodes(
    products: list[Dict[str, Any]], existing_barcodes: Dict[str, int]
) -> list[str]:
    """
    Extract barcodes from products that don't exist in the existing barcodes dictionary.

    Args:
        products: List of product dictionaries.
        existing_barcodes: Dictionary mapping barcodes to global product IDs.

    Returns:
        List of missing barcodes that need to be registered.
    """
    new_barcodes = []
    for product in products:
        barcode = product["barcode"]
        if barcode not in existing_barcodes:
            new_barcodes.append(barcode)
    return new_barcodes


async def _register_missing_barcodes_to_database(
    new_products: list[Dict[str, Any]], barcodes_dict: Dict[str, int]
) -> None:
    """
    Register missing barcodes to database and update the global barcodes dictionary.

    Args:
        new_products: List of new product dictionaries.
        barcodes_dict: Dictionary mapping barcodes to global product IDs (modified in place).
    """
    new_barcodes = _extract_missing_barcodes(new_products, barcodes_dict)

    if new_barcodes:
        new_barcode_ids = await db.add_many_eans(new_barcodes)
        barcodes_dict.update(new_barcode_ids)
        logger.debug(f"Added {len(new_barcodes)} new barcodes to global products")


def _sanitize_product_optional_fields(product: Dict[str, Any]) -> Dict[str, str | None]:
    """
    Sanitize optional product fields by converting empty strings to None.

    Args:
        product: Raw product dictionary from CSV.

    Returns:
        Dictionary with sanitized optional field values.
    """
    return {
        "brand": (product["brand"] or "").strip() or None,
        "category": (product["category"] or "").strip() or None,
        "unit": (product["unit"] or "").strip() or None,
        "quantity": (product["quantity"] or "").strip() or None,
    }


def _create_chain_product_objects(
    products: list[Dict[str, Any]], chain_id: int, barcodes_dict: Dict[str, int]
) -> list[ChainProduct]:
    """
    Create ChainProduct objects from product dictionaries.

    Args:
        products: List of product dictionaries.
        chain_id: ID of the chain.
        barcodes_dict: Dictionary mapping barcodes to global product IDs.

    Returns:
        List of ChainProduct objects ready for database insertion.
    """
    products_to_create = []
    for product in products:
        barcode = product["barcode"]
        code = product["product_id"]
        global_product_id = barcodes_dict[barcode]

        validated_data = _sanitize_product_optional_fields(product)

        products_to_create.append(
            ChainProduct(
                chain_id=chain_id,
                product_id=global_product_id,
                code=code,
                name=product["name"],
                brand=validated_data["brand"],
                category=validated_data["category"],
                unit=validated_data["unit"],
                quantity=validated_data["quantity"],
            )
        )

    return products_to_create


async def _insert_chain_products(products_to_create: list[ChainProduct]) -> int:
    """
    Insert ChainProduct objects into the database with validation.

    Args:
        products_to_create: List of ChainProduct objects to insert.

    Returns:
        Number of products successfully inserted.
    """
    if not products_to_create:
        return 0

    n_inserts = await db.add_many_chain_products(products_to_create)
    if n_inserts != len(products_to_create):
        logger.warning(
            f"Expected to insert {len(products_to_create)} products, but inserted {n_inserts}."
        )
    logger.debug(f"Imported {len(products_to_create)} new chain products")
    return n_inserts


async def _fetch_updated_chain_product_map(chain_id: int) -> Dict[str, int]:
    """
    Fetch the updated chain product map from the database.

    Args:
        chain_id: ID of the chain.

    Returns:
        Dictionary mapping product codes to their database IDs for the chain.
    """
    return await db.get_chain_product_map(chain_id)


async def process_products(
    products_path: Path,
    chain_id: int,
    chain_code: str,
    barcodes: Dict[str, int],
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

    new_products = _filter_new_products(products_data, chain_product_map, chain_code)
    if not new_products:
        return chain_product_map

    logger.debug(
        f"Found {len(new_products)} new products out of {len(products_data)} total"
    )

    await _register_missing_barcodes_to_database(new_products, barcodes)

    products_to_create = _create_chain_product_objects(new_products, chain_id, barcodes)
    await _insert_chain_products(products_to_create)

    return await _fetch_updated_chain_product_map(chain_id)


async def process_prices(
    price_date: date,
    prices_path: Path,
    chain_id: int,
    store_map: Dict[str, int],
    chain_product_map: Dict[str, int],
) -> int:
    """
    Process prices CSV and import to database using direct CSV streaming.

    Args:
        price_date: The date for which the prices are valid.
        prices_path: Path to the prices CSV file.
        chain_id: ID of the chain to which these prices belong.
        store_map: Dictionary mapping store codes to their database IDs.
        chain_product_map: Dictionary mapping product codes to their database IDs.

    Returns:
        The number of prices successfully inserted into the database.
    """
    logger.debug(f"Processing prices directly from CSV: {prices_path}")

    # Use direct CSV streaming for optimal performance
    n_inserted = await db.add_many_prices_direct_csv(  # type: ignore[possibly-unbound-attribute]
        prices_path, price_date, store_map, chain_product_map
    )

    logger.debug(f"Imported {n_inserted} prices using direct CSV streaming")
    return n_inserted
