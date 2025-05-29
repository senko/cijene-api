import datetime
from csv import DictWriter
from decimal import Decimal
from logging import getLogger
from os import makedirs
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from .models import Store

logger = getLogger(__name__)

STORE_COLUMNS = [
    "store_id",
    "type",
    "address",
    "city",
    "zipcode",
]

PRODUCT_COLUMNS = [
    "product_id",
    "barcode",
    "name",
    "brand",
    "category",
    "unit",
    "quantity",
]

PRICE_COLUMNS = [
    "store_id",
    "product_id",
    "price",
    "unit_price",
    "best_price_30",
    "anchor_price",
    "special_price",
]


def transform_products(
    stores: list[Store],
) -> tuple[list[dict], list[dict], list[dict]]:
    """
    Transform store data into a structured format for CSV export.

    Args:
        stores: List of Store objects containing product data.

    Returns:
        Tuple containing:
            - List of store dictionaries with STORE_COLUMNS
            - List of product dictionaries with PRODUCT_COLUMNS
            - List of price dictionaries with PRICE_COLUMNS
    """
    store_list = []
    product_map = {}
    price_list = []

    def maybe(val: Decimal | None) -> Decimal | str:
        return val if val is not None else ""

    for store in stores:
        store_data = {
            "store_id": store.store_id,
            "type": store.store_type,
            "address": store.street_address,
            "city": store.city,
            "zipcode": store.zipcode or "",
        }
        store_list.append(store_data)

        for product in store.items:
            key = f"{store.chain}:{product.product_id}"
            if key not in product_map:
                product_map[key] = {
                    "barcode": product.barcode or key,
                    "product_id": product.product_id,
                    "name": product.product,
                    "brand": product.brand,
                    "category": product.category,
                    "unit": product.unit,
                    "quantity": product.quantity,
                }
            price_list.append(
                {
                    "store_id": store.store_id,
                    "product_id": product.product_id,
                    "price": product.price,
                    "unit_price": maybe(product.unit_price),
                    "best_price_30": maybe(product.best_price_30),
                    "anchor_price": maybe(product.anchor_price),
                    "special_price": maybe(product.special_price),
                }
            )

    return store_list, list(product_map.values()), price_list


def save_csv(path: Path, data: list[dict], columns: list[str]):
    """
    Save data to a CSV file.

    Args:
        path: Path to the CSV file.
        data: List of dictionaries containing the data to save.
        columns: List of column names for the CSV file.
    """
    if not data:
        logger.warning(f"No data to save at {path}, skipping")
        return

    if set(columns) != set(data[0].keys()):
        raise ValueError(
            f"Column mismatch: expected {columns}, got {list(data[0].keys())}"
        )
        return

    with open(path, "w", newline="") as f:
        writer = DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in data:
            writer.writerow({k: str(v) for k, v in row.items()})


def save_chain(chain_path: Path, stores: list[Store]):
    """
    Save retail chain data to CSV files.

    This function creates a directory for the retail chain and saves:

    * stores.csv - containing store information with STORE_COLUMNS
    * products.csv - containing product information with PRODUCT_COLUMNS
    * prices.csv - containing price information with PRICE_COLUMNS

    Args:
        chain_path: Path to the directory where CSV files will be saved
            (will be created if it doesn't exist).
        stores: List of Store objects containing product data.
    """

    makedirs(chain_path, exist_ok=True)
    store_list, product_list, price_list = transform_products(stores)
    save_csv(chain_path / "stores.csv", store_list, STORE_COLUMNS)
    save_csv(chain_path / "products.csv", product_list, PRODUCT_COLUMNS)
    save_csv(chain_path / "prices.csv", price_list, PRICE_COLUMNS)


def copy_archive_info(path: Path):
    archive_info = open(Path(__file__).parent / "archive-info.txt", "r").read()
    with open(path / "archive-info.txt", "w") as f:
        f.write(archive_info)


def create_archive(path: Path, output: Path):
    """
    Create a ZIP archive of price files for a given date.

    Args:
        path: Path to the directory to archive.
        output: Path to the output ZIP file.
    """
    with ZipFile(output, "w", compression=ZIP_DEFLATED, compresslevel=9) as zf:
        for file in path.rglob("*"):
            zf.write(file, arcname=file.relative_to(path))


logger = getLogger(__name__)


def save_to_db(date: datetime.date, stores: list[Store]):
    """
    Save store and product pricing data to a database.
    Handles existing data by updating prices if different, and adding new entities.
    Args:
        date: The date for which the prices are valid.
        stores: List of Store objects containing product data.
    """
    import os
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    from crawler.db.model import (
        Base,
        Chain,
        ProductPrice,
        StoreProduct,
    )
    from crawler.db.model import (
        Product as DbProduct,
    )
    from crawler.db.model import (
        Store as DbStore,
    )

    db_url = os.getenv("SQLALCHEMY_DATABASE_URI")
    if not db_url:
        logger.error("SQLALCHEMY_DATABASE_URI is not set")
        raise RuntimeError("SQLALCHEMY_DATABASE_URI is not set")
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)  # Ensure tables are created
    Session = sessionmaker(bind=engine)
    session = Session()
    chains_to_add = []
    products_to_add = []
    db_stores_to_add = []
    store_products_to_add = []
    product_prices_to_add = []
    try:
        # --- Pre-fetch global caches for existing Chains and Products ---
        # Cache for Chain: {chain_slug: Chain_obj}
        chains_cache = {c.slug: c for c in session.query(Chain).all()}
        # Cache for DbProduct: {barcode: DbProduct_obj}
        db_products_cache = {p.barcode: p for p in session.query(DbProduct).all()}
        # --- Pass 1: Identify and collect new Chains and Products ---
        logger.info("Pass 1: Identifying new Chains and Products...")
        for store_item_data in stores:
            chain_slug = store_item_data.chain
            # Comment: Check for existing Chain.
            if chain_slug not in chains_cache:
                new_chain = Chain(name=chain_slug, slug=chain_slug)
                chains_cache[chain_slug] = new_chain
                chains_to_add.append(new_chain)
            for product_item_data in store_item_data.items:
                effective_barcode = product_item_data.barcode
                if not effective_barcode or len(effective_barcode) < 8:
                    effective_barcode = f"{chain_slug}:{product_item_data.product_id}"

                # Comment: Check for existing DbProduct.
                if effective_barcode not in db_products_cache:
                    new_db_product = DbProduct(
                        barcode=effective_barcode,
                        ext_name=product_item_data.product,
                        ext_brand=product_item_data.brand,
                        ext_category=product_item_data.category,
                        ext_unit=product_item_data.unit,
                        ext_quantity=product_item_data.quantity,
                    )
                    db_products_cache[effective_barcode] = new_db_product
                    products_to_add.append(new_db_product)
        if chains_to_add:
            session.add_all(chains_to_add)
        if products_to_add:
            session.add_all(products_to_add)
        if chains_to_add or products_to_add:
            session.flush()  # Assign IDs to new chains and products
            logger.info(
                f"Flushed {len(chains_to_add)} new chains and {len(products_to_add)} new products to assign IDs."
            )
        # --- Pre-fetch Stores cache now that Chain IDs are stable ---
        db_stores_cache = {
            (s.chain_id, s.ext_store_id): s for s in session.query(DbStore).all()
        }
        processed_db_store_ids = set()
        # --- Pass 2: Identify and collect new Stores ---
        logger.info("Pass 2: Identifying new Stores...")
        for store_item_data in stores:
            chain_obj = chains_cache[store_item_data.chain]
            store_key = (chain_obj.id, store_item_data.store_id)
            # Comment: Check for existing DbStore.
            if store_key not in db_stores_cache:
                new_db_store = DbStore(
                    chain_id=chain_obj.id,
                    ext_store_id=store_item_data.store_id,
                    ext_name=store_item_data.name,
                    ext_store_type=store_item_data.store_type,
                    ext_street_address=store_item_data.street_address,
                    ext_city=store_item_data.city,
                    ext_zipcode=store_item_data.zipcode or None,
                )
                db_stores_cache[store_key] = new_db_store
                db_stores_to_add.append(new_db_store)
        if db_stores_to_add:
            session.add_all(db_stores_to_add)
            session.flush()  # Assign IDs to new stores
            logger.info(f"Flushed {len(db_stores_to_add)} new stores to assign IDs.")
        for store_item_data in stores:
            chain_obj = chains_cache[store_item_data.chain]
            db_store_obj = db_stores_cache[(chain_obj.id, store_item_data.store_id)]
            processed_db_store_ids.add(db_store_obj.id)
        # --- Pre-fetch StoreProducts for all relevant stores ---
        store_products_cache = {}  # {(db_store_id, ext_product_id): StoreProduct_obj}
        if processed_db_store_ids:
            existing_store_products = (
                session.query(StoreProduct)
                .filter(StoreProduct.store_id.in_(list(processed_db_store_ids)))
                .all()
            )
            for sp in existing_store_products:
                store_products_cache[(sp.store_id, sp.ext_product_id)] = sp
        temp_all_sp_objects_cache = store_products_cache.copy()
        # --- Pass 3: Identify and collect new StoreProducts ---
        logger.info("Pass 3: Identifying new StoreProducts...")
        for store_item_data in stores:
            chain_obj = chains_cache[store_item_data.chain]
            db_store_obj = db_stores_cache[(chain_obj.id, store_item_data.store_id)]
            for product_item_data in store_item_data.items:
                sp_key = (db_store_obj.id, product_item_data.product_id)
                # Comment: Check for existing StoreProduct.
                if sp_key not in temp_all_sp_objects_cache:
                    effective_barcode = product_item_data.barcode
                    if not effective_barcode or len(effective_barcode) < 8:
                        effective_barcode = (
                            f"{chain_obj.slug}:{product_item_data.product_id}"
                        )
                    db_product_obj = db_products_cache[effective_barcode]
                    new_store_product = StoreProduct(
                        store_id=db_store_obj.id,
                        barcode=db_product_obj.barcode,
                        ext_product_id=product_item_data.product_id,
                    )
                    temp_all_sp_objects_cache[sp_key] = new_store_product
                    store_products_to_add.append(new_store_product)
        if store_products_to_add:
            session.add_all(store_products_to_add)
            session.flush()  # Assign IDs to new store products
            logger.info(
                f"Flushed {len(store_products_to_add)} new store products to assign IDs."
            )
        # --- Pass 4: Process ProductPrices (Create new or Update existing) ---
        logger.info("Pass 4: Processing ProductPrices...")
        for store_item_data in stores:
            chain_obj = chains_cache[store_item_data.chain]
            db_store_obj = db_stores_cache[(chain_obj.id, store_item_data.store_id)]
            # Load existing prices for this specific store and date (single store cache for prices)
            current_store_date_prices_cache = {  # {store_product_id: ProductPrice_obj}
                pp.store_product_id: pp
                for pp in session.query(ProductPrice)
                .join(StoreProduct)  # Join to filter by store_id
                .filter(
                    StoreProduct.store_id == db_store_obj.id,
                    ProductPrice.valid_date == date,
                )
                .all()
            }
            for product_item_data in store_item_data.items:
                sp_key = (db_store_obj.id, product_item_data.product_id)
                sp_obj = temp_all_sp_objects_cache[sp_key]  # Guaranteed to have ID
                # Comment: Check for existing ProductPrice for this store_product and date.
                existing_price_record = current_store_date_prices_cache.get(sp_obj.id)
                if existing_price_record:
                    # Comment: Update existing ProductPrice if values differ.
                    changed = False
                    if existing_price_record.price != product_item_data.price:
                        logger.info(
                            f"Price change detected for StoreProductID {sp_obj.id} on {date}. "
                            f"Old: {existing_price_record.price}, New: {product_item_data.price}"
                        )
                        existing_price_record.price = product_item_data.price
                        changed = True
                    if existing_price_record.unit_price != product_item_data.unit_price:
                        logger.info(
                            f"Unit price change detected for StoreProductID {sp_obj.id} on {date}. "
                            f"Old: {existing_price_record.unit_price}, New: {product_item_data.unit_price}"
                        )
                        existing_price_record.unit_price = product_item_data.unit_price
                        changed = True
                    if (
                        existing_price_record.best_price_30
                        != product_item_data.best_price_30
                    ):
                        logger.info(
                            f"Best price 30 change detected for StoreProductID {sp_obj.id} on {date}. "
                            f"Old: {existing_price_record.best_price_30}, New: {product_item_data.best_price_30}"
                        )
                        existing_price_record.best_price_30 = (
                            product_item_data.best_price_30
                        )
                        changed = True
                    if (
                        existing_price_record.anchor_price
                        != product_item_data.anchor_price
                    ):
                        logger.info(
                            f"Anchor price change detected for StoreProductID {sp_obj.id} on {date}. "
                            f"Old: {existing_price_record.anchor_price}, New: {product_item_data.anchor_price}"
                        )
                        existing_price_record.anchor_price = (
                            product_item_data.anchor_price
                        )
                        changed = True
                    if (
                        existing_price_record.special_price
                        != product_item_data.special_price
                    ):
                        logger.info(
                            f"Special price change detected for StoreProductID {sp_obj.id} on {date}. "
                            f"Old: {existing_price_record.special_price}, New: {product_item_data.special_price}"
                        )
                        existing_price_record.special_price = (
                            product_item_data.special_price
                        )
                        changed = True
                    if changed:
                        logger.debug(
                            f"Updating price for StoreProductID {sp_obj.id} on {date}."
                        )
                else:
                    new_price = ProductPrice(
                        store_product_id=sp_obj.id,
                        valid_date=date,
                        price=product_item_data.price,
                        unit_price=product_item_data.unit_price,
                        best_price_30=product_item_data.best_price_30,
                        anchor_price=product_item_data.anchor_price,
                        special_price=product_item_data.special_price,
                    )
                    product_prices_to_add.append(new_price)
        if product_prices_to_add:
            session.add_all(product_prices_to_add)
            logger.info(
                f"Prepared {len(product_prices_to_add)} new product prices for commit."
            )
        session.commit()
        logger.info("Successfully saved data to DB and committed session.")
    except Exception as e:
        logger.error(f"Error during DB operation: {e}", exc_info=True)
        session.rollback()
        logger.info("Session rolled back due to error.")
        raise
    finally:
        session.close()
        logger.info("Session closed.")
