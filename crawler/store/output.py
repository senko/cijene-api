import datetime
from csv import DictWriter
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from logging import getLogger
from os import makedirs
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED

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
    import os
    from sqlalchemy import create_engine, func, and_
    from sqlalchemy.orm import sessionmaker
    from decimal import Decimal

    from crawler.db.model import (
        Base,
        Chain,
        Product,
        ProductPrice,
        Store,
        StoreProduct,
    )

    def _get_barcode_or_replacement(
        barcode: str, chain: str, ext_product_id: str
    ) -> str:
        if not barcode.isdigit() or len(barcode) < 8:
            return f"{chain}:{ext_product_id}"
        return barcode

    def _normalize_decimal(val):
        if val is None:
            return None
        if not isinstance(val, Decimal):
            try:
                val = Decimal(val)
            except (ValueError, TypeError, InvalidOperation):
                return None
        return val.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    db_url = os.getenv("SQLALCHEMY_DATABASE_URI")
    if not db_url:
        logger.error("SQLALCHEMY_DATABASE_URI is not set")
        raise RuntimeError("SQLALCHEMY_DATABASE_URI is not set")

    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    session.connection(
        execution_options={
            "executemany_mode": "values_plus_batch",
            "executemany_values_page_size": 10000,
        }
    )

    logger.info("DB Processing started")

    try:
        existing_chains = {c.slug: c for c in session.query(Chain).all()}
        existing_products = {p.barcode: p for p in session.query(Product).all()}
        existing_stores = {
            (s.chain_id, s.ext_store_id): s for s in session.query(Store).all()
        }

        chains_to_add, products_to_add, stores_to_add = [], [], []
        store_products_to_add, product_prices_to_add, product_prices_to_update = (
            [],
            [],
            [],
        )

        for store in stores:
            chain_obj = existing_chains.get(store.chain)
            if not chain_obj:
                chain_obj = Chain(name=store.chain, slug=store.chain)
                chains_to_add.append(chain_obj)
                existing_chains[store.chain] = chain_obj
                logger.info(f"Adding new chain: {store.chain}")

        session.add_all(chains_to_add)
        session.flush()

        # Products and Stores
        for store in stores:
            chain_obj = existing_chains[store.chain]
            store_key = (chain_obj.id, store.store_id)
            db_store = existing_stores.get(store_key)
            if not db_store:
                db_store = Store(
                    chain_id=chain_obj.id,
                    ext_store_id=store.store_id,
                    ext_name=store.name,
                    ext_store_type=store.store_type,
                    ext_street_address=store.street_address,
                    ext_city=store.city,
                    ext_zipcode=store.zipcode or "",
                )
                stores_to_add.append(db_store)
                existing_stores[store_key] = db_store
                logger.info(f"Adding new store: {store.store_id} ({store.name})")

        session.add_all(stores_to_add)
        session.flush()

        # Pre-fetch store IDs to filter StoreProduct query
        store_ids_to_fetch = []
        for store_info in stores:
            chain_obj = existing_chains.get(store_info.chain)
            if chain_obj:
                store_key = (chain_obj.id, store_info.store_id)
                db_store = existing_stores.get(store_key)
                if db_store:
                    store_ids_to_fetch.append(db_store.id)

        existing_store_products = {}
        if store_ids_to_fetch:
            existing_store_products = {
                (sp.store_id, sp.ext_product_id): sp
                for sp in session.query(StoreProduct)
                .filter(StoreProduct.store_id.in_(store_ids_to_fetch))
                .all()
            }

        # Products and StoreProducts
        for store in stores:
            chain_obj = existing_chains[store.chain]
            db_store = existing_stores[(chain_obj.id, store.store_id)]

            for prod in store.items:
                prod_barcode = _get_barcode_or_replacement(
                    prod.barcode or "", store.chain, prod.product_id
                )

                # TODO: if some chain provide more data for the product, use it
                # like product name, brand, category, unit, quantity - update Product
                # TODO: Maybe we used syntetic barcode {chain}:{product_id} until now
                # and now chain sends valid barcode, so we need to update/merge/delete                
                product_obj = existing_products.get(prod_barcode)
                if not product_obj:
                    product_obj = Product(
                        barcode=prod_barcode,
                        ext_name=prod.product,
                        ext_brand=prod.brand,
                        ext_category=prod.category,
                        ext_unit=prod.unit,
                        ext_quantity=prod.quantity,
                    )
                    products_to_add.append(product_obj)
                    existing_products[prod_barcode] = product_obj

                store_product_key = (db_store.id, prod.product_id)
                db_store_product = existing_store_products.get(store_product_key)
                if not db_store_product:
                    db_store_product = StoreProduct(
                        store_id=db_store.id,
                        barcode=prod_barcode,
                        ext_product_id=prod.product_id,
                    )
                    store_products_to_add.append(db_store_product)
                    existing_store_products[store_product_key] = db_store_product
        logger.info(
            f"Adding {len(products_to_add)} new Products and {len(store_products_to_add)} new StoreProducts"
        )
        session.add_all(products_to_add + store_products_to_add)
        session.flush()

        # Product Prices handling
        for store in stores:
            chain_obj = existing_chains[store.chain]
            db_store = existing_stores[(chain_obj.id, store.store_id)]

            sp_keys = [(db_store.id, prod.product_id) for prod in store.items]
            store_product_objs = {k: existing_store_products[k] for k in sp_keys}

            sp_ids = [sp.id for sp in store_product_objs.values()]
            subq = (
                session.query(
                    ProductPrice.store_product_id,
                    func.max(ProductPrice.valid_date).label("max_date"),
                )
                .filter(
                    ProductPrice.store_product_id.in_(sp_ids),
                    ProductPrice.valid_date <= date,
                )
                .group_by(ProductPrice.store_product_id)
                .subquery()
            )

            last_prices = {
                pp.store_product_id: pp
                for pp in session.query(ProductPrice)
                .join(
                    subq,
                    and_(
                        ProductPrice.store_product_id == subq.c.store_product_id,
                        ProductPrice.valid_date == subq.c.max_date,
                    ),
                )
                .all()
            }

            count_changed = 0
            count_added = 0
            processed_products = set()  # 2+ for the same product in the same store!
            count_duplicates = 0
            for prod in store.items:
                prod_key = (store.store_id, prod.product_id)
                if prod_key in processed_products:
                    count_duplicates += 1
                    logger.debug(
                        f" **** ERROR: Skipping duplicate product {prod.product_id} for store {store.store_id}"
                    )
                    continue
                processed_products.add(prod_key)
                sp = store_product_objs[(db_store.id, prod.product_id)]
                new_price = _normalize_decimal(prod.price)
                new_unit_price = _normalize_decimal(prod.unit_price)
                new_best_price_30 = _normalize_decimal(prod.best_price_30)
                new_anchor_price = _normalize_decimal(prod.anchor_price)
                new_special_price = _normalize_decimal(prod.special_price)

                last_pp = last_prices.get(sp.id)
                if last_pp and last_pp.valid_date == date:
                    changes = []
                    for field, new_val in [
                        ("price", new_price),
                        ("unit_price", new_unit_price),
                        ("best_price_30", new_best_price_30),
                        ("anchor_price", new_anchor_price),
                        ("special_price", new_special_price),
                    ]:
                        old_val = _normalize_decimal(getattr(last_pp, field))
                        if old_val != new_val:
                            setattr(last_pp, field, new_val)
                            changes.append(f"{field} {old_val}->{new_val}")

                    if changes:
                        logger.debug(
                            f"Detected changes store={store.chain}, store_id={store.store_id}, "
                            f"product_id={prod.product_id}, date={date}: {'; '.join(changes)}"
                        )
                        count_changed += 1
                        product_prices_to_update.append(last_pp)
                else:
                    if not last_pp or any(
                        [
                            _normalize_decimal(last_pp.price) != new_price,
                            _normalize_decimal(last_pp.unit_price) != new_unit_price,
                            _normalize_decimal(last_pp.best_price_30)
                            != new_best_price_30,
                            _normalize_decimal(last_pp.anchor_price)
                            != new_anchor_price,
                            _normalize_decimal(last_pp.special_price)
                            != new_special_price,
                        ]
                    ):
                        new_pp = ProductPrice(
                            store_product_id=sp.id,
                            valid_date=date,
                            price=new_price,
                            unit_price=new_unit_price,
                            best_price_30=new_best_price_30,
                            anchor_price=new_anchor_price,
                            special_price=new_special_price,
                        )
                        count_added += 1
                        product_prices_to_add.append(new_pp)
            if count_changed > 0 or count_added > 0:
                logger.info(
                    f"Processed {len(store.items)} products for store {store.store_id} "
                    f"({store.chain}): {count_changed} prices updated, {count_added} new prices added"
                )
            if count_duplicates > 0:
                logger.info(
                    f"**** ERROR: Found {count_duplicates} duplicate products for store "
                    f"{store.store_id} ({store.chain}), skipping them"
                )
        session.add_all(product_prices_to_add + product_prices_to_update)
        session.commit()
        logger.info("DB Processing completed successfully")

    except Exception as e:
        logger.error(f"Error during DB processing: {e}", exc_info=True)
        session.rollback()
        raise

    finally:
        session.close()
