import csv
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import List

from .models import Store, Product


def load_from_csv(chain: str, date: date, csv_dir: Path) -> List[Store]:
    """
    Load store and product data from CSV files for a given chain and date.

    Args:
        chain: Retail chain slug matching the subdirectory name.
        date: Date for which to load the CSV data.
        csv_dir: Root directory containing date/chain subfolders.

    Returns:
        List of Store models populated with Product items.
    """
    root = Path(csv_dir) / date.strftime("%Y-%m-%d") / chain
    if not root.exists():
        raise FileNotFoundError(f"CSV directory not found: {root}")

    # Load store metadata
    stores: dict[str, Store] = {}
    stores_csv = root / "stores.csv"
    with stores_csv.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            store_id = row.get("store_id")
            if not store_id:
                continue
            stores[store_id] = Store(
                chain=chain,
                store_id=store_id,
                name=store_id,
                store_type=row.get("type", ""),
                city=row.get("city", ""),
                street_address=row.get("address", ""),
                zipcode=row.get("zipcode", ""),
            )

    # Load product metadata
    products: dict[str, dict] = {}
    products_csv = root / "products.csv"
    with products_csv.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            prod_id = row.get("product_id")
            if not prod_id:
                continue
            products[prod_id] = row

    # Helper to parse decimals
    def _parse_decimal(val: str) -> Decimal | None:
        if val is None or val == "":
            return None
        try:
            return Decimal(val)
        except (InvalidOperation, ValueError):
            return None

    # Load prices and attach Product items to stores
    prices_csv = root / "prices.csv"
    with prices_csv.open(newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            store_id = row.get("store_id")
            prod_id = row.get("product_id")
            if not store_id or not prod_id:
                continue
            store = stores.get(store_id)
            prod_meta = products.get(prod_id)
            if store is None or prod_meta is None:
                continue

            price = _parse_decimal(row.get("price", "")) or Decimal("0")
            unit_price = _parse_decimal(row.get("unit_price", ""))
            best_price_30 = _parse_decimal(row.get("best_price_30", ""))
            anchor_price = _parse_decimal(row.get("anchor_price", ""))
            special_price = _parse_decimal(row.get("special_price", ""))

            item = Product(
                product=prod_meta.get("name", ""),
                product_id=prod_id,
                brand=prod_meta.get("brand", ""),
                quantity=prod_meta.get("quantity", ""),
                unit=prod_meta.get("unit", ""),
                price=price,
                unit_price=unit_price,
                barcode=prod_meta.get("barcode", ""),
                category=prod_meta.get("category", ""),
                best_price_30=best_price_30,
                anchor_price=anchor_price,
                special_price=special_price,
            )
            store.items.append(item)

    return list(stores.values())
