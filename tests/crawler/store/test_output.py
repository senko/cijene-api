from decimal import Decimal

from crawler.store.models import Product, Store
from crawler.store.output import PRICE_COLUMNS, save_csv, transform_products


def _product(product_id: str, available: bool | None) -> Product:
    return Product(
        product="Mlijeko",
        product_id=product_id,
        brand="Dukat",
        quantity="1",
        unit="l",
        price=Decimal("1.29"),
        unit_price=Decimal("1.29"),
        barcode="3850102030405",
        category="Mlijeko",
        available=available,
    )


def _store() -> Store:
    return Store(
        chain="plodine",
        store_id="030",
        name="Plodine Osijek",
        store_type="supermarket",
        city="Osijek",
        street_address="Ulica 1",
        items=[_product("1", True), _product("2", False), _product("3", None)],
    )


def test_available_is_a_price_column():
    assert PRICE_COLUMNS[-1] == "available"


def test_price_rows_carry_availability():
    _, _, prices = transform_products([_store()])

    assert [p["available"] for p in prices] == [1, 0, ""]


def test_csv_writes_one_zero_and_empty(tmp_path):
    _, _, prices = transform_products([_store()])

    save_csv(tmp_path / "prices.csv", prices, PRICE_COLUMNS)

    lines = (tmp_path / "prices.csv").read_text().splitlines()
    assert lines[0].endswith(",available")
    assert [line.rsplit(",", 1)[1] for line in lines[1:]] == ["1", "0", ""]
