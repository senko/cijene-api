from decimal import Decimal

from crawler.store.models import Product, Store
from crawler.store.output import PRICE_COLUMNS, transform_products


def _product(product_id: str, available: bool | None, **kwargs) -> Product:
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
        **kwargs,
    )


def _store() -> Store:
    return Store(
        chain="plodine",
        store_id="030",
        name="Plodine Osijek",
        store_type="supermarket",
        city="Osijek",
        street_address="Ulica 1",
        items=[
            _product(
                "1",
                True,
                special_price=Decimal("0.99"),
                special_sale_type="Akcija",
                anchor_price=Decimal("1.19"),
                anchor_price_date="2025-05-02",
            ),
            _product("2", False),
            _product("3", None),
        ],
    )


def test_known_price_columns_keep_their_order():
    # prices.csv is a public format: new columns go at the end, existing
    # ones are never removed or reordered.
    assert PRICE_COLUMNS[:10] == [
        "store_id",
        "product_id",
        "price",
        "unit_price",
        "best_price_30",
        "anchor_price",
        "special_price",
        "available",
        "special_sale_type",
        "anchor_price_date",
    ]


def test_price_rows_carry_availability():
    _, _, prices = transform_products([_store()])

    # Compare as save_csv writes them; True == 1, so raw values would not
    # catch a bool leaking through as "True"/"False".
    assert [str(p["available"]) for p in prices] == ["1", "0", ""]


def test_price_rows_carry_special_sale_type_and_anchor_date():
    _, _, prices = transform_products([_store()])

    assert [p["special_sale_type"] for p in prices] == ["Akcija", "", ""]
    assert [p["anchor_price_date"] for p in prices] == ["2025-05-02", "", ""]
