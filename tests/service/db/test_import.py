import asyncio
import importlib
from datetime import date
from decimal import Decimal

import pytest

# "import" is a keyword, so the module can't be imported with a statement
importer = importlib.import_module("service.db.import")

OLD_HEADER = (
    "store_id,product_id,price,unit_price,best_price_30,anchor_price,special_price"
)
NEW_HEADER = OLD_HEADER + ",available,special_sale_type,anchor_price_date"


class _FakeDatabase:
    def __init__(self):
        self.prices = []

    async def add_many_prices(self, prices):
        self.prices.extend(prices)
        return len(prices)


@pytest.fixture
def db(monkeypatch):
    fake = _FakeDatabase()
    monkeypatch.setattr(importer, "db", fake)
    return fake


def _import(tmp_path, *lines):
    path = tmp_path / "prices.csv"
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return asyncio.run(
        importer.process_prices(
            date(2026, 11, 17),
            path,
            chain_id=1,
            store_map={"S1": 10},
            chain_product_map={"P1": 20, "P2": 21, "P3": 22},
        )
    )


def test_new_columns_are_imported(db, tmp_path):
    _import(
        tmp_path,
        NEW_HEADER,
        "S1,P1,1.29,1.29,,1.19,0.99,1,Akcija,2025-05-02",
        "S1,P2,2.49,2.49,,,,0,,",
        "S1,P3,3.99,3.99,,,,,,",
    )

    p1, p2, p3 = db.prices
    assert (p1.available, p1.special_sale_type, p1.anchor_price_date) == (
        True,
        "Akcija",
        date(2025, 5, 2),
    )
    assert p1.anchor_price == Decimal("1.19")
    assert (p2.available, p2.special_sale_type, p2.anchor_price_date) == (
        False,
        None,
        None,
    )
    assert (p3.available, p3.special_sale_type, p3.anchor_price_date) == (
        None,
        None,
        None,
    )


def test_archives_without_new_columns_still_import(db, tmp_path):
    _import(tmp_path, OLD_HEADER, "S1,P1,1.29,1.29,,1.19,0.99")

    [price] = db.prices
    assert price.special_price == Decimal("0.99")
    assert (price.available, price.special_sale_type, price.anchor_price_date) == (
        None,
        None,
        None,
    )
