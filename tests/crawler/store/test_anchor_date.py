"""Tests for where the anchor price date comes from: the product, the header, or nowhere."""

import datetime

import pytest

from crawler.store.base import BaseCrawler
from crawler.store.roto import RotoCrawler


class _Crawler(BaseCrawler):
    CHAIN = "test"
    BASE_URL = "https://example.test"
    PRICE_MAP = {
        "price": ("MPC", True),
        "unit_price": ("Cijena za JM", False),
        "anchor_price": ("Sidrena cijena", False),
    }
    FIELD_MAP = {
        "product": ("Naziv", True),
        "product_id": ("Šifra", True),
        "brand": ("Marka", False),
        "quantity": ("Količina", False),
        "unit": ("JM", False),
        "barcode": ("Barkod", False),
        "category": ("Kategorija", False),
        "anchor_price_date": ("Datum sidrene cijene", False),
    }
    REQUIRED_COLUMNS = []


class _DatedCrawler(_Crawler):
    ANCHOR_PRICE_DATE = datetime.date(2025, 5, 2)


HEADER = "Naziv,Šifra,MPC,Sidrena cijena,Datum sidrene cijene\n"


@pytest.mark.parametrize(
    "crawler_class, row, expected",
    [
        # The header states the date and the product doesn't
        (_DatedCrawler, "Mlijeko,1,1.29,1.19,", "2025-05-02"),
        # A date published per product wins over the header
        (_DatedCrawler, "Mlijeko,1,1.29,1.19,27.04.2026", "2026-04-27"),
        (_Crawler, "Mlijeko,1,1.29,1.19,27.04.2026", "2026-04-27"),
        # Neither states it: left empty, not guessed
        (_Crawler, "Mlijeko,1,1.29,1.19,", None),
        # Unparseable per-product date falls back to the header
        (_DatedCrawler, "Mlijeko,1,1.29,1.19,uskoro", "2025-05-02"),
        # No anchor price, no date
        (_DatedCrawler, "Mlijeko,1,1.29,,02.05.2025", None),
    ],
)
def test_anchor_price_date(crawler_class, row, expected):
    [product] = crawler_class().parse_csv(HEADER + row + "\n")

    assert product.anchor_price_date == expected


def test_roto_falls_back_to_the_newer_anchor_price():
    header = (
        "ŠIFRA ARTIKLA;Naziv artikla;BRAND;Barkod;Jedinica mjere;MPC;"
        "Cijena za jedinicu mjere;sidrena cijena na 2.5.2025.;"
        "sidrena cijena na 10.9.2026."
    )
    rows = [
        "1;Mlijeko;Dukat;3850102030405;l;1,29;1,29;1,09;1,19",
        "2;Jogurt;Dukat;3850102030412;kom;0,99;0,99;;0,95",
    ]

    old, new = RotoCrawler().parse_csv("\n".join([header, *rows]), delimiter=";")

    assert (str(old.anchor_price), old.anchor_price_date) == ("1.09", "2025-05-02")
    assert (str(new.anchor_price), new.anchor_price_date) == ("0.95", "2026-09-10")
