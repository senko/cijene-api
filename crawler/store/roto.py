from __future__ import annotations

import datetime
import logging
import re
from typing import NamedTuple
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from crawler.store.base import BaseCrawler
from crawler.store.models import Product, Store

logger = logging.getLogger(__name__)


class RotoCrawler(BaseCrawler):
    """
    Crawler for Roto store prices.
    https://www.rotodinamic.hr/cjenici/
    """

    CHAIN = "roto"
    BASE_URL = "https://www.rotodinamic.hr"
    INDEX_URL = f"{BASE_URL}/cjenici/"

    ANCHOR_PRICE_COLUMN = "sidrena cijena na 2.5.2025."
    PRICE_MAP = {
        "price": ("MPC", True),
        "unit_price": ("Cijena za jedinicu mjere", True),
        "special_price": ("MPC za vrijeme posebnog oblika prodaje", False),
        "best_price_30": ("Najniža cijena u posljednjih 30 dana", False),
        "anchor_price": (ANCHOR_PRICE_COLUMN, False),
    }

    FIELD_MAP = {
        "product": ("Naziv artikla", False),
        "product_id": ("Šifra artikla", True),
        "brand": ("BRAND", False),
        "quantity": ("neto koli?ina", False),
        "unit": ("Jedinica mjere", False),
        "barcode": ("Barkod", False),
        "category": ("Kategorija proizvoda", False),
    }

    def get_csv_url(self, soup: BeautifulSoup, date: datetime.date) -> str:
        anchors = soup.select("a.cjenici-table-row")
        hr_date = date.strftime("%d.%m.%Y")

        for anchor in anchors:
            url = anchor.attrs["href"]
            assert isinstance(url, str)
            url_date = urlparse(url).path.split(",")[-2].strip()
            if url_date == hr_date:
                return url

        raise ValueError(f"No price list found for {date}")

    def get_all_products(self, date: datetime.date) -> list[Store]:
        html_content = self.fetch_text(self.INDEX_URL)
        soup = BeautifulSoup(html_content, "html.parser")
        csv_url = self.get_csv_url(soup, date)
        addresses = self.parse_store_addresses(soup)

        # Roto has the same prices for all stores
        products = self.get_store_products(csv_url)
        return list(self.get_stores(csv_url, products, addresses))

    def get_store_products(self, csv_url: str) -> list[Product]:
        try:
            content = self.fetch_text(csv_url, encodings=["cp1250"])
            return self.parse_csv(content, delimiter=";")
        except Exception:
            logger.exception(f"Failed to get store prices from {csv_url}")
            return []

    def get_stores(
        self,
        csv_url: str,
        products: list[Product],
        addresses: dict[str, Address],
    ):
        # Extract store ids and names from the CSV file
        matches = []
        parts = urlparse(csv_url).path.split(",")
        for part in parts:
            part = part.strip()
            if re.match("D[0-9]+ ", part):
                store_id, name = part.split(" ")
                matches.append((store_id, name))

        # Ideally the count will match the addresses extracted from the web page
        if len(matches) != len(addresses):
            logger.warning(
                f"Store count mismatch: found {len(matches)} stores in CSV name and {len(addresses)} stores on the roto web page."
            )

        for store_id, name in matches:
            if name in addresses:
                street_address, zipcode, city = addresses[name]
            else:
                street_address, zipcode, city = "", "", ""
                logger.warning(f"Unable to find address for {store_id} {name}")

            yield Store(
                chain=self.CHAIN,
                store_type="Cash & Carry",
                store_id=store_id,
                name=f"Cash & Carry {name}",
                street_address=street_address,
                zipcode=zipcode,
                city=city,
                items=products,
            )

    def parse_store_addresses(self, soup: BeautifulSoup) -> dict[str, Address]:
        """Returns store address indexed by store name"""
        addresses = {}

        spans = soup.select(".container > div.mBottom50 > p > span.bold")
        for span in spans:
            name = span.text
            assert span.parent is not None
            _, address = span.parent
            assert isinstance(address, str)
            street_address, zipcode_city = address.strip(" -").split(", ")
            zipcode, city = zipcode_city.split(" ", maxsplit=1)

            # Remove unwanted address prefix
            to_strip = "Jankomir- "
            if street_address.startswith(to_strip):
                street_address = street_address[len(to_strip) :]

            if name in addresses:
                logger.warning(f"Duplicate store: {name}")

            addresses[name] = Address(street_address, zipcode, city)

        return addresses


class Address(NamedTuple):
    street_address: str
    zipcode: str
    city: str


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    crawler = RotoCrawler()
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    stores = crawler.get_all_products(yesterday)
    from pprint import pp

    pp(stores[0])
