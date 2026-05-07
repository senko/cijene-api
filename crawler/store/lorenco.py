import datetime
import logging
import re
from csv import DictReader
from typing import Any
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from crawler.store.models import Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)

CSV_DATE_PATTERN = re.compile(r"Cijen[a-z]+-(\d{2}\.\d{2}\.\d{4})\.csv$")


class LorencoCrawler(BaseCrawler):
    """Crawler for Lorenco store prices."""

    CHAIN = "lorenco"
    BASE_URL = "https://lorenco.hr"
    INDEX_URL = f"{BASE_URL}/dnevne-cijene/"

    # Lorenco has global prices, not per-store prices
    STORE_ID = "all"
    STORE_NAME = "Lorenco"

    # Map CSV columns to price fields
    PRICE_MAP = {
        "unit_price": ("MpcJmj", False),
        "price": ("MPC", False),
        "anchor_price": ("CijenaSid", False),
    }

    # Map CSV columns to other fields
    FIELD_MAP = {
        "product": ("Naziv", True),
        "barcode": ("Barkod", True),
        "unit": ("JMjere", False),
    }

    def get_csv_url(self, soup: BeautifulSoup, date: datetime.date) -> str | None:
        """Find the CSV URL for the given date from the index page."""
        hr_date = date.strftime("%d.%m.%Y")

        for anchor in soup.select("a[href$='.csv']"):
            href = anchor.get("href")
            if not isinstance(href, str):
                continue

            path = urlparse(href).path
            m = CSV_DATE_PATTERN.search(path)
            if m and m.group(1) == hr_date:
                return href

        return None

    def read_csv(self, text: str, delimiter: str = ",") -> DictReader:
        # Lorenco publishes two CSV variants: an older one with a single "MPC"
        # column, and a newer one that splits price into "Cijena" (whole) +
        # "CijenaDec" (hundredths). Declare a synthetic "MPC" fieldname so
        # base header validation passes for both; parse_csv_row fills the
        # value per row from whichever columns are present.
        reader = super().read_csv(text, delimiter=delimiter)
        if reader.fieldnames and "MPC" not in reader.fieldnames:
            reader.fieldnames = list(reader.fieldnames) + ["MPC"]
        return reader

    def parse_csv_row(self, row: dict) -> Any:
        # Lorenco's newer CSV variant has no "MPC" column; price is split
        # across "Cijena" (whole part) + "CijenaDec" (hundredths), e.g.
        # 1;40 → 1.40 EUR. Reassemble it here so the rest of the pipeline
        # (PRICE_MAP -> "MPC") works for both variants without re-parsing
        # the file. Detection is column-based, not filename-based, because
        # the two variants have appeared under both filename prefixes.
        if not (row.get("MPC") or "").strip():
            whole = (row.get("Cijena") or "").strip()
            dec = (row.get("CijenaDec") or "").strip()
            row["MPC"] = f"{whole},{dec.zfill(2)}" if whole else ""
        return super().parse_csv_row(row)

    def fix_product_data(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        Override base class method to handle missing fields specific to Lorenco.
        """
        # Set default values for fields not available in Lorenco CSV
        data["product_id"] = data["barcode"]
        data["brand"] = ""
        data["category"] = ""
        data["quantity"] = ""

        # Call parent method to apply common fixups
        return super().fix_product_data(data)

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Main method to fetch and parse all product and price info.

        Args:
            date: The date to search for in the price list.

        Returns:
            List with a single Store object containing all products.
        """
        html = self.fetch_text(self.INDEX_URL)
        soup = BeautifulSoup(html, "html.parser")
        csv_url = self.get_csv_url(soup, date)

        if not csv_url:
            logger.info(f"No price list found for {date}")
            return []

        logger.info(f"Fetching CSV from: {csv_url}")
        csv_content = self.fetch_text(csv_url, encodings=["windows-1250"])

        if not csv_content:
            logger.warning(f"No content found at {csv_url}")
            return []

        products = self.parse_csv(csv_content, delimiter=";")

        if not products:
            logger.warning(f"No products found for date {date}")
            return []

        store = Store(
            chain=self.CHAIN,
            store_type="store",
            store_id=self.STORE_ID,
            name=self.STORE_NAME,
            street_address="",
            zipcode="",
            city="",
            items=products,
        )

        return [store]


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = LorencoCrawler()
    stores = crawler.crawl(datetime.date.today())
    print(stores[0])
    if stores[0].items:
        print(stores[0].items[0])
