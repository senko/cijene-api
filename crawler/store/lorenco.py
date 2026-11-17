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

# Seen: "Cijenik-DD.MM.YYYY.csv", "Cijene-...", "Cjenik-...", "Cijenik12.11.2026.csv",
# and since 2026-11-17 "Prodavaonica_ZAGREBACKA_12_PAG_PJ01_5_17.11.2026_13_11.csv"
CSV_DATE_PATTERN = re.compile(
    r"(?:Ci?jen[a-z]+-?|_)(\d{2}\.\d{2}\.\d{4})(?:_\d{2}_\d{2})?\.csv$"
)

# Store code in the file names used since 2026-11-17, e.g. "..._PAG_PJ01_5_..."
STORE_CODE_PATTERN = re.compile(r"_(PJ\d+)_")

# The newer variant spells the anchor date out: "Cijena na datum 02.05.2025: 1,87 EUR"
ANCHOR_DATE_PATTERN = re.compile(r"\d{1,2}\.\d{1,2}\.\d{4}")


class LorencoCrawler(BaseCrawler):
    """Crawler for Lorenco store prices."""

    CHAIN = "lorenco"
    BASE_URL = "https://lorenco.hr"
    INDEX_URL = f"{BASE_URL}/dnevne-cijene/"

    # Lorenco publishes one price list, for its store PJ01 in Pag. The file
    # names give the store since 2026-11-17. The ID stays "all" so the store
    # keeps its price history; change it when a second store appears.
    STORE_ID = "all"
    STORE_CODE = "PJ01"
    STORE_NAME = "Lorenco"
    STORE_TYPE = "prodavaonica"
    STREET_ADDRESS = "Zagrebačka 12"
    CITY = "Pag"
    ZIPCODE = "23250"

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
        # Published per product: the date of the anchor price
        "anchor_price_date": ("DatumSid", False),
    }

    REQUIRED_COLUMNS = [
        "MpcJmj",
        "MPC",
        "CijenaSid",
        "Naziv",
        "Barkod",
        "JMjere",
    ]

    OPTIONAL_COLUMNS = [
        "DatumSid",
    ]

    # A full price list has ~20k products. Now and then the chain publishes
    # only a few changed prices for the day instead. The API serves only the
    # chain's latest load, so importing such a file would hide the full list.
    MIN_PRODUCTS = 1000

    def get_csv_url(self, soup: BeautifulSoup, date: datetime.date) -> str | None:
        """Find the CSV URL for the given date from the index page."""
        hr_date = date.strftime("%d.%m.%Y")
        urls = []

        for anchor in soup.select("a[href$='.csv']"):
            href = anchor.get("href")
            if not isinstance(href, str):
                continue

            path = urlparse(href).path
            m = CSV_DATE_PATTERN.search(path)
            if m and m.group(1) == hr_date:
                urls.append(href)

        # Price lists of another store must not be read as store PJ01
        other_stores = [
            url
            for url in urls
            if (m := STORE_CODE_PATTERN.search(url)) and m.group(1) != self.STORE_CODE
        ]
        if other_stores:
            logger.warning(
                f"Lorenco published price lists for other stores, not crawled: "
                f"{other_stores}"
            )

        return next((url for url in urls if url not in other_stores), None)

    def read_csv(self, text: str, delimiter: str = ",") -> DictReader:
        # Since 2026-11-17 the file starts with a few lines of company and
        # store info before the header ("LORENCO d.o.o.", "OIB: ...",
        # "Prodavaonica 01", blank). Skip to the header row.
        lines = text.splitlines()
        header = next(
            (
                i
                for i, line in enumerate(lines)
                if line.startswith(f"Barkod{delimiter}")
            ),
            0,
        )
        if header:
            text = "\n".join(lines[header:])

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

        m = ANCHOR_DATE_PATTERN.search(row.get("DatumSid") or "")
        row["DatumSid"] = m.group(0) if m else ""
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

        if len(products) < self.MIN_PRODUCTS:
            logger.warning(
                f"Skipping {csv_url}: {len(products)} products, "
                f"expected at least {self.MIN_PRODUCTS} for a full price list"
            )
            return []

        store = Store(
            chain=self.CHAIN,
            store_type=self.STORE_TYPE,
            store_id=self.STORE_ID,
            name=self.STORE_NAME,
            street_address=self.STREET_ADDRESS,
            zipcode=self.ZIPCODE,
            city=self.CITY,
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
