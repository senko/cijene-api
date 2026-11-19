import datetime
import logging
import re
from csv import DictReader
from typing import Any
from urllib.parse import unquote, urlparse

from bs4 import BeautifulSoup

from crawler.store.models import Store

from .base import BaseCrawler, CrawlerBlocked

logger = logging.getLogger(__name__)

# Seen: "Cijenik-DD.MM.YYYY.csv", "Cijene-...", "Cjenik-...", "Cijenik12.11.2026.csv",
# and since 2026-11-17 "Prodavaonica_ZAGREBACKA_12_PAG_PJ01_5_17.11.2026_13_11.csv".
# A file uploaded again under the same name gets "-1", "-1-1", ... appended.
CSV_DATE_PATTERN = re.compile(
    r"(?:Ci?jen[a-z]+-?|_)(\d{2}\.\d{2}\.\d{4})(?:_(\d{2}_\d{2}))?(?:-\d+)*\.csv$"
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

    # Until 2026-11-17 Lorenco published one price list, for its store PJ01
    # in Pag, imported as store "all". Since 2026-11-19 it publishes one per
    # store, with the store code in the file name. PJ01 keeps the ID "all" so
    # its price history continues; the other stores use their code.
    MAIN_STORE_CODE = "PJ01"
    MAIN_STORE_ID = "all"

    # The file names spell the address without diacritics ("ANTE-STARCEVICA"),
    # so known stores are listed here: (type, street address, city, zip code).
    # Zip codes are from GeoNames, which gives 23250 for all of these places.
    STORES = {
        "PJ01": ("prodavaonica", "Zagrebačka 12", "Pag", "23250"),
        "PJ03": ("prodavaonica", "Golija 23", "Pag", "23250"),
        "PJ04": ("prodavaonica", "Ante Starčevića 39", "Povljana", "23250"),
        "PJ06": ("prodavaonica", "Šimuni 36", "Kolan", "23250"),
        "PJ09": ("prodavaonica", "Zadarska 15", "Povljana", "23250"),
        "PJ10": ("prodavaonica", "Put Kolana 42", "Kolan", "23250"),
    }

    # Points of sale that are not grocery shops, by store code
    SKIPPED_STORES = {
        "PJ02": "hardware and household goods",
    }

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

    def get_csv_urls(self, soup: BeautifulSoup, date: datetime.date) -> dict[str, str]:
        """
        Find the price list of each store for the given date on the index page.

        Older file names have no store code; they are the main store's. When a
        store has several files for the date, the one with the latest time in
        its name wins, and among equal times the first one listed (the index
        lists the newest uploads first).

        Returns:
            CSV URL by store code
        """
        hr_date = date.strftime("%d.%m.%Y")
        candidates: dict[str, tuple[str, str]] = {}

        for anchor in soup.select("a[href$='.csv']"):
            href = anchor.get("href")
            if not isinstance(href, str):
                continue

            path = unquote(urlparse(href).path)
            m = CSV_DATE_PATTERN.search(path)
            if not m or m.group(1) != hr_date:
                continue

            code_match = STORE_CODE_PATTERN.search(path)
            code = code_match.group(1) if code_match else self.MAIN_STORE_CODE
            time = m.group(2) or ""
            if code not in candidates or time > candidates[code][0]:
                candidates[code] = (time, href)

        return {code: href for code, (_time, href) in candidates.items()}

    def get_store(self, code: str, csv_url: str) -> Store:
        """
        Build the store for a store code, from STORES or else the file name.

        File name: "Prodavaonica_PUT-KOLANA_42_KOLAN_PJ10_2_19.11.2026_06_46.csv"
        """
        if code in self.STORES:
            store_type, street_address, city, zipcode = self.STORES[code]
        else:
            logger.warning(f"Unknown Lorenco store {code} in {csv_url}")
            parts = unquote(urlparse(csv_url).path).rsplit("/", 1)[-1].split("_")
            index = parts.index(code) if code in parts else len(parts)
            store_type = parts[0].lower() if index > 0 else ""
            street_address = " ".join(parts[1 : index - 1]).replace("-", " ").title()
            city = parts[index - 1].title() if index > 1 else ""
            zipcode = ""

        return Store(
            chain=self.CHAIN,
            store_type=store_type,
            store_id=self.MAIN_STORE_ID if code == self.MAIN_STORE_CODE else code,
            name=f"Lorenco {code}",
            street_address=street_address,
            zipcode=zipcode,
            city=city,
            items=[],
        )

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
            List of Store objects, one per store with a price list.
        """
        html = self.fetch_text(self.INDEX_URL)
        soup = BeautifulSoup(html, "html.parser")
        csv_urls = self.get_csv_urls(soup, date)

        if not csv_urls:
            logger.info(f"No price list found for {date}")
            return []

        stores = []
        for code, csv_url in sorted(csv_urls.items()):
            if code in self.SKIPPED_STORES:
                logger.info(
                    f"Skipping Lorenco store {code} "
                    f"({self.SKIPPED_STORES[code]}): not a grocery shop"
                )
                continue

            try:
                store = self.get_store(code, csv_url)

                logger.info(f"Fetching CSV from: {csv_url}")
                csv_content = self.fetch_text(csv_url, encodings=["windows-1250"])
                if not csv_content:
                    logger.warning(f"No content found at {csv_url}")
                    continue

                products = self.parse_csv(csv_content, delimiter=";")
            except CrawlerBlocked:
                raise
            except Exception as e:
                logger.error(f"Error processing {csv_url}: {e}", exc_info=True)
                continue

            if len(products) < self.MIN_PRODUCTS:
                logger.warning(
                    f"Skipping {csv_url}: {len(products)} products, "
                    f"expected at least {self.MIN_PRODUCTS} for a full price list"
                )
                continue

            store.items = products
            stores.append(store)

        return stores


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = LorencoCrawler()
    stores = crawler.crawl(datetime.date.today())
    print(stores[0])
    if stores[0].items:
        print(stores[0].items[0])
