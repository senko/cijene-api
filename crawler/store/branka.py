import datetime
import logging
import re
from urllib.parse import unquote

from bs4 import BeautifulSoup

from crawler.store.models import Product, Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)

# Legacy filename, used until 2026-09-22.
# e.g., Hipermarket070426.csv -> ("Hipermarket", "07", "04", "26")
LEGACY_FILENAME_PATTERN = re.compile(
    r"(Hipermarket|Supermarket)(\d{2})(\d{2})(\d{2})\.csv"
)

# Since 2026-09-23 Branka uses the naming NN 101/2026 asks for, which carries
# the store form, address, store code, storage number and a timestamp, e.g.
# 2026266_Hipermarket_BRANKA_Optujska_70_P02_2392026_0735.csv
# Note the store form is spelled "Super_market" there.
STORE_TYPE_PATTERN = re.compile(r"(hiper|super)[_ ]?market", re.IGNORECASE)

# Day and month in the new filenames are not zero-padded, so the split between
# them depends on the length of the date segment. Only the 7-character case is
# ambiguous, and both of its readings can be real dates ("1122026" is either
# 11.2.2026 or 1.12.2026), which is why candidates are filtered by plausibility
# and then matched against the requested date.
DATE_SEGMENT_SPLITS = {
    6: ((1, 1, 4),),
    7: ((2, 1, 4), (1, 2, 4)),
    8: ((2, 2, 4),),
}

# Hardcoded store info — Branka has exactly 2 locations, both in Varazdin
STORES = {
    "Hipermarket": Store(
        chain="branka",
        store_id="hipermarket",
        name="Branka Hipermarket",
        store_type="hipermarket",
        city="Varazdin",
        street_address="Optujska ulica 70",
        zipcode="42000",
    ),
    "Supermarket": Store(
        chain="branka",
        store_id="supermarket",
        name="Branka Supermarket",
        store_type="supermarket",
        city="Varazdin",
        street_address="Zrinskih i Frankopana 2",
        zipcode="42000",
    ),
}


class BrankaCrawler(BaseCrawler):
    """
    Crawler for Branka retail store prices.

    Branka publishes daily CSV price lists for two store locations
    (Hipermarket and Supermarket, both in Varazdin) on their /cjenik page.
    CSV files are encoded in windows-1250 and use semicolon delimiters.
    """

    CHAIN = "branka"
    BASE_URL = "https://www.branka.hr"
    INDEX_URL = "https://www.branka.hr/cjenik"

    # How far back a new-style filename may plausibly be dated. Deliberately
    # generous; it exists to reject misparses, not to bound the index.
    MAX_FILE_AGE_DAYS = 40

    PRICE_MAP = {
        "price": ("MPC", True),
        "unit_price": ("MPC", True),
        "anchor_price": ("SIDRENA_CIJENA_NA_02_05_25", False),
    }

    FIELD_MAP = {
        "product_id": ("SIFRA", True),
        "product": ("NAZIV", True),
        "brand": ("MARKA", False),
        "quantity": ("NETO_KOLICINA", False),
        "unit": ("JEDINICA_MJERE", False),
        "barcode": ("BARKOD", False),
        "category": ("NAZIV_KATEGORIJE", False),
    }

    REQUIRED_COLUMNS = [
        "MPC",
        "SIDRENA_CIJENA_NA_02_05_25",
        "SIFRA",
        "NAZIV",
        "MARKA",
        "JEDINICA_MJERE",
        "BARKOD",
    ]

    OPTIONAL_COLUMNS = [
        "NETO_KOLICINA",
        "NAZIV_KATEGORIJE",
    ]

    def parse_index(self, content: str) -> list[str]:
        """
        Parse the index page HTML to extract all CSV download URLs.

        Args:
            content: HTML content of the /cjenik page

        Returns:
            List of absolute CSV URLs found on the page
        """
        soup = BeautifulSoup(content, "html.parser")
        urls = []

        for link in soup.select('a[href$=".csv"]'):
            href = str(link.get("href"))
            if not href.startswith("http"):
                href = (
                    f"{self.BASE_URL}{href}"
                    if href.startswith("/")
                    else f"{self.BASE_URL}/{href}"
                )
            urls.append(href)

        return urls

    @staticmethod
    def parse_date_segment(segment: str) -> set[datetime.date]:
        """
        Read the unpadded date segment of a new-style filename.

        Args:
            segment: Digits between the store code and the time, e.g. "2392026"

        Returns:
            Every real calendar date the segment could denote, which is more
            than one only for the ambiguous 7-character case.
        """
        dates = set()
        for day_len, month_len, year_len in DATE_SEGMENT_SPLITS.get(len(segment), ()):
            day = segment[:day_len]
            month = segment[day_len : day_len + month_len]
            year = segment[day_len + month_len : day_len + month_len + year_len]
            try:
                dates.add(datetime.date(int(year), int(month), int(day)))
            except ValueError:
                continue
        return dates

    def candidate_dates(self, url: str) -> set[datetime.date]:
        """
        Determine which dates a price list URL could be for.

        Legacy filenames carry an unambiguous zero-padded date and are accepted
        as-is, so historical backfills keep working. New-style filenames are
        ambiguous, so their candidates are additionally restricted to a
        generous recent window: anything outside it is a misparse rather than a
        real date, which also guards against picking up the wrong segment.

        Args:
            url: CSV file URL

        Returns:
            Set of plausible dates, empty if the name isn't recognized.
        """
        name = unquote(url).rsplit("/", 1)[-1]

        legacy = LEGACY_FILENAME_PATTERN.search(name)
        if legacy:
            day, month, year = (
                int(legacy.group(2)),
                int(legacy.group(3)),
                int(legacy.group(4)),
            )
            try:
                return {datetime.date(2000 + year, month, day)}
            except ValueError:
                return set()

        stem = name[:-4] if name.lower().endswith(".csv") else name
        segments = stem.split("_")
        if len(segments) < 2:
            return set()

        # Take the date by position rather than scanning for digit runs: the
        # name holds several other numeric segments, and the leading one parses
        # as a valid date under two of the splits.
        today = datetime.date.today()
        earliest = today - datetime.timedelta(days=self.MAX_FILE_AGE_DAYS)
        latest = today + datetime.timedelta(days=1)
        return {
            found
            for found in self.parse_date_segment(segments[-2])
            if earliest <= found <= latest
        }

    def get_index(self, date: datetime.date) -> list[str]:
        """
        Fetch the index page and return CSV URLs matching the given date.

        Args:
            date: The date for which to find price list CSVs

        Returns:
            List of CSV URLs (up to 2, one per store) for the given date
        """
        content = self.fetch_text(self.INDEX_URL)
        all_urls = self.parse_index(content)

        matching = [url for url in all_urls if date in self.candidate_dates(url)]

        logger.info(f"Found {len(matching)} CSV files for date {date}")
        return matching

    def parse_store_info(self, url: str) -> Store:
        """
        Determine store type from the CSV URL and return the corresponding Store.

        Args:
            url: CSV file URL containing 'Hipermarket' or 'Supermarket' in the path

        Returns:
            Store object with hardcoded location info

        Raises:
            ValueError: If the URL doesn't match a known store type
        """
        name = unquote(url).rsplit("/", 1)[-1]
        m = STORE_TYPE_PATTERN.search(name)
        if not m:
            raise ValueError(f"Cannot parse store type from URL: {url}")

        # Normalize the new "Super_market" spelling onto the keys used here
        store_type = "Hipermarket" if m.group(1).lower() == "hiper" else "Supermarket"
        if store_type not in STORES:
            raise ValueError(f"Unknown store type: {store_type}")

        return STORES[store_type].model_copy()

    def get_store_prices(self, csv_url: str) -> list[Product]:
        """
        Download and parse a CSV price list for a single store.

        Args:
            csv_url: URL of the CSV file to download and parse

        Returns:
            List of Product objects parsed from the CSV
        """
        content = self.fetch_text(csv_url, encodings=["windows-1250"])
        return self.parse_csv(content, delimiter=";")

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Fetch and parse price data for all Branka stores for the given date.

        Args:
            date: The date for which to fetch price data

        Returns:
            List of Store objects (up to 2) each containing their products
        """
        csv_urls = self.get_index(date)

        if not csv_urls:
            logger.warning(f"No Branka data available for {date}")
            return []

        stores = []
        for url in csv_urls:
            try:
                store = self.parse_store_info(url)
                products = self.get_store_prices(url)
                store.items = products
                stores.append(store)
                logger.info(
                    f"Branka {store.store_type}: {len(products)} products found"
                )
            except Exception as e:
                logger.error(f"Error processing {url}: {e}", exc_info=True)
                continue

        return stores


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = BrankaCrawler()
    stores = crawler.crawl(datetime.date.today())
    for store in stores:
        print(store)
        if store.items:
            print(store.items[0])
