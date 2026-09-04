import datetime
import logging
import re

from bs4 import BeautifulSoup

from crawler.store.models import Product, Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)

# Regex to extract store type and date from CSV filename
# e.g., Hipermarket070426.csv -> ("Hipermarket", "07", "04", "26")
CSV_FILENAME_PATTERN = re.compile(
    r"(Hipermarket|Supermarket)(\d{2})(\d{2})(\d{2})\.csv"
)

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

    PRICE_MAP = {
        "price": ("MPC", True),
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

    def get_index(self, date: datetime.date) -> list[str]:
        """
        Fetch the index page and return CSV URLs matching the given date.

        Args:
            date: The date for which to find price list CSVs

        Returns:
            List of CSV URLs (up to 2 — one per store) for the given date
        """
        content = self.fetch_text(self.INDEX_URL)
        all_urls = self.parse_index(content)

        matching = []
        for url in all_urls:
            m = CSV_FILENAME_PATTERN.search(url)
            if not m:
                continue

            dd, mm, yy = int(m.group(2)), int(m.group(3)), int(m.group(4))
            url_date = datetime.date(2000 + yy, mm, dd)

            if url_date == date:
                matching.append(url)

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
        m = CSV_FILENAME_PATTERN.search(url)
        if not m:
            raise ValueError(f"Cannot parse store type from URL: {url}")

        store_type = m.group(1)
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
