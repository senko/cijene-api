import datetime
import logging
import re

from bs4 import BeautifulSoup

from crawler.store.models import Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)

CSV_PATTERN = re.compile(r"^(Supermarket_.+)_(\d+)_\d+_(\d{8})_\d{2}_\d{2}_\d{2}\.csv$")


class GavranovicCrawler(BaseCrawler):
    """
    Crawler for Gavranović store prices.

    Gavranović publishes daily CSV pricelists on an nginx directory listing at
    gavranoviccjenik.com.hr. Each CSV file contains products for a single store
    on a single date. The filename encodes the store name, ID, and date.
    """

    CHAIN = "gavranovic"
    BASE_URL = "https://gavranovic.hr"
    INDEX_URL = "https://gavranoviccjenik.com.hr/"

    PRICE_MAP = {
        "price": ("Maloprodajna cijena", True),
        "unit_price": ("Cijena za jedinicu mjere", True),
        "special_price": ("MPC za vrijeme posebnog oblika prodaje", False),
        "best_price_30": ("Najniža cijena u poslj.30 dana", False),
        "anchor_price": ("Sidrena cijena na 2.5.2025", False),
    }

    FIELD_MAP = {
        "product": ("Naziv proizvoda", True),
        "product_id": ("Šifra proizvoda", True),
        "brand": ("Marka proizvoda", False),
        "quantity": ("Neto količina", False),
        "unit": ("Jedinica mjere", False),
        "barcode": ("Barkod", True),
        "category": ("Kategorija proizvoda", False),
    }

    REQUIRED_COLUMNS = [
        "Maloprodajna cijena",
        "Cijena za jedinicu mjere",
        "MPC za vrijeme posebnog oblika prodaje",
        "Najniža cijena u poslj.30 dana",
        "Sidrena cijena na 2.5.2025",
        "Naziv proizvoda",
        "Šifra proizvoda",
        "Marka proizvoda",
        "Neto količina",
        "Jedinica mjere",
        "Barkod",
        "Kategorija proizvoda",
    ]

    def get_csv_urls(
        self, html: str, date: datetime.date
    ) -> list[tuple[str, str, str]]:
        """
        Parse the index page HTML and find CSV URLs for the given date.

        Args:
            html: HTML content of the index page.
            date: The target date to filter files by.

        Returns:
            List of (url, store_name_part, store_id) tuples for matching files.
        """
        date_str = date.strftime("%d%m%Y")
        soup = BeautifulSoup(html, "html.parser")
        results = []

        for anchor in soup.select("a[href$='.csv']"):
            href = anchor.get("href")
            if not isinstance(href, str):
                continue

            m = CSV_PATTERN.match(href)
            if not m:
                continue

            name_part, store_id, file_date = m.groups()
            if file_date == date_str:
                url = f"{self.INDEX_URL}{href}"
                results.append((url, name_part, store_id))

        return results

    def parse_store_info(self, name_part: str, store_id: str) -> Store:
        """
        Extract store information from the filename name part.

        The name part has the format: Supermarket_{Address}_{CITY}
        where underscores replace spaces in the address.

        Args:
            name_part: The store name portion of the filename
                (e.g. "Supermarket_Brace_Gojaka_4_KARLOVAC").
            store_id: The numeric store identifier.

        Returns:
            A Store object with parsed location info and no items.
        """
        parts = name_part.split("_")
        # Store type is the first token (e.g. "Supermarket")
        store_type = parts[0]

        # City tokens are the trailing all-uppercase tokens (e.g. KARLOVAC,
        # or SV_KRIZ_ZACRETJE for multi-word cities). Address is in between.
        city_start = len(parts)
        for i in range(len(parts) - 1, 0, -1):
            if parts[i].isupper():
                city_start = i
            else:
                break

        city = " ".join(parts[city_start:]).title()
        address = " ".join(parts[1:city_start])

        return Store(
            chain=self.CHAIN,
            store_type=store_type.lower(),
            store_id=store_id,
            name=f"Gavranović {address}, {city}",
            street_address=address,
            city=city,
            zipcode="",
            items=[],
        )

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Fetch and parse all product and price data for the given date.

        Downloads the index page, finds CSV files matching the date,
        then downloads and parses each one into Store objects with products.

        Args:
            date: The date to fetch pricelists for.

        Returns:
            List of Store objects, one per store location, each containing
            the parsed product list.
        """
        html = self.fetch_text(self.INDEX_URL)
        csv_urls = self.get_csv_urls(html, date)

        if not csv_urls:
            logger.info(f"No price lists found for {date}")
            return []

        stores = []
        for url, name_part, store_id in csv_urls:
            try:
                logger.info(f"Fetching CSV from: {url}")
                csv_content = self.fetch_text(url, encodings=["windows-1250"])

                if not csv_content:
                    logger.warning(f"No content found at {url}")
                    continue

                products = self.parse_csv(csv_content, delimiter=";")

                if not products:
                    logger.warning(f"No products parsed from {url}")
                    continue

                store = self.parse_store_info(name_part, store_id)
                store.items = products
                stores.append(store)

            except Exception as e:
                logger.error(f"Error processing {url}: {e}", exc_info=True)
                continue

        return stores


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = GavranovicCrawler()
    stores = crawler.crawl(datetime.date.today())
    print(f"Found {len(stores)} stores")
    for store in stores:
        print(f"  {store.name} ({store.store_id}): {len(store.items)} products")
    if stores and stores[0].items:
        print(f"  Sample product: {stores[0].items[0]}")
