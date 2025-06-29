import datetime
import logging
import urllib.parse
import re
from typing import List

from bs4 import BeautifulSoup
from crawler.store.models import Product, Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)


class KonzumCrawler(BaseCrawler):
    """Crawler for Konzum store prices."""

    CHAIN = "konzum"
    BASE_URL = "https://www.konzum.hr"
    INDEX_URL = f"{BASE_URL}/cjenici"

    # Mapping for price fields
    PRICE_MAP = {
        # field: (column, is_required)
        "price": ("MALOPRODAJNA CIJENA", False),
        "unit_price": ("CIJENA ZA JEDINICU MJERE", True),
        "special_price": ("MPC ZA VRIJEME POSEBNOG OBLIKA PRODAJE", False),
        "best_price_30": ("NAJNIŽA CIJENA U POSLJEDNIH 30 DANA", False),
        "anchor_price": ("SIDRENA CIJENA NA 2.5.2025", False),
    }

    # Mapping for other fields
    FIELD_MAP = {
        "product": ("NAZIV PROIZVODA", True),
        "product_id": ("ŠIFRA PROIZVODA", True),
        "brand": ("MARKA PROIZVODA", False),
        "quantity": ("NETO KOLIČINA", False),
        "unit": ("JEDINICA MJERE", False),
        "barcode": ("BARKOD", False),
        "category": ("KATEGORIJA PROIZVODA", False),
    }

    ADDRESS_PATTERN = re.compile(r"(.*) (\d{5}) (.*)")

    def parse_index(self, content: str) -> list[str]:
        """
        Parse the Konzum index page to extract the price date and CSV links.

        Args:
            content: HTML content of the index page

        Returns:
            List of CSV urls on the page
        """

        soup = BeautifulSoup(content, "html.parser")

        urls = []
        csv_links = soup.select("a[format='csv']")

        for link in csv_links:
            href = link.get("href")
            if href:
                urls.append(f"{self.BASE_URL}{href}")

        return list(set(urls))

    def parse_store_info(self, url: str) -> Store:
        """
        Extracts store information from a CSV download URL.

        Args:
            url: CSV download URL with store information in the query parameters

        Returns:
            Store object with parsed store information, or None if parsing fails
        """

        logger.debug(f"Parsing store information from URL: {url}")

        parsed_url = urllib.parse.urlparse(url)
        query_params = urllib.parse.parse_qs(parsed_url.query)
        title = urllib.parse.unquote(query_params.get("title", [""])[0])
        title = title.replace("_", " ")

        if not title:
            raise ValueError(f"No title parameter found in URL: {url}")

        logger.debug(f"Decoded title: {title}")

        parts = [part.strip() for part in title.split(",")]
        if len(parts) < 6:  # Ensure we have the expected number of parts
            raise ValueError(f"Invalid CSV title format: {title}")

        # Extract store type
        store_type = (parts[0]).lower()
        store_id = parts[2] if len(parts) == 6 else parts[3]

        # Format:
        # SUPERMARKET,REPUBLIKE 1 31300 BELI MANASTIR,0904,1629,21.05.2025, 05-22.CSV
        # SUPERMARKET,CARLOTTA GRISI 5, SVETI ANTON 52466 NOVIGRAD,3274,1332,19.05.2025, 05-52.CSV
        m = self.ADDRESS_PATTERN.match(
            parts[1] if len(parts) == 6 else f"{parts[1]} {parts[2]}"
        )
        if not m:
            raise ValueError(f"Could not parse address from: {parts[1]}")

        # Extract address components
        street_address = m.group(1).strip().title()
        zipcode = m.group(2).strip()
        city = m.group(3).strip().title()

        store = Store(
            chain=self.CHAIN,
            store_type=store_type,
            store_id=store_id,
            name=f"{self.CHAIN.capitalize()} {city}",
            street_address=street_address,
            zipcode=zipcode,
            city=city,
            items=[],
        )

        logger.info(
            f"Parsed store: {store.store_type}, {store.street_address}, {store.zipcode}, {store.city}"
        )
        return store

    def get_index(self, date: datetime.date) -> list[str]:
        url = f"{self.INDEX_URL}?date={date:%Y-%m-%d}"

        csv_urls = []
        for page in range(1, 10):
            page_url = f"{url}&page={page}"
            content = self.fetch_text(page_url)
            if not content:
                break

            csv_urls_on_page = self.parse_index(content)
            if not csv_urls_on_page:
                break

            csv_urls.extend(csv_urls_on_page)

        return csv_urls

    def get_store_prices(self, csv_url: str) -> List[Product]:
        try:
            content = self.fetch_text(csv_url)
            return self.parse_csv(content)
        except Exception as e:
            logger.error(
                f"Failed to get store prices from {csv_url}: {e}",
                exc_info=True,
            )
            return []

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Main method to fetch and parse all store, product and price info.

        Args:
            date: The date to search for in the price list.

        Returns:
            List of Store objects with their products.

        Raises:
            ValueError: If no price list is found for the given date.
        """

        csv_links = self.get_index(date)
        stores = []

        for url in csv_links:
            try:
                store = self.parse_store_info(url)
                products = self.get_store_prices(url)
            except Exception as e:
                logger.error(f"Error processing store from {url}: {e}", exc_info=True)
                continue

            if not products:
                logger.warning(f"Error getting prices from {url}, skipping")
                continue
            store.items = products
            stores.append(store)

        return stores


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = KonzumCrawler()
    stores = crawler.crawl(datetime.date.today())
    print(stores[0])
    print(stores[0].items[0])
