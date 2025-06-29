import datetime
import logging
import re
from typing import List
from urllib.parse import urlparse, unquote

from bs4 import BeautifulSoup
from crawler.store.models import Product, Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)


class KtcCrawler(BaseCrawler):
    """Crawler for KTC store prices."""

    CHAIN = "ktc"
    BASE_URL = "https://www.ktc.hr"
    INDEX_URL = f"{BASE_URL}/cjenici"

    # CSV fields mapping
    PRICE_MAP = {
        # field: (column, is_required)
        "price": ("Maloprodajna cijena", True),
        "unit_price": ("Cijena za jedinicu mjere", True),
        "special_price": ("MPC za vrijeme posebnog oblika prodaje", False),
        "best_price_30": ("Najniža cijena u posljednjih 30 dana", False),
    }

    # Mapping for other fields
    FIELD_MAP = {
        "product": ("Naziv proizvoda", True),
        "product_id": ("Šifra proizvoda", True),
        "brand": ("Marka proizvoda", False),
        "quantity": ("Neto količina", False),
        "unit": ("Jedinica mjere", False),
        "barcode": ("Barkod", False),
        "category": ("Kategorija", False),
    }

    CITIES = [
        "KRIZEVCI",
        "VARAZDIN",
        "BJELOVAR",
        "CAKOVEC",
        "DARUVAR",
        "DUGO SELO",
        "DURDEVAC",
        "GRUBISNO POLJE",
        "IVANEC",
        "JALZABET",
        "KARLOVAC",
        "KOPRIVNICA",
        "KRAPINA",
        "KUTINA",
        "MURSKO SREDISCE",
        "PAKRAC",
        "PETRINJA",
        "PITOMACA",
        "POZEGA",
        "PRELOG",
        "SISAK II",
        "SISAK",
        "SLATINA",
        "VELIKA GORICA",
        "VIROVITICA",
        "VRBOVEC",
        "ZABOK",
        "CAZMA",
    ]

    def parse_index(self) -> list[str]:
        """
        Parse the KTC index page to extract store pages.

        Returns:
            List of store page URLs
        """
        content = self.fetch_text(self.INDEX_URL)
        soup = BeautifulSoup(content, "html.parser")

        store_urls = []
        store_links = soup.select('a[href^="cjenici?poslovnica="]')

        for link in store_links:
            href = link.get("href")
            if href:
                store_urls.append(f"{self.BASE_URL}/{href}")

        return list(set(store_urls))

    def get_store_csv_url(self, store_url: str, date: datetime.date) -> str:
        """
        Fetch the store page and extract the CSV URL for the specified date.

        Args:
            store_url: URL to the store's price list page
            date: The date to search for in the CSV filename

        Returns:
            CSV URL for the specified store and date, or None if not found
        """
        content = self.fetch_text(store_url)
        soup = BeautifulSoup(content, "html.parser")

        date_str = date.strftime("%Y%m%d")
        csv_links = soup.select('a[href$=".csv"]')

        for link in csv_links:
            href = str(link.get("href"))
            if date_str in href:
                if href.startswith("/"):
                    return f"{self.BASE_URL}{href}"
                else:
                    return f"{self.BASE_URL}/{href}"

        raise ValueError(f"No CSV found for date {date} at {store_url}")

    def parse_store_info(self, csv_url: str) -> Store:
        """
        Extracts store information from a CSV download URL.


        Format example (URL path basename):
            `TRGOVINA-SENJSKA ULICA 118 KARLOVAC-PJ8A-1-20250515-071626.csv`

        Args:
            csv_url: CSV download URL with store information

        Returns:
            Store object with parsed store information
        """
        logger.debug(f"Parsing store information from URL: {csv_url}")

        parsed_url = urlparse(csv_url)
        path_parts = parsed_url.path.split("/")

        # Get the last two parts of the path
        csv_filename = unquote(path_parts[-1])

        # Try to guess the city if possible
        for city in self.CITIES:
            if city in csv_filename:
                break
        else:
            city = ""

        # Parse csv_filename to get store type and address
        parts = csv_filename.split("-")
        if len(parts) < 3:
            raise ValueError(f"Invalid CSV filename format: {csv_filename}")

        store_type = parts[0].lower()
        store_id = parts[2]

        # Address is the second part, but might contain the city name too
        street_address = parts[1].strip()
        if city:
            # Remove the city name to get just the street address
            street_address = street_address.replace(city, "").strip()
            street_address = re.sub(r"\s+", " ", street_address)

        # Create the store object
        store = Store(
            chain=self.CHAIN,
            store_type=store_type,
            store_id=f"PJ{store_id}",
            name=f"{self.CHAIN.upper()} {city}",
            street_address=street_address.title(),
            zipcode="",  # No ZIP code in the URL
            city=city.title(),
            items=[],
        )

        logger.info(
            f"Parsed store: {store.store_type}, {store.street_address}, {store.city}"
        )
        return store

    def get_store_prices(self, csv_url: str) -> List[Product]:
        """
        Fetch and parse CSV content to extract product prices.

        Args:
            csv_url: URL of the CSV file to download

        Returns:
            List of Product objects
        """
        try:
            # KTC CSVs are encoded in Windows-1250
            content = self.fetch_text(csv_url, encodings=["windows-1250"])
            return self.parse_csv(content, delimiter=";")
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
        store_urls = self.parse_index()
        stores = []

        for store_url in store_urls:
            try:
                csv_url = self.get_store_csv_url(store_url, date)
                if not csv_url:
                    logger.warning(f"No CSV found for date {date} at {store_url}")
                    continue

                store = self.parse_store_info(csv_url)
                products = self.get_store_prices(csv_url)

                if not products:
                    logger.warning(f"No products found in {csv_url}, skipping")
                    continue

                store.items = products
                stores.append(store)

            except Exception as e:
                logger.error(
                    f"Error processing store from {store_url}: {e}", exc_info=True
                )
                continue

        return stores


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = KtcCrawler()
    stores = crawler.crawl(datetime.date.today())
    print(stores[0])
    print(stores[0].items[0])
