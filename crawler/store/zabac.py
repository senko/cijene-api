import datetime
import logging
import os
import re
from urllib.parse import unquote

from bs4 import BeautifulSoup
from crawler.store.models import Product, Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)


class ZabacCrawler(BaseCrawler):
    """Crawler for Žabac store prices."""

    CHAIN = "zabac"
    BASE_URL = "https://zabacfoodoutlet.hr/cjenik/"

    # Regex to parse store information from the filename
    # Format: Cjenik-Zabac-Food-Outlet-PJ-<store_id>-<address>.csv
    # Example: Cjenik-Zabac-Food-Outlet-PJ-11-Savska-Cesta-206.csv
    STORE_FILENAME_PATTERN = re.compile(r".*PJ-(?P<store_id>\d+)-(?P<address>.+)\.csv$")

    # Mapping for price fields from CSV columns
    PRICE_MAP = {
        # field: (column_name, is_required)
        "price": ("MPC", False),
        "unit_price": ("MPC", False),  # Use same as price
        "special_price": ("", False),  # Not available in Zabac CSV
        "anchor_price": ("", False),  # Not available in Zabac CSV
    }

    # Mapping for other product fields from CSV columns
    FIELD_MAP = {
        "product_id": ("Artikl Šifra", True),
        "barcode": ("Barcode", False),
        "product": ("Naziv artikla / usluge", True),
        "brand": ("", False),  # Not available in Zabac CSV
        "quantity": ("", False),  # Not available in Zabac CSV
        "unit": ("", False),  # Not available in Zabac CSV
        "category": ("", False),  # Not available in Zabac CSV
    }

    def parse_index(self, content: str) -> list[str]:
        """
        Parse the Žabac index page to extract CSV links.

        Args:
            content: HTML content of the index page

        Returns:
            List of absolute CSV URLs on the page
        """
        soup = BeautifulSoup(content, "html.parser")
        urls = []

        for link_tag in soup.select('a[href$=".csv"]'):
            href = str(link_tag.get("href"))
            urls.append(href)

        return list(set(urls))  # Return unique URLs

    def parse_store_info(self, url: str) -> Store:
        """
        Extracts store information from a CSV download URL.

        Example URL:
        https://zabacfoodoutlet.hr/wp-content/uploads/2025/05/Cjenik-Zabac-Food-Outlet-PJ-11-Savska-Cesta-206.csv

        Args:
            url: CSV download URL with store information in the filename

        Returns:
            Store object with parsed store information
        """
        logger.debug(f"Parsing store information from Zabac URL: {url}")

        filename = unquote(os.path.basename(url))

        match = self.STORE_FILENAME_PATTERN.match(filename)
        if not match:
            raise ValueError(f"Invalid CSV filename format for Zabac: {filename}")

        data = match.groupdict()

        store_id = data["store_id"]
        # Address: "Savska-Cesta-206" -> "Savska Cesta 206"
        address_raw = data["address"]
        street_address = address_raw.replace("-", " ")

        store = Store(
            chain=self.CHAIN,
            store_type="",  # Store type is not available in the filename
            store_id=f"PJ-{store_id}",
            name=f"Žabac PJ-{store_id}",  # e.g. "Žabac PJ-11"
            street_address=street_address,
            zipcode="",  # Zipcode is not available in the filename
            city="",  # City is not available in the filename
            items=[],
        )

        logger.info(
            f"Parsed Žabac store: {store.name}, Address: {store.street_address}"
        )
        return store

    def get_store_prices(self, csv_url: str) -> list[Product]:
        """
        Fetch and parse store prices from a Žabac CSV URL.
        The CSV is semicolon-separated and windows-1250 encoded.

        Args:
            csv_url: URL to the CSV file containing prices

        Returns:
            List of Product objects
        """
        try:
            content = self.fetch_text(csv_url, encodings=["windows-1250"])
            return self.parse_csv(content, delimiter=";")
        except Exception as e:
            logger.error(
                f"Failed to get Žabac store prices from {csv_url}: {e}",
                exc_info=True,
            )
            return []

    def get_index(self, date: datetime.date) -> list[str]:
        """
        Fetch and parse the Žabac index page to get CSV URLs for given date.

        Args:
            date: The date parameter

        Returns:
            List of CSV URLs available on the index page for the given date.
        """
        content = self.fetch_text(self.BASE_URL)

        if not content:
            logger.warning(f"No content found at Žabac index URL: {self.BASE_URL}")
            return []

        # strftime doesn't support unpadded day and month
        url_date = f"{date.day}.{date.month}.{date.year}"
        return [url for url in self.parse_index(content) if url_date in url]

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Main method to fetch and parse all Žabac store, product, and price info.

        Note: Date parameter is ignored as Žabac only provides current prices.

        Args:
            date: The date parameter (ignored for Žabac)

        Returns:
            List of Store objects with their products.
        """
        csv_links = self.get_index(date)

        if not csv_links:
            logger.warning("No Žabac CSV links found")
            return []

        stores = []
        for url in csv_links:
            try:
                store = self.parse_store_info(url)
                products = self.get_store_prices(url)
            except ValueError as ve:
                logger.error(
                    f"Skipping store due to parsing error from URL {url}: {ve}",
                    exc_info=False,
                )
                continue
            except Exception as e:
                logger.error(
                    f"Error processing Žabac store from {url}: {e}", exc_info=True
                )
                continue

            if not products:
                logger.warning(f"No products found for Žabac store at {url}, skipping.")
                continue

            store.items = products
            stores.append(store)

        return stores

    def fix_product_data(self, data: dict) -> dict:
        """
        Clean and fix Žabac-specific product data.

        Args:
            data: Dictionary containing the row data

        Returns:
            The cleaned data
        """
        if "product" in data and data["product"]:
            data["product"] = data["product"].strip()

        # Call parent method for common fixups
        return super().fix_product_data(data)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = ZabacCrawler()
    stores = crawler.crawl(datetime.date.today())
    print(stores[0])
    print(stores[0].items[0])
