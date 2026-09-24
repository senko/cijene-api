import datetime
import logging
import os
import re
from typing import List
from urllib.parse import unquote

from bs4 import BeautifulSoup
from crawler.store.models import Product, Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)


class MetroCrawler(BaseCrawler):
    """Crawler for Metro store prices."""

    CHAIN = "metro"
    BASE_URL = "https://metrocjenik.com.hr"

    # Regex to parse store information from the filename
    # Format: <store_type>_METRO_YYYYMMDDTHHMM_<store_id>_<address>,<city>.csv
    # Example: skladiste_za_trgovanje_robom_na_veliko_i_malo_METRO_20250521T1149_S20_CESTA_PAPE_IVANA_PAVLA_II_3,_KASTEL_SUCURAC.csv
    STORE_FILENAME_PATTERN = re.compile(
        r"^(?P<store_type>.+?)_METRO_\d{8}T\d{4}_"
        r"(?P<store_id>[^_]+)_"
        r"(?P<address>[^,]+),"
        r"(?P<city>[^.]+)\.csv$"
    )

    # Mapping for price fields from CSV columns
    PRICE_MAP = {
        # field: (column_name, is_required)
        "price": ("MPC", True),
        "unit_price": ("CIJENA_PO_MJERI", True),
        "special_price": ("POSEBNA_PRODAJA", False),
        "best_price_30": ("NAJNIZA_30_DANA", False),
        "anchor_price": ("SIDRENA_02_05", False),
    }

    # Mapping for other product fields from CSV columns
    FIELD_MAP = {
        "product": ("NAZIV", True),
        "product_id": ("SIFRA", True),
        "brand": ("MARKA", False),
        "quantity": ("NETO_KOLICINA", False),
        "unit": ("JED_MJERE", False),
        "barcode": ("BARKOD", False),
        "category": ("KATEGORIJA", False),
    }

    REQUIRED_COLUMNS = [
        "MPC",
        "CIJENA_PO_MJERI",
        "POSEBNA_PRODAJA",
        "NAJNIZA_30_DANA",
        "SIDRENA_02_05",
        "NAZIV",
        "SIFRA",
        "MARKA",
        "NETO_KOLICINA",
        "JED_MJERE",
        "BARKOD",
        "KATEGORIJA",
    ]

    def parse_index(self, content: str) -> list[str]:
        """
        Parse the Metro index page to extract CSV links.

        Args:
            content: HTML content of the index page

        Returns:
            List of absolute CSV URLs on the page
        """
        soup = BeautifulSoup(content, "html.parser")
        urls = []

        for link_tag in soup.select('a[href$=".csv"]'):
            href = str(link_tag.get("href"))
            if href:
                full_url = f"{self.BASE_URL}/{href.lstrip('/')}"
                urls.append(full_url)

        return list(set(urls))  # Return unique URLs

    def parse_store_info(self, url: str) -> Store:
        """
        Extracts store information from a CSV download URL.

        Example URL path part:
        skladiste_za_trgovanje_robom_na_veliko_i_malo_METRO_20250521T1149_S20_CESTA_PAPE_IVANA_PAVLA_II_3%2C_KASTEL_SUCURAC.csv

        Args:
            url: CSV download URL with store information in the filename

        Returns:
            Store object with parsed store information
        """
        logger.debug(f"Parsing store information from Metro URL: {url}")

        filename = unquote(os.path.basename(url))

        match = self.STORE_FILENAME_PATTERN.match(filename)
        if not match:
            raise ValueError(f"Invalid CSV filename format for Metro: {filename}")

        data = match.groupdict()

        store_type = data["store_type"].replace("_", " ").lower()
        store_id = data["store_id"]
        # Address: "CESTA_PAPE_IVANA_PAVLA_II_3" -> "Cesta Pape Ivana Pavla Ii 3"
        address_raw = data["address"]
        street_address = address_raw.replace("_", " ").title()
        # City: "_KASTEL_SUCURAC" -> "Kastel Sucurac" (strip potential leading/trailing _ from regex capture)
        city_raw = data["city"]
        city = city_raw.strip("_").replace("_", " ").title()

        store = Store(
            chain=self.CHAIN,
            store_type=store_type,
            store_id=store_id,
            name=f"{self.CHAIN.capitalize()} {city} {store_id}",  # e.g. "Metro Kastel Sucurac S20"
            street_address=street_address,
            zipcode="",  # Zipcode is not available in the filename
            city=city,
            items=[],
        )

        logger.info(
            f"Parsed Metro store: {store.name}, Type: {store.store_type}, Address: {store.street_address}, City: {store.city}"
        )
        return store

    def get_store_prices(self, csv_url: str) -> List[Product]:
        """
        Fetch and parse store prices from a Metro CSV URL.
        The CSV is comma-separated and UTF-8 encoded.

        Args:
            csv_url: URL to the CSV file containing prices

        Returns:
            List of Product objects
        """
        try:
            # fetch_text handles potential HTTP errors. CSV is UTF-8 by default from response.text.
            content = self.fetch_text(csv_url)
            # Metro CSVs are comma-delimited
            return self.parse_csv(content, delimiter=",")
        except Exception as e:
            logger.error(
                f"Failed to get Metro store prices from {csv_url}: {e}",
                exc_info=True,
            )
            return []

    def get_index(self, date: datetime.date) -> list[str]:
        """
        Fetch and parse the Metro index page to get CSV URLs for the specified date.

        Args:
            date: The date to search for in the price list (YYYYMMDD format).

        Returns:
            List of CSV URLs containing prices for the specified date.
        """
        content = self.fetch_text(self.BASE_URL)

        if not content:
            logger.warning(f"No content found at Metro index URL: {self.BASE_URL}")
            return []

        all_urls = self.parse_index(content)
        # Date format in Metro filenames is YYYYMMDD, e.g., _METRO_20250521T...
        date_str = date.strftime("%Y%m%d")

        matching_urls = []
        for url in all_urls:
            filename = os.path.basename(url)
            # Check if the YYYYMMDD date string (followed by 'T' for time) is in the filename
            if f"_{date_str}T" in filename:
                matching_urls.append(url)

        if not matching_urls:
            logger.warning(f"No Metro URLs found matching date {date:%Y-%m-%d}")

        return matching_urls

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Main method to fetch and parse all Metro store, product, and price info for a given date.

        Args:
            date: The date to search for in the price list.

        Returns:
            List of Store objects with their products.
        """
        csv_links = self.get_index(date)

        if not csv_links:
            logger.warning(f"No Metro CSV links found for date {date.isoformat()}")
            return []

        stores = []
        for url in csv_links:
            try:
                store = self.parse_store_info(url)
                products = self.get_store_prices(url)
            except ValueError as ve:  # Catch specific error from parse_store_info
                logger.error(
                    f"Skipping store due to parsing error from URL {url}: {ve}",
                    exc_info=False,
                )  # exc_info=False to reduce noise for expected parsing errors
                continue
            except Exception as e:
                logger.error(
                    f"Error processing Metro store from {url}: {e}", exc_info=True
                )
                continue  # Skip to the next URL on error

            if not products:
                logger.warning(f"No products found for Metro store at {url}, skipping.")
                continue

            store.items = products
            stores.append(store)

        return stores


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = MetroCrawler()
    stores = crawler.crawl(datetime.date.today())
    print(stores[0])
    print(stores[0].items[0])
