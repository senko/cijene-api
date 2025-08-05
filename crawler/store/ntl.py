import datetime
import logging
import os
import re
from urllib.parse import unquote, quote_plus

from bs4 import BeautifulSoup
from crawler.store.models import Product, Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)


class NtlCrawler(BaseCrawler):
    """Crawler for NTL store prices."""

    CHAIN = "ntl"
    BASE_URL = "https://ntl.hr/cjenik/"

    # Regex to parse store information from the filename
    # Format: Supermarket_Ljudevita Gaja 1_DUGA RESA_10103_263_25052025_07_22_36.csv
    STORE_FILENAME_PATTERN = re.compile(
        r"(?P<store_type>[^_]+)_(?P<street_address>[^_]+)_(?P<city>[^_]+)_(?P<store_id>\d+)_.*\.csv$"
    )

    # Mapping for price fields from CSV columns
    PRICE_MAP = {
        # field: (column_name, is_required)
        "price": ("Maloprodajna cijena", False),
        "unit_price": ("Cijena za jedinicu mjere", False),
        "special_price": ("MPC za vrijeme posebnog oblika prodaje", False),
        "anchor_price": ("Sidrena cijena na 2.5.2025", False),
    }

    # Mapping for other product fields from CSV columns
    FIELD_MAP = {
        "product_id": ("Šifra proizvoda", True),
        "barcode": ("Barkod", False),
        "product": ("Naziv proizvoda", True),
        "brand": ("Marka proizvoda", False),
        "quantity": ("Neto količina", False),
        "unit": ("Jedinica mjere", False),
        "category": ("Kategorija proizvoda", False),
    }

    def parse_index(self, content: str) -> list[str]:
        """
        Parse the NTL index page to extract CSV links.

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

    def get_store_list(self) -> list[str]:
        """
        Get list of all available stores from the main page dropdown.

        Returns:
            List of store names
        """
        content = self.fetch_text(self.BASE_URL)
        if not content:
            logger.warning(f"No content found at NTL index URL: {self.BASE_URL}")
            return []

        soup = BeautifulSoup(content, "html.parser")
        stores = []

        select_element = soup.find("select")
        if not select_element:
            logger.warning("No store dropdown found on the NTL index page")
            return []

        options = select_element.select("option[value]")
        for option in options:
            store_value = option.get("value", "").strip()
            if store_value and not store_value.startswith("Odaberi"):
                stores.append(store_value)

        logger.info(f"Found {len(stores)} stores: {'; '.join(stores)}")
        return stores

    def get_historical_csv_for_date(
        self,
        store_name: str,
        target_date: datetime.date,
    ) -> str | None:
        """
        Get historical CSV URL for a specific store and date.

        Args:
            store_name: Store name from dropdown
            target_date: Date to find CSV for

        Returns:
            CSV URL if found, None if not available
        """
        archive_url = f"{self.BASE_URL}?pageName=archeive&archive_file_name={quote_plus(store_name)}"
        logger.debug(f"Fetching archive page for {store_name}: {archive_url}")

        try:
            content = self.fetch_text(archive_url)
            if not content:
                logger.warning(f"No content found at archive URL: {archive_url}")
                return None

            soup = BeautifulSoup(content, "html.parser")

            target_date_str = target_date.strftime("%d-%m-%Y")

            for row in soup.select("table tr"):
                cells = row.find_all("td")
                if len(cells) >= 4:  # Expect at least 4 cells: #, store, date, download
                    date_cell = cells[2].get_text().strip()
                    if date_cell == target_date_str:
                        # Find the download link in the last cell
                        download_link = cells[-1].select_one("a[href$='.csv']")
                        if download_link:
                            csv_url = download_link.get("href")
                            logger.info(
                                f"Found historical CSV for {store_name} on {target_date_str}: {csv_url}"
                            )
                            return csv_url

            logger.debug(
                f"No historical data found for {store_name} on {target_date_str}"
            )
            return None

        except Exception as e:
            logger.error(
                f"Error fetching historical data for {store_name}: {e}", exc_info=True
            )
            return None

    def parse_store_info(self, url: str) -> Store:
        """
        Extracts store information from a CSV download URL.

        Example URL:
        https://www.ntl.hr/csv_files/Supermarket_Ljudevita Gaja 1_DUGA RESA_10103_263_25052025_07_22_36.csv

        Args:
            url: CSV download URL with store information in the filename

        Returns:
            Store object with parsed store information
        """
        logger.debug(f"Parsing store information from NTL URL: {url}")

        filename = unquote(os.path.basename(url))

        match = self.STORE_FILENAME_PATTERN.match(filename)
        if not match:
            raise ValueError(f"Invalid CSV filename format for NTL: {filename}")

        data = match.groupdict()

        store_type = data["store_type"].lower()
        street_address = data["street_address"]
        city = data["city"].title()
        store_id = data["store_id"]

        store = Store(
            chain=self.CHAIN,
            store_type=store_type,
            store_id=store_id,
            name=f"NTL {city}",
            street_address=street_address,
            zipcode="",  # Zipcode is not available in the filename
            city=city,
            items=[],
        )

        logger.info(
            f"Parsed NTL store: {store.name}, Address: {store.street_address}, City: {store.city}"
        )
        return store

    def get_store_prices(self, csv_url: str) -> list[Product]:
        """
        Fetch and parse store prices from an NTL CSV URL.
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
                f"Failed to get NTL store prices from {csv_url}: {e}",
                exc_info=True,
            )
            return []

    def get_index(self, date: datetime.date) -> list[str]:
        """
        Fetch and parse the NTL index page to get CSV URLs.

        Args:
            date: The date to fetch CSV files for

        Returns:
            List of CSV URLs available for the given date.
        """
        today = datetime.date.today()

        if date == today:
            logger.info(f"Fetching current CSV files for today ({date:%Y-%m-%d})")

            content = self.fetch_text(self.BASE_URL)
            if not content:
                logger.warning(f"No content found at NTL index URL: {self.BASE_URL}")
                return []

            all_urls = self.parse_index(content)
            if not all_urls:
                logger.warning("No NTL CSV URLs found on index page")

            return all_urls
        else:
            logger.info(f"Fetching historical CSV files for date ({date:%Y-%m-%d})")

            stores = self.get_store_list()
            if not stores:
                logger.warning("No stores found in dropdown")
                return []

            historical_urls = []
            for store_name in stores:
                csv_url = self.get_historical_csv_for_date(store_name, date)
                if csv_url:
                    historical_urls.append(csv_url)

            if not historical_urls:
                raise ValueError(f"No stores found for date {date:%Y-%m-%d}")

            logger.info(
                f"Found {len(historical_urls)} historical CSV files for {date:%Y-%m-%d}"
            )
            return historical_urls

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Main method to fetch and parse all NTL store, product, and price info.

        Args:
            date: The date to fetch data for

        Returns:
            List of Store objects with their products.
        """
        csv_links = self.get_index(date)

        if not csv_links:
            logger.warning(f"No NTL CSV links found for date {date:%Y-%m-%d}")
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
                    f"Error processing NTL store from {url}: {e}", exc_info=True
                )
                continue

            if not products:
                logger.warning(f"No products found for NTL store at {url}, skipping.")
                continue

            store.items = products
            stores.append(store)

        return stores

    def fix_product_data(self, data: dict) -> dict:
        """
        Clean and fix NTL-specific product data.

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
    crawler = NtlCrawler()
    stores = crawler.crawl(datetime.date.today())
    print(stores[0])
    print(stores[0].items[0])
