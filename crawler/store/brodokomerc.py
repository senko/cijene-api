import datetime
import logging
import re
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from crawler.store.models import Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)


class BrodokomercCrawler(BaseCrawler):
    """
    Crawler for Brodokomerc retail store prices.

    This class handles downloading and parsing price data from Brodokomerc's website.
    It fetches the main index page, extracts CSV file links for each store location,
    and processes the CSV files to extract product information.

    The crawler handles:
    - Windows-1250 encoded CSV files
    - Croatian column headers
    - Store location mapping from filename codes
    - Date pattern matching in filenames
    """

    CHAIN = "brodokomerc"
    BASE_URL = "http://www.brodokomerc.hr"
    INDEX_URL = "http://www.brodokomerc.hr/cijene"

    # Mapping from filename address codes to full street addresses
    STORE_ADDRESS_MAPPING = {
        "ZRINSKI+TRG+BB": "Zrinski trg bb",
        "CANDEKOVA+32": "Candekova 32",
        "DRAZICKIH+BORACA+BB": "Dražičkih boraca bb",
        "KVATERNIKOVA+65": "Kvaternikova 65",
        "F.+BELULOVICA+5.": "Ulica Franje Belulovića 5",
    }

    # Mapping for price fields
    PRICE_MAP = {
        "price": ("Maloprodajna cijena", True),
        "unit_price": ("Cijena za jedinicu mjere", False),
        "special_price": ("MPC za vrijeme posebnog oblika prodaje", False),
        "best_price_30": ("Najniža cijena u poslj.30 dana", False),
        "anchor_price": ("Sidrena cijena na 2.5.2025", False),
    }

    # Mapping for other fields
    FIELD_MAP = {
        "product": ("Naziv proizvoda", True),
        "product_id": ("Šifra proizvoda", True),
        "brand": ("Marka proizvoda", False),
        "quantity": ("Neto količina", False),
        "unit": ("Jedinica mjere", False),
        "barcode": ("Barkod", False),
        "category": ("Kategorija proizvoda", False),
    }

    def get_all_products(self, date: datetime.date) -> List[Store]:
        """
        Main method to fetch and parse all products from Brodokomerc's price lists.

        Args:
            date: The date for which to fetch the price list

        Returns:
            List of Store objects, each containing its products.

        Raises:
            ValueError: If the price list cannot be fetched or processed
        """
        try:
            # Get the index page
            content = self.fetch_text(self.INDEX_URL)
            soup = BeautifulSoup(content, "html.parser")

            # Find CSV links for the given date
            csv_links = self._parse_csv_links(soup, date)

            if not csv_links:
                logger.warning(f"No CSV files found for date {date}")
                return []

            stores = []

            for csv_url, store_info in csv_links:
                logger.info(f"Processing store: {store_info['name']}")

                # Create store object
                store = Store(
                    chain=self.CHAIN,
                    store_id=store_info["store_id"],
                    name=store_info["name"],
                    store_type="supermarket",
                    city=store_info["city"],
                    street_address=store_info["address"],
                    zipcode="",  # Not available in the data
                    items=[],
                )

                # Download and process CSV
                products = self._process_csv_file(csv_url)
                store.items = products
                stores.append(store)

                logger.info(
                    f"Retrieved {len(products)} products from {store_info['name']}"
                )

            return stores

        except Exception as e:
            logger.error(f"Error getting products: {str(e)}")
            raise

    def _parse_csv_links(self, soup: BeautifulSoup, date: datetime.date) -> List[tuple]:
        """
        Parse the index page to extract CSV links for the given date.

        Args:
            soup: BeautifulSoup object of the index page
            date: The date to look for in CSV filenames

        Returns:
            List of tuples containing (csv_url, store_info)
        """
        csv_links = []

        # Convert date to filename format (DDMMYYYY)
        date_pattern = self._format_date_for_filename(date)

        # Find all CSV links containing the date pattern
        for link in soup.find_all("a", href=True):
            full_path = link["href"]

            # The actual filename is the part of the path before the UUID
            # e.g., /documents/.../filename.csv/uuid -> filename.csv
            path_parts = full_path.split("/")
            filename_with_uuid = path_parts[-1]
            # If the last part is a UUID, the actual filename is the second to last part
            if len(path_parts) >= 2 and re.match(
                r"^[0-9a-fA-F-]{36}$", filename_with_uuid
            ):
                filename = path_parts[-2]
                full_csv_url = urljoin(self.BASE_URL, "/".join(path_parts[:-1]))
            else:
                filename = filename_with_uuid
                full_csv_url = urljoin(self.BASE_URL, full_path)

            if ".csv" in filename and date_pattern in filename:
                # Extract store information from the filename
                store_info = self._extract_store_info(filename)
                if store_info:
                    csv_links.append((full_csv_url, store_info))

        logger.info(f"Found {len(csv_links)} CSV files for date {date}")
        return csv_links

    def _format_date_for_filename(self, date: datetime.date) -> str:
        """
        Convert date object to filename format (DDMMYYYY).

        Args:
            date: datetime.date object

        Returns:
            Date string in DDMMYYYY format
        """
        return f"{date.day:02d}{date.month:02d}{date.year}"

    def _extract_store_info(self, filename: str) -> Optional[Dict[str, Any]]:
        """
        Extract store information from CSV filename.

        The filename format is:
            Supermarket_ADDRESS_CITY_CODE_ID_DATETIME.csv

        Example:
            Supermarket_DRAZICKIH BORACA BB_DRAZICE_22041_243_27052025_07_22_02.csv

        Args:
            filename: The CSV filename or path

        Returns:
            Dictionary with store information, or None if parsing fails
        """
        try:
            # Extract filename from path
            basename = filename.split("/")[-1]

            # Split by underscores
            parts = basename.split("_")
            if len(parts) < 5:
                logger.warning(f"Filename doesn't have enough parts: {basename}")
                return None

            # Extract components
            # Format: Supermarket_ADDRESS_CITY_CODE_ID_DATETIME.csv
            address_code = parts[1]  # e.g., 'CANDEKOVA+32'
            city = parts[2]  # e.g., 'RIJEKA'
            store_id = parts[3]  # e.g., '22041'

            # Map address code to full address
            full_address = self.STORE_ADDRESS_MAPPING.get(
                address_code, address_code.replace("+", " ")
            )

            # Clean up city name
            clean_city = city.replace("+", " ").title()

            return {
                "store_id": store_id,
                "address": full_address,
                "city": clean_city,
                "name": f"Brodokomerc {clean_city}",
            }

        except Exception as e:
            logger.error(f"Error extracting store info from {filename}: {e}")
            return None

    def _process_csv_file(self, csv_url: str) -> List:
        """
        Download and process a single CSV file.

        Args:
            csv_url: URL of the CSV file to download

        Returns:
            List of Product objects
        """
        try:
            # Download CSV file with Windows-1250 encoding
            content = self.fetch_text(csv_url, encodings=["windows-1250"])

            # Parse CSV
            products = self.parse_csv(content, delimiter=";")

            return products

        except Exception as e:
            logger.error(f"Error processing CSV file {csv_url}: {e}", exc_info=True)
            return []

    def fix_product_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Clean up product data specific to Brodokomerc.

        Args:
            data: Dictionary containing the product data

        Returns:
            The cleaned product data
        """
        # Call parent method first
        data = super().fix_product_data(data)

        # Clean up product name - remove quotes and collapse multiple spaces
        if data.get("product"):
            data["product"] = re.sub(r"\s+", " ", data["product"].strip().strip('"'))

        # Clean up brand name
        if data.get("brand"):
            data["brand"] = data["brand"].strip().strip('"')

        # Clean up category
        if data.get("category"):
            data["category"] = data["category"].strip().strip('"')

        # Clean up quantity and unit
        if data.get("quantity"):
            data["quantity"] = data["quantity"].strip().strip('"').replace(",", ".")

        if data.get("unit"):
            data["unit"] = data["unit"].strip().strip('"')

        return data


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = BrodokomercCrawler()
    stores = crawler.crawl(datetime.date.today())
    print(stores[0])
    print(stores[0].items[0])
