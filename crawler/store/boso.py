import datetime
import json
import logging
import re
import time
from typing import Optional

from bs4 import BeautifulSoup

from crawler.store.models import Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)


class BosoCrawler(BaseCrawler):
    """
    Crawler for Boso store prices.

    This class handles downloading and parsing price data from Boso's website.
    It fetches the main page, extracts store information from the dropdown,
    makes AJAX requests to get CSV file listings, and downloads/parses the CSVs.
    """

    CHAIN = "boso"
    BASE_URL = "https://www.boso.hr"
    PRICE_LIST_URL = "https://www.boso.hr/cjenik/"

    PRICE_MAP = {
        "price": ("MPC", False),
        "unit_price": ("cijena za jedinicu mjere", False),
        "special_price": ("MPC za vrijeme posebnog oblika prodaje", False),
        "anchor_price": ("sidrena cijena na 2.5.2025", False),
    }

    FIELD_MAP = {
        "product_id": ("šifra", True),
        "product": ("naziv", True),
        "brand": ("marka", False),
        "quantity": ("neto količina", False),
        "unit": ("jedinica mjere", False),
        "barcode": ("barkod", False),
        "category": ("kategorija proizvoda", False),
    }

    # Date pattern for parsing dates from CSV filenames and HTML
    DATE_PATTERN = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")

    def __init__(self):
        super().__init__()
        self._ajax_config = None

    def get_ajax_config(self) -> dict:
        """
        Extract AJAX configuration (URL and nonce) from the main page.

        Returns:
            Dictionary containing ajax_url and nonce

        Raises:
            ValueError: If configuration cannot be extracted
        """
        if self._ajax_config is not None:
            return self._ajax_config

        logger.debug("Fetching AJAX configuration from main page")

        content = self.fetch_text(self.PRICE_LIST_URL)
        soup = BeautifulSoup(content, "html.parser")

        # Find the script tag containing the AJAX configuration
        script_tag = soup.find("script", id="marketshop-csv-js-js-extra")
        if not script_tag:
            raise ValueError("Could not find AJAX configuration script tag")

        script_content = script_tag.string
        if not script_content:
            raise ValueError("Script tag is empty")

        # Extract JSON from JavaScript variable assignment
        # Format: var marketshop_csv_ajax = {"ajax_url":"...","nonce":"...","version":"..."};
        start = script_content.find("{")
        end = script_content.rfind("}") + 1

        if start == -1 or end == 0:
            raise ValueError("Could not find JSON object in script")

        json_str = script_content[start:end]

        try:
            config = json.loads(json_str)
            if "ajax_url" not in config or "nonce" not in config:
                raise ValueError("Missing required fields in AJAX configuration")

            logger.debug(f"Extracted AJAX config: {config}")
            self._ajax_config = config
            return config
        except json.JSONDecodeError as e:
            raise ValueError(f"Failed to parse JSON configuration: {e}")

    def get_stores(self) -> dict[str, dict]:
        """
        Extract store information from the dropdown on the main page.

        Returns:
            Dictionary mapping store values to store information

        Raises:
            ValueError: If stores cannot be extracted
        """
        logger.debug("Fetching store list from main page")

        content = self.fetch_text(self.PRICE_LIST_URL)
        soup = BeautifulSoup(content, "html.parser")

        # Find the store dropdown
        select = soup.find("select", id="marketshop-filter")
        if not select:
            raise ValueError("Could not find store dropdown")

        stores = {}
        options = select.find_all("option")

        for option in options:
            value = option.get("value", "").strip()
            if not value:  # Skip empty option (placeholder)
                continue

            store_info = self.parse_store_from_option(value)
            if store_info:
                stores[value] = store_info

        logger.debug(f"Found {len(stores)} stores")
        return stores

    def parse_store_from_option(self, option_value: str) -> Optional[dict]:
        """
        Parse store information from dropdown option value.

        Args:
            option_value: Value from dropdown option (e.g., "supermarket, M.J.ZAGORKE BB, Slavonski brod, SB-ZVEČEVO")

        Returns:
            Dictionary with store information or None if parsing fails
        """
        # Split by comma and strip whitespace
        parts = [part.strip() for part in option_value.split(",")]

        if len(parts) < 4:
            logger.warning(f"Invalid store option format: {option_value}")
            return None

        store_type = parts[0]
        street_address = parts[1]
        city = parts[2]
        store_code = parts[3]

        return {
            "store_type": store_type,
            "street_address": street_address,
            "city": city,
            "store_code": store_code,
            "option_value": option_value,
        }

    def get_csv_links_for_store(
        self, store_value: str, date: datetime.date
    ) -> list[str]:
        """
        Get CSV download links for a specific store and date.

        Args:
            store_value: Store value from dropdown
            date: Target date for CSV files

        Returns:
            List of CSV download URLs for the specified date

        Raises:
            ValueError: If AJAX request fails or response is invalid
        """
        logger.debug(f"Getting CSV links for store: {store_value}, date: {date}")

        # Get AJAX configuration
        ajax_config = self.get_ajax_config()
        ajax_url = ajax_config["ajax_url"]
        nonce = ajax_config["nonce"]

        # Prepare AJAX request data
        data = {
            "action": "filter_by_marketshop",
            "marketshop": store_value,
            "nonce": nonce,
            "_": str(int(time.time() * 1000)),  # Current timestamp in milliseconds
        }

        # Make AJAX request
        headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
            "X-Requested-With": "XMLHttpRequest",
            "Referer": self.PRICE_LIST_URL,
        }

        response = self.client.post(ajax_url, data=data, headers=headers)
        response.raise_for_status()

        try:
            json_response = response.json()
            if not json_response.get("success"):
                logger.warning(f"AJAX request failed for store {store_value}")
                return []

            html_content = json_response.get("data", {}).get("html", "")
            if not html_content:
                logger.warning(
                    f"No HTML content in AJAX response for store {store_value}"
                )
                return []

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse AJAX response: {e}")
            return []

        # Parse HTML to extract CSV links
        soup = BeautifulSoup(html_content, "html.parser")
        csv_links = []

        # Find all download links
        download_links = soup.find_all("a", class_="download-button")

        for link in download_links:
            href = link.get("href")
            if not href or not href.endswith(".csv"):
                continue

            # Extract date from the table row
            row = link.find_parent("tr")
            if not row:
                continue

            date_cell = row.find_all("td")[2]  # Third column contains the date
            if not date_cell:
                continue

            date_text = date_cell.get_text(strip=True)
            match = self.DATE_PATTERN.match(date_text)
            if not match:
                continue

            # Parse date
            day, month, year = match.groups()
            file_date = datetime.date(int(year), int(month), int(day))

            if file_date == date:
                csv_links.append(href)

        logger.debug(f"Found {len(csv_links)} CSV files for {store_value} on {date}")
        return csv_links

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Main method to fetch and parse all products from Boso's price lists.

        Args:
            date: The date for which to fetch the price list

        Returns:
            List of Store objects, each containing its products.

        Raises:
            ValueError: If stores cannot be fetched or parsed
        """
        # Get all stores
        stores_info = self.get_stores()

        if not stores_info:
            raise ValueError("No stores found")

        logger.info(f"Processing {len(stores_info)} stores for date {date}")

        stores = []

        for store_value, store_info in stores_info.items():
            try:
                # Get CSV links for this store and date
                csv_links = self.get_csv_links_for_store(store_value, date)

                if not csv_links:
                    logger.debug(
                        f"No CSV files found for {store_info['store_code']} on {date}"
                    )
                    continue

                # Create store object
                store = Store(
                    chain=self.CHAIN,
                    store_id=store_info["store_code"],
                    name=f"{store_info['store_type'].title()} {store_info['city']}",
                    store_type=store_info["store_type"],
                    city=store_info["city"],
                    street_address=store_info["street_address"],
                    items=[],
                )

                # Process each CSV file for this store
                for csv_url in csv_links:
                    try:
                        # Download and parse CSV
                        csv_content = self.fetch_text(csv_url, ["utf-8"])
                        if not csv_content:
                            logger.warning(f"Failed to download CSV from {csv_url}")
                            continue

                        products = self.parse_csv(csv_content, ";")
                        store.items.extend(products)

                    except Exception as e:
                        logger.error(
                            f"Error processing CSV from {csv_url}: {e}", exc_info=True
                        )
                        continue

                if store.items:
                    stores.append(store)
                    logger.debug(
                        f"Added store {store.name} with {len(store.items)} products"
                    )

            except Exception as e:
                logger.error(
                    f"Error processing store {store_info['store_code']}: {e}",
                    exc_info=True,
                )
                continue

        logger.info(f"Successfully processed {len(stores)} stores")
        return stores


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = BosoCrawler()
    stores = crawler.get_all_products(datetime.date.today())
    print(f"Found {len(stores)} stores")
    print(f"First store: {stores[0]}")
    print(f"First product: {stores[0].items[0]}")
