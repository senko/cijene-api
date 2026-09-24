import datetime
from json import loads
import logging
import re
from typing import Any, Optional, Tuple

from crawler.store.base import BaseCrawler
from crawler.store.models import Store
from crawler.store.utils import to_camel_case

logger = logging.getLogger(__name__)


class TommyCrawler(BaseCrawler):
    """
    Crawler for Tommy store prices.

    This class handles downloading and parsing price data from Tommy's API.
    It retrieves JSON data about available store price tables and processes
    the corresponding CSV files for product information.
    """

    CHAIN = "tommy"
    BASE_URL = "https://spiza.tommy.hr/api/v2"

    PRICE_MAP = {
        "price": ("MPC", False),
        "unit_price": ("CIJENA_PO_JM", False),
        "special_price": ("MPC_POSEBNA_PRODAJA", False),
        "best_price_30": ("MPC_NAJNIZA_30", False),
        "anchor_price": ("MPC_020525", False),
        "initial_price": ("PRVA_CIJENA_NOVOG_ARTIKLA", False),
    }

    FIELD_MAP = {
        "product": ("NAZIV_ARTIKLA", True),
        "product_id": ("SIFRA_ARTIKLA", True),
        "barcode": ("BARKOD_ARTIKLA", False),
        "brand": ("BRAND", False),
        "category": ("ROBNA_STRUKTURA", False),
        "unit": ("JEDINICA_MJERE", False),
        "quantity": ("NETO_KOLICINA", False),
        "date_added": ("DATUM_ULASKA_NOVOG_ARTIKLA", False),
    }

    REQUIRED_COLUMNS = [
        "MPC",
        "CIJENA_PO_JM",
        "MPC_POSEBNA_PRODAJA",
        "MPC_NAJNIZA_30",
        "MPC_020525",
        "PRVA_CIJENA_NOVOG_ARTIKLA",
        "NAZIV_ARTIKLA",
        "SIFRA_ARTIKLA",
        "BARKOD_ARTIKLA",
        "BRAND",
        "ROBNA_STRUKTURA",
        "JEDINICA_MJERE",
        "NETO_KOLICINA",
        "DATUM_ULASKA_NOVOG_ARTIKLA",
    ]

    def fetch_stores_list(self, date: datetime.date) -> dict[str, str]:
        """
        Fetch the list of store price tables for a specific date.

        Args:
            date: The date for which to fetch the price tables

        Returns:
            List of dictionaries containing store price table information

        Raises:
            httpx.RequestError: If the API request fails
            ValueError: If the response cannot be parsed
        """
        url = (
            f"{self.BASE_URL}/shop/store-prices-tables"
            f"?date={date:%Y-%m-%d}&page=1&itemsPerPage=200&channelCode=general"
        )
        content = self.fetch_text(url)
        data = loads(content)
        store_list = data.get("hydra:member", [])

        stores = {}
        for store in store_list:
            csv_id = store.get("@id")
            filename = store.get("fileName", "Unknown")
            if not csv_id or not filename:
                logger.warning(
                    f"Skipping store with missing CSV ID or filename: {store}"
                )
                continue
            if csv_id.startswith("/api/v2"):
                csv_id = csv_id[len("/api/v2") :]

            stores[filename] = self.BASE_URL + csv_id

        return stores

    def parse_date_string(self, date_str: str) -> Optional[datetime.date]:
        """
        Parse date string from CSV (format DD.MM.YYYY. HH:MM:SS).

        Args:
            date_str: The date string to parse (e.g., "16.5.2025. 0:00:00")

        Returns:
            datetime.date object or None if parsing fails
        """
        if not date_str or date_str.strip() == "":
            return None

        try:
            match = re.match(r"(\d{1,2})\.(\d{1,2})\.(\d{4})\.", date_str)

            if match:
                day, month, year = map(int, match.groups())
                return datetime.date(year, month, day)
            else:
                logger.warning(f"Date string format not recognized: {date_str}")
                return None

        except (ValueError, IndexError) as e:
            logger.warning(f"Failed to parse date string '{date_str}': {e}")
            return None

    def fix_product_data(self, data: dict[str, Any]) -> dict[str, Any]:
        # Parse date_added from string to date before the base class processes it
        date_str = data.get("date_added", "")
        if date_str:
            data["date_added"] = self.parse_date_string(date_str)
        else:
            data["date_added"] = None

        return super().fix_product_data(data)

    def parse_store_from_filename(
        self, filename: str
    ) -> Tuple[str, str, str, str, str]:
        """
        Parse store information from the filename.

        Args:
            filename: The filename from the API

        Returns:
            Tuple of (store_type, store_id, address, zipcode, city)

        Example:
            "SUPERMARKET, ANTE STARČEVIĆA 6, 20260 KORČULA, 10180, 2, 20250516 0530"
            Will return:
            ("supermarket", "10180", "Ante Starčevića 6", "20260", "Korčula")
        """
        try:
            # Split by commas
            parts = filename.split(",")

            if len(parts) < 3:
                logger.warning(f"Filename doesn't have enough parts: {filename}")
                raise ValueError(f"Unparseable filename: {filename}")

            # Extract store type (first part)
            store_type = parts[0].strip().lower()

            # Extract address (second part)
            address = to_camel_case(parts[1].strip())

            # Extract zipcode and city (third part)
            location_part = parts[2].strip()

            # Use regex to extract zipcode and city
            # Pattern looks for 5 digits followed by any text
            match = re.match(r"(\d{5})\s+(.+)", location_part)

            if match:
                zipcode = match.group(1)
                city = to_camel_case(match.group(2))
            else:
                logger.warning(
                    f"Could not extract zipcode and city from: {location_part}"
                )
                zipcode = ""
                # Try to extract just the city if no zipcode pattern found
                city = to_camel_case(location_part)

            store_id = parts[3].strip()

            logger.debug(
                f"Parsed store info: type={store_type}, address={address}, zipcode={zipcode}, city={city}"
            )

            return (store_type, store_id, address, zipcode, city)

        except Exception as e:
            logger.error(f"Error parsing store from filename {filename}: {e}")
            raise

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Main method to fetch and parse all products from Tommy's price lists.

        Args:
            date: The date for which to fetch the price list

        Returns:
            List of Store objects, each containing its products.

        Raises:
            ValueError: If the price list cannot be fetched or parsed
        """

        store_map = self.fetch_stores_list(date)
        if not store_map:
            logger.warning(f"No stores found for date {date}")
            return []

        stores = []
        for filename, url in store_map.items():
            # Extract store information
            store_type, store_id, address, zipcode, city = (
                self.parse_store_from_filename(filename)
            )

            store = Store(
                chain="tommy",
                name=f"Tommy {store_type.title()} {address}",
                store_type=store_type,
                store_id=store_id,
                city=city,
                street_address=address,
                zipcode=zipcode,
                items=[],
            )

            csv_content = self.fetch_text(url)
            products = self.parse_csv(csv_content)

            store.items = products
            stores.append(store)

        return stores


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = TommyCrawler()
    current_date = datetime.date.today() - datetime.timedelta(days=1)
    stores = crawler.get_all_products(current_date)
    print(stores[0])
    print(stores[0].items[0])
