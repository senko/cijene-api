import datetime
import logging
import re
from typing import Any, Dict, List, Optional

from bs4 import BeautifulSoup

from crawler.store.models import Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)


class TrgovinaKrkCrawler(BaseCrawler):
    """
    Crawler for Trgovina Krk chain stores.

    Retrieves price data from daily CSV files published on their website.
    Each store location has separate CSV files updated daily.
    """

    CHAIN = "trgovina-krk"
    BASE_URL = "https://trgovina-krk.hr"
    INDEX_URL = "https://trgovina-krk.hr/objava-cjenika/"

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
        """Get all products from all store locations."""
        try:
            # Get the index page
            content = self.fetch_text(self.INDEX_URL)
            soup = BeautifulSoup(content, "html.parser")

            # Find all store sections
            store_sections = self._parse_store_sections(soup)

            stores = []

            for store_info in store_sections:
                logger.info(f"Processing store: {store_info['name']}")

                # Get the latest CSV file for this store
                csv_url = store_info["latest_csv_url"]

                # Create store object
                store = Store(
                    chain=self.CHAIN,
                    store_id=store_info["store_id"],
                    name=store_info["name"],
                    store_type="supermarket",
                    city=store_info["city"],
                    street_address=store_info["address"],
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

    def _parse_store_sections(self, soup: BeautifulSoup) -> List[Dict[str, Any]]:
        """
        Parse store sections from the index page.

        Finds all div elements containing store names and extracts store information
        along with their associated CSV download links.

        Args:
            soup: BeautifulSoup object of the index page

        Returns:
            List of dictionaries containing store information and CSV URLs
        """
        store_sections = []

        # Find all div elements containing store names
        for div in soup.find_all("div"):
            # Check if this div contains only the store name
            if div.string and div.string.strip().startswith("Supermarket"):
                store_name = div.string.strip()

                # Parse store info from header
                store_info = self._parse_store_info(store_name)

                # Find the next ul element with CSV links
                next_ul = div.find_next("ul")
                if next_ul:
                    csv_links = next_ul.find_all("a", href=True)
                    if csv_links:
                        # Get the first (most recent) CSV link
                        latest_link = csv_links[0]
                        csv_url = latest_link["href"]  # Already absolute URL

                        store_info["latest_csv_url"] = csv_url
                        store_info["date"] = self._extract_date_from_link(
                            latest_link.get_text()
                        )

                        store_sections.append(store_info)

        return store_sections

    def _parse_store_info(self, header_text: str) -> Dict[str, Any]:
        """
        Parse store information from header text.

        Extracts store name, address, and city from header text format:
        "Supermarket [Address] [City]"

        Args:
            header_text: Header text containing store information

        Returns:
            Dictionary with parsed store information
        """
        # Examples:
        # "Supermarket Set. sv. Bernardina 6C KRK"
        # "Supermarket Trg sv. Jurja 11 A GORNJA STUBICA"
        # Pattern: Supermarket (address ending with number+optional letter) (UPPERCASE CITY)
        pattern = r"Supermarket (.*?[ 0-9][a-zA-Z]?) ([A-Z][ A-Z]*|[A-Z]+)$"
        match = re.match(pattern, header_text)

        if not match:
            raise ValueError(f"Unable to parse store info from: {header_text}")

        address = match.group(1).strip()
        city = match.group(2).strip().title()

        # Generate a simple store ID from the address and city
        store_id = (
            f"{address.replace(' ', '_').lower()}_{city.replace(' ', '_').lower()}"
        )

        return {
            "name": f"Trgovina Krk {city}",
            "address": address,
            "city": city,
            "store_id": store_id,
            "full_name": header_text,
        }

    def _extract_date_from_link(self, link_text: str) -> Optional[str]:
        """Extract date from CSV link text."""
        # Format: "05.07.2025 – filename.csv"
        date_match = re.match(r"(\d{2}\.\d{2}\.\d{4})", link_text)
        return date_match.group(1) if date_match else None

    def _process_csv_file(self, csv_url: str) -> List:
        """Download and process a CSV file."""
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
        """Clean up product data, including collapsing multiple spaces."""
        # Call parent method first
        data = super().fix_product_data(data)

        # Collapse multiple spaces in product name
        if data.get("product"):
            data["product"] = re.sub(r"\s+", " ", data["product"].strip())

        return data


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = TrgovinaKrkCrawler()
    stores = crawler.crawl(datetime.date.today())
    print(stores[0])
    print(stores[0].items[0])
