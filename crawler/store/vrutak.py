import datetime
import logging
import os
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from lxml import etree  # type: ignore

from crawler.store.models import Product, Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)


class VrutakCrawler(BaseCrawler):
    """
    Crawler for Vrutak store prices.

    This class handles downloading and parsing price data from Vrutak's website.
    It fetches the HTML index page to find XML files for each store, downloads them,
    and parses the XML data to create a structured representation of stores and their products.
    """

    CHAIN = "vrutak"
    BASE_URL = "https://www.vrutak.hr"
    INDEX_URL = "https://www.vrutak.hr/cjenik-svih-artikala"

    # Known store types
    STORE_TYPES = ["hipermarket", "supermarket"]

    PRICE_MAP = {
        "price": ("mpcijena", True),
        "unit_price": ("mpcijenamjera", False),
        "special_price": ("", False),  # No equivalent in Vrutak XML
        "best_price_30": ("", False),  # No equivalent in Vrutak XML
        "anchor_price": ("", False),  # No equivalent in Vrutak XML
    }

    FIELD_MAP = {
        "product": ("naziv", True),
        "product_id": ("sifra", True),
        "brand": ("marka", False),
        "quantity": ("nettokolicina", False),
        "unit": ("mjera", False),
        "barcode": ("barkod", False),
        "category": ("kategorija", False),
    }

    REQUIRED_COLUMNS = [
        "mpcijena",
        "mpcijenamjera",
        "naziv",
        "sifra",
        "marka",
        "nettokolicina",
        "mjera",
        "barkod",
        "kategorija",
    ]

    def parse_index(self, content: str) -> dict[datetime.date, list[str]]:
        """
        Parse the Vrutak index page to extract XML file URLs grouped by date.

        Args:
            content: HTML content of the index page

        Returns:
            Dictionary mapping dates to lists of XML file URLs
        """
        soup = BeautifulSoup(content, "html.parser")
        urls_by_date = {}

        # Find all rows in tbody
        for row in soup.select("tbody tr"):
            cells = row.select("td")
            if len(cells) < 3:
                continue

            # Second cell contains the date
            date_cell = cells[1]
            date_text = date_cell.get_text(strip=True)

            try:
                # Parse date in DD.MM.YYYY format
                date_obj = datetime.datetime.strptime(date_text, "%d.%m.%Y.").date()
            except ValueError:
                # Non-data row
                continue

            # Extract XML URLs from remaining cells
            xml_urls = []
            for cell in cells[2:]:  # Skip index and date cells
                link = cell.select_one('a[href$=".xml"]')
                if link:
                    href = str(link.get("href"))
                    full_url = urljoin(self.BASE_URL, href)
                    xml_urls.append(full_url)

            if xml_urls:
                urls_by_date[date_obj] = xml_urls

        return urls_by_date

    def parse_store_info(self, xml_url: str) -> Store:
        """
        Parse store information from an XML file URL.

        Args:
            xml_url: URL to the XML file containing store/product data

        Returns:
            Store object with parsed store information
        """
        logger.debug(f"Parsing store information from Vrutak URL: {xml_url}")

        filename = os.path.basename(xml_url)
        # Remove .xml extension and split by dashes
        parts = filename[:-4].split("-")

        if len(parts) < 4:
            raise ValueError(f"Invalid XML filename format for Vrutak: {filename}")

        # Expected format: vrutak-type-address-store_id-serial-datetime
        store_type = parts[1]  # hipermarket or supermarket
        street_address = parts[2].title()
        store_id = parts[3]

        store = Store(
            chain=self.CHAIN,
            store_type=store_type,
            store_id=store_id,
            name=f"{self.CHAIN.capitalize()} {store_type} {store_id}",
            street_address=street_address,
            zipcode="10000",
            city="Zagreb",
            items=[],
        )

        logger.info(
            f"Parsed Vrutak store: {store.name}, Type: {store.store_type}, "
            f"Address: {store.street_address}, City: {store.city}"
        )
        return store

    def parse_xml(self, xml_content: bytes) -> list[Product]:
        """
        Parse XML content into a list of products.

        Args:
            xml_content: XML content as bytes

        Returns:
            List of Product objects parsed from the XML
        """
        try:
            root = etree.fromstring(xml_content)
            products = []

            product_elems = root.xpath("//item")
            self.check_xml_columns(product_elems)

            for product_elem in product_elems:
                try:
                    product = self.parse_xml_product(product_elem)
                    products.append(product)
                except Exception as e:
                    logger.warning(
                        f"Failed to parse product: {etree.tostring(product_elem)}: {e}",
                        exc_info=True,
                    )
                    continue

            logger.debug(f"Parsed {len(products)} products from XML")
            return products

        except Exception as e:
            logger.error(f"Failed to parse XML: {e}", exc_info=True)
            return []

    def get_store_data(self, xml_url: str) -> Store:
        """
        Fetch and parse both store info and products from a Vrutak XML URL.

        Args:
            xml_url: URL to the XML file

        Returns:
            Store populated with Products
        """
        try:
            store = self.parse_store_info(xml_url)

            xml_content = self.fetch_text(xml_url).encode("utf-8")
            products = self.parse_xml(xml_content)
            store.items = products
            return store
        except Exception as e:
            logger.error(
                f"Failed to get Vrutak store data from {xml_url}: {e}",
                exc_info=True,
            )
            raise

    def get_index_urls_for_date(self, date: datetime.date) -> list[str]:
        """
        Fetch and parse the Vrutak index page to get XML URLs for the specified date.

        Args:
            date: The date to search for in the XML filenames.

        Returns:
            List of XML URLs containing data for the specified date.
        """
        content = self.fetch_text(self.INDEX_URL)

        if not content:
            logger.warning(f"No content found at Vrutak index URL: {self.INDEX_URL}")
            return []

        urls_by_date = self.parse_index(content)
        matching_urls = urls_by_date.get(date, [])

        if not matching_urls:
            logger.warning(f"No Vrutak URLs found matching date {date:%Y-%m-%d}")

        return matching_urls

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Main method to fetch and parse all Vrutak store, product, and price info for a given date.

        Args:
            date: The date to search for in the price list.

        Returns:
            List of Store objects with their products.
        """
        xml_urls = self.get_index_urls_for_date(date)

        if not xml_urls:
            logger.warning(f"No Vrutak XML URLs found for date {date.isoformat()}")
            return []

        stores = []
        for url in xml_urls:
            try:
                store = self.get_store_data(url)
            except Exception as e:
                logger.error(
                    f"Error processing Vrutak store from {url}: {e}", exc_info=True
                )
                continue

            if not store.items:
                logger.warning(
                    f"No products found for Vrutak store at {url}, skipping."
                )
                continue

            stores.append(store)

        return stores


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = VrutakCrawler()
    stores = crawler.crawl(datetime.date.today())
    print(stores[0])
    print(stores[0].items[0])
