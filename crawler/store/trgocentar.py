import datetime
import logging
import os
import re
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from lxml import etree  # type: ignore

from crawler.store.models import Product, Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)


class TrgocentarCrawler(BaseCrawler):
    """
    Crawler for Trgocentar store prices.

    This class handles downloading and parsing price data from Trgocentar's website.
    It fetches the HTML index page to find XML files for each store, downloads them,
    and parses the XML data to create a structured representation of stores and their products.
    """

    CHAIN = "trgocentar"
    BASE_URL = "https://trgocentar.com"
    INDEX_URL = "https://trgocentar.com/Trgovine-cjenik/"

    # Regex to parse store information from XML filename
    # Format: <store_type>_<address_parts>_P<store_id>_<serial>_<DDMMYYYY><time>.xml
    # Example: SUPERMARKET_VL_NAZORA_58_SV_IVAN_ZELINA_P120_009_230520250745.xml
    FILENAME_PATTERN = re.compile(
        r"^(?P<store_type>[^_]+)_"
        r"(?P<address_city>.+?)_"
        r"P(?P<store_id>\d+)_"
        r"(?P<serial>\d+)_"
        r"(?P<date>\d{8})"
        r"(?P<time>\d+)\.xml$"
    )

    # Known cities to detect and separate from address
    CITIES = [
        "HUM NA SUTLI",
        "ZLATAR",
        "SV IVAN ZELINA",
        "SV KRIZ ZACRETJE",
        "ZABOK",
        "ZAPRESIC",
    ]

    PRICE_MAP = {
        "price": ("mpc", False),
        "unit_price": ("c_jmj", False),
        "special_price": ("mpc_pop", False),
        "best_price_30": ("c_najniza_30", False),
        "anchor_price": ("c_020525", False),
    }

    FIELD_MAP = {
        "product": ("naziv_art", True),
        "product_id": ("sif_art", True),
        "brand": ("marka", False),
        "quantity": ("net_kol", False),
        "unit": ("jmj", False),
        "barcode": ("ean_kod", False),
        "category": ("naz_kat", False),
    }

    REQUIRED_COLUMNS = [
        "mpc",
        "c_jmj",
        "mpc_pop",
        "c_najniza_30",
        "c_020525",
        "naziv_art",
        "sif_art",
        "marka",
        "net_kol",
        "jmj",
        "ean_kod",
        "naz_kat",
    ]

    def parse_index(self, content: str) -> list[str]:
        """
        Parse the Trgocentar index page to extract XML file URLs.

        Args:
            content: HTML content of the index page

        Returns:
            List of XML file URLs found on the page
        """
        soup = BeautifulSoup(content, "html.parser")
        urls = []

        # Find all links ending with .xml
        for link_tag in soup.select('a[href$=".xml"]'):
            href = str(link_tag.get("href"))
            full_url = urljoin(self.INDEX_URL, href)
            urls.append(full_url)

        return list(set(urls))

    def parse_address_city(self, address_city_raw: str) -> tuple[str, str]:
        """
        Parse address and city from the combined string.

        Args:
            address_city_raw: Raw address+city string with underscores

        Returns:
            Tuple of (street_address, city)
        """
        # Convert underscores to spaces
        address_city = address_city_raw.replace("_", " ")

        # Check if it ends with any known city
        for city in self.CITIES:
            if address_city.endswith(city):
                # Strip city from the end to get address
                street_address = address_city[: -len(city)].strip()
                return street_address.title(), city.title()

        # No known city found, treat entire string as address
        return address_city.title(), ""

    def parse_store_info(self, xml_url: str) -> Store:
        """
        Parse store information from an XML file URL.

        Args:
            xml_url: URL to the XML file containing store/product data

        Returns:
            Store object with parsed store information
        """
        logger.debug(f"Parsing store information from Trgocentar URL: {xml_url}")

        filename = os.path.basename(xml_url)
        match = self.FILENAME_PATTERN.match(filename)

        if not match:
            raise ValueError(f"Invalid XML filename format for Trgocentar: {filename}")

        data = match.groupdict()

        store_type = data["store_type"].lower()
        store_id = f"P{data['store_id']}"
        street_address, city = self.parse_address_city(data["address_city"])

        store = Store(
            chain=self.CHAIN,
            store_type=store_type,
            store_id=store_id,
            name=f"{self.CHAIN.capitalize()} {city} {store_id}".strip(),
            street_address=street_address,
            zipcode="",
            city=city,
            items=[],
        )

        logger.info(
            f"Parsed Trgocentar store: {store.name}, Type: {store.store_type}, "
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

            product_elems = root.xpath("//cjenik")
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
        Fetch and parse both store info and products from a Trgocentar XML URL.

        Args:
            xml_url: URL to the XML file

        Returns:
            Store populated with with Products
        """
        try:
            store = self.parse_store_info(xml_url)

            xml_content = self.fetch_text(xml_url).encode("utf-8")
            products = self.parse_xml(xml_content)
            store.items = products
            return store
        except Exception as e:
            logger.error(
                f"Failed to get Trgocentar store data from {xml_url}: {e}",
                exc_info=True,
            )
            raise

    def get_index_urls_for_date(self, date: datetime.date) -> list[str]:
        """
        Fetch and parse the Trgocentar index page to get XML URLs for the specified date.

        Args:
            date: The date to search for in the XML filenames (DDMMYYYY format).

        Returns:
            List of XML URLs containing data for the specified date.
        """
        content = self.fetch_text(self.INDEX_URL)

        if not content:
            logger.warning(
                f"No content found at Trgocentar index URL: {self.INDEX_URL}"
            )
            return []

        all_urls = self.parse_index(content)

        # Date format in Trgocentar filenames is DDMMYYYY
        date_str = date.strftime("%d%m%Y")

        matching_urls = []
        for url in all_urls:
            filename = os.path.basename(url)
            # Check if the DDMMYYYY date string is in the filename with underscore prefix
            if f"_{date_str}" in filename:
                matching_urls.append(url)

        if not matching_urls:
            logger.warning(f"No Trgocentar URLs found matching date {date:%Y-%m-%d}")

        return matching_urls

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Main method to fetch and parse all Trgocentar store, product, and price info for a given date.

        Args:
            date: The date to search for in the price list.

        Returns:
            List of Store objects with their products.
        """
        xml_urls = self.get_index_urls_for_date(date)

        if not xml_urls:
            logger.warning(f"No Trgocentar XML URLs found for date {date.isoformat()}")
            return []

        stores = []
        for url in xml_urls:
            try:
                store = self.get_store_data(url)
            except Exception as e:
                logger.error(
                    f"Error processing Trgocentar store from {url}: {e}", exc_info=True
                )
                continue

            if not store.items:
                logger.warning(
                    f"No products found for Trgocentar store at {url}, skipping."
                )
                continue

            stores.append(store)

        return stores


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = TrgocentarCrawler()
    stores = crawler.crawl(datetime.date.today())
    print(stores[0])
    print(stores[0].items[0])
