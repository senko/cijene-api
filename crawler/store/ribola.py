import datetime
import logging
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from lxml import etree  # type: ignore

from crawler.store.models import Product, Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)


class RibolaCrawler(BaseCrawler):
    """
    Crawler for Ribola store prices.

    This class handles downloading and parsing price data from Ribola's website.
    It fetches the HTML index page to find XML files for each store, downloads them,
    and parses the XML data to create a structured representation of stores and their products.
    """

    CHAIN = "ribola"
    BASE_URL = "https://ribola.hr"
    INDEX_URL = f"{BASE_URL}/ribola-cjenici/"

    # Known cities for address parsing
    CITIES = [
        "Kastel Sucurac",
        "Ploče",
        "Kaštel Gomilica",
        "Trogir",
        "Kaštel Lukšić",
        "Okrug Gornji",
        "Makarska",
        "Kaštel Stari",
        "Kaštel Novi",
        "Kastel Kambelovac",
        "Split",
        "Sinj",
        "Solin",
        "Orebić",
        "Nečujam",
        "Dubrovnik",
        "Podstrana",
        "Dugi Rat",
        "Ražanj",
        "Primošten",
        "Jelsa",
        "Stobrec",
        "Trilj",
        "Seget Donji",
        "Brela",
        "Šibenik",
        "Zadar",
    ]

    PRICE_MAP = {
        "price": ("MaloprodajnaCijena", False),
        "unit_price": ("CijenaZaJedinicuMjere", False),
        "special_price": ("MaloprodajnaCijenaAkcija", False),
        "best_price_30": ("NajnizaCijena", False),
        "anchor_price": ("SidrenaCijena", False),
    }

    FIELD_MAP = {
        "product": ("NazivProizvoda", True),
        "product_id": ("SifraProizvoda", True),
        "brand": ("MarkaProizvoda", False),
        "quantity": ("NetoKolicina", False),
        "unit": ("JedinicaMjere", False),
        "barcode": ("Barkod", False),
        "category": ("KategorijeProizvoda", False),
    }

    REQUIRED_COLUMNS = [
        "MaloprodajnaCijena",
        "CijenaZaJedinicuMjere",
        "MaloprodajnaCijenaAkcija",
        "NajnizaCijena",
        "SidrenaCijena",
        "NazivProizvoda",
        "SifraProizvoda",
        "MarkaProizvoda",
        "NetoKolicina",
        "JedinicaMjere",
        "Barkod",
        "KategorijeProizvoda",
    ]

    def parse_index(self, content: str) -> list[str]:
        """
        Parse the Ribola index page to extract XML file URLs.

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

    def parse_address_city(self, address_raw: str) -> tuple[str, str]:
        """
        Parse address and city from the combined string.

        Args:
            address_raw: Raw address string containing both street and city

        Returns:
            Tuple of (street_address, city)
        """
        address = address_raw.strip()

        # Check if it ends with any known city
        for city in self.CITIES:
            addr_norm = self.strip_diacritics(address.lower())
            city_norm = self.strip_diacritics(city.lower())

            if addr_norm.endswith(city_norm):
                # Strip city from the end to get street address
                street_address = address[: -len(city)].strip()
                return street_address, city

        # No known city found, treat entire string as address
        return address, ""

    def parse_store_info_from_xml(self, root: etree._Element) -> Store:
        """
        Parse store information from XML root element.

        Args:
            root: XML root element containing store data

        Returns:
            Store object with parsed store information
        """
        # Find the ProdajniObjekt element
        store_elem = root.find(".//ProdajniObjekt")
        if store_elem is None:
            raise ValueError("No ProdajniObjekt element found in XML")

        # Extract store information
        store_type_elem = store_elem.find("Oblik")
        store_type = (
            store_type_elem.text.lower()
            if store_type_elem is not None and store_type_elem.text
            else ""
        )

        store_id_elem = store_elem.find("Oznaka")
        store_id = (
            store_id_elem.text
            if store_id_elem is not None and store_id_elem.text
            else ""
        )

        address_elem = store_elem.find("Adresa")
        address_raw = (
            address_elem.text if address_elem is not None and address_elem.text else ""
        )

        street_address, city = self.parse_address_city(address_raw)

        store = Store(
            chain=self.CHAIN,
            store_type=store_type,
            store_id=store_id,
            name=f"{self.CHAIN.capitalize()} {city} {store_id}".strip(),
            street_address=street_address,
            zipcode="",
            city=city.title(),
            items=[],
        )

        logger.info(
            f"Parsed Ribola store: {store.name}, Type: {store.store_type}, "
            f"Address: {store.street_address}, City: {store.city}"
        )
        return store

    def parse_xml(self, xml_content: bytes) -> tuple[Store, list[Product]]:
        """
        Parse XML content into store info and list of products.

        Args:
            xml_content: XML content as bytes

        Returns:
            Tuple of (Store object, List of Product objects)
        """
        try:
            root = etree.fromstring(xml_content)

            # Parse store information
            store = self.parse_store_info_from_xml(root)

            # Parse products
            product_elems = root.xpath("//Proizvod")
            self.check_xml_columns(product_elems)

            products = []
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
            return store, products

        except Exception as e:
            logger.error(f"Failed to parse XML: {e}", exc_info=True)
            raise

    def get_store_data(self, xml_url: str) -> Store:
        """
        Fetch and parse both store info and products from a Ribola XML URL.

        Args:
            xml_url: URL to the XML file

        Returns:
            Store populated with Products
        """
        try:
            logger.debug(f"Fetching Ribola store data from: {xml_url}")

            xml_content = self.fetch_text(xml_url).encode("utf-8")
            store, products = self.parse_xml(xml_content)
            store.items = products
            return store
        except Exception as e:
            logger.error(
                f"Failed to get Ribola store data from {xml_url}: {e}",
                exc_info=True,
            )
            raise

    def get_index_urls_for_date(self, date: datetime.date) -> list[str]:
        """
        Fetch and parse the Ribola index page to get XML URLs for the specified date.

        Args:
            date: The date to search for in the price list.

        Returns:
            List of XML URLs containing data for the specified date.
        """
        index_url = f"{self.INDEX_URL}?date={date:%d.%m.%Y}"

        logger.debug(f"Fetching Ribola index page: {index_url}")

        content = self.fetch_text(index_url)
        if not content:
            logger.warning(f"No content found at Ribola index URL: {index_url}")
            return []

        xml_urls = list(set(self.parse_index(content)))

        if not xml_urls:
            logger.warning(f"No Ribola XML URLs found for date {date:%Y-%m-%d}")

        return xml_urls

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Main method to fetch and parse all Ribola store, product, and price info for a given date.

        Args:
            date: The date to search for in the price list.

        Returns:
            List of Store objects with their products.
        """
        xml_urls = self.get_index_urls_for_date(date)

        if not xml_urls:
            logger.warning(f"No Ribola XML URLs found for date {date.isoformat()}")
            return []

        stores = []
        for url in xml_urls:
            try:
                store = self.get_store_data(url)
            except Exception as e:
                logger.error(
                    f"Error processing Ribola store from {url}: {e}", exc_info=True
                )
                continue

            if not store.items:
                logger.warning(
                    f"No products found for Ribola store at {url}, skipping."
                )
                continue

            stores.append(store)

        return stores


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = RibolaCrawler()
    stores = crawler.crawl(datetime.date.today())
    print(stores[0])
    print(stores[0].items[0])
