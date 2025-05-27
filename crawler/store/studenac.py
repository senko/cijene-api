import datetime
import logging
from pathlib import Path
import re
import subprocess
from tempfile import TemporaryDirectory
from typing import Generator, Optional, Tuple

from lxml import etree  # type: ignore

from crawler.store.models import Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)


class StudenacCrawler(BaseCrawler):
    """
    Crawler for Studenac store prices.

    This class handles downloading and parsing price data from Studenac's website.
    It fetches the ZIP file containing XML files for each store, extracts them,
    and parses the XML data to create a structured representation of stores and their products.
    """

    CHAIN = "studenac"
    BASE_URL = "https://www.studenac.hr"
    TIMEOUT = 120.0  # Longer timeout for ZIP download

    PRICE_MAP = {
        "price": ("MaloprodajnaCijena", False),
        "unit_price": ("CijenaPoJedinici", False),
        "special_price": ("MaloprodajnaCijenaAkcija", False),
        "best_price_30": ("NajnizaCijena", False),
        "anchor_price": ("SidrenaCijena", False),
    }

    FIELD_MAP = {
        "product": ("NazivProizvoda", False),
        "product_id": ("SifraProizvoda", True),
        "brand": ("MarkaProizvoda", False),
        "quantity": ("NetoKolicina", False),
        "unit": ("JedinicaMjere", False),
        "barcode": ("Barkod", False),
        "category": ("KategorijeProizvoda", False),
    }

    def parse_address(self, address: str) -> Tuple[str, str]:
        """
        Parse the address string into street address and city components.

        Args:
            address: Address string in format "<street> <number> <CITY>"

        Returns:
            Tuple of (street_address, city)
        """
        logger.debug(f"Parsing address: {address}")

        try:
            # The regex matches the last set of uppercase words (city)
            # and everything before it (street address)
            pattern = r"^(.*?)([A-ZČĆĐŠŽ][A-ZČĆĐŠŽ\s]+)$"
            match = re.match(pattern, address)

            if match:
                street_address, city = match.groups()
                return (
                    street_address.strip().title(),
                    city.strip().title(),
                )

            logger.warning(f"Failed to parse address: {address}")
            return address.strip().title(), ""
        except Exception as e:
            logger.warning(f"Error parsing address {address}: {e}", exc_info=True)
            return address.strip().title(), ""

    def parse_xml(self, xml_content: bytes) -> Optional[Store]:
        """
        Parse XML content into a unified Store object.

        Args:
            xml_content: XML content as bytes

        Returns:
            Store object with parsed store and product information,
            or None if parsing fails
        """
        try:
            root = etree.fromstring(xml_content)

            # Extract store information
            store_type = root.xpath("//ProdajniObjekt/Oblik/text()")[0].lower()
            store_id = root.xpath("//ProdajniObjekt/Oznaka/text()")[0]
            store_code = root.xpath("//ProdajniObjekt/Oznaka/text()")[0]
            address = root.xpath("//ProdajniObjekt/Adresa/text()")[0]

            street_address, city = self.parse_address(address)

            store = Store(
                chain=self.CHAIN,
                name=f"Studenac {store_code}",
                store_type=store_type.lower(),
                store_id=store_id,
                city=city,
                street_address=street_address,
                items=[],
            )

            logger.debug(
                f"Parsed store: {store.name} ({store_id}), {store.store_type}, {store.city}, {store.street_address}"
            )

            # Extract product information
            products = []
            for product_elem in root.xpath("//ProdajniObjekt/Proizvodi/Proizvod"):
                try:
                    product = self.parse_xml_product(product_elem)
                    products.append(product)
                except Exception as e:
                    logger.warning(
                        f"Failed to parse product: {etree.tostring(product_elem)}: {e}",
                        exc_info=True,
                    )
                    continue

            store.items = products
            logger.debug(f"Parsed {len(products)} products for store {store.name}")
            return store

        except Exception as e:
            logger.error(f"Failed to parse XML: {e}", exc_info=True)
            return None

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Main method to fetch and parse all products from Studenac's price lists.

        Args:
            date: The date for which to fetch the price list

        Returns:
            Tuple with the date and the list of Store objects,
            each containing its products.

        Raises:
            ValueError: If the price list cannot be fetched or parsed
        """
        stores = []
        zip_url = f"{self.BASE_URL}/cjenici/PROIZVODI-{date:%Y-%m-%d}.zip"

        for filename, content in self.get_zip_contents(zip_url, ".xml"):
            logger.debug(f"Processing file: {filename}")
            store = self.parse_xml(content)
            if store:
                stores.append(store)

        return stores

    def get_zip_contents(
        self, url: str, suffix: str
    ) -> Generator[tuple[str, bytes], None, None]:
        with TemporaryDirectory() as temp_dir:  # type: ignore
            temp_path = Path(temp_dir)
            temp_zip = temp_path / "archive.zip"
            with open(temp_zip, "wb") as fp:
                self.fetch_binary(url, fp)

            subprocess.run(["unzip", "-x", temp_zip], cwd=temp_dir)

            for file in temp_path.iterdir():
                if file.suffix != suffix:
                    continue

                xml_content = open(file, "rb").read()
                yield file.name, xml_content


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = StudenacCrawler()
    stores = crawler.crawl(datetime.date.today())
    print(stores[0])
    print(stores[0].items[0])
