import datetime
import logging
import os
import re
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from lxml import etree  # type: ignore

from crawler.store.models import Product, Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)


class TrgocentarCrawler(BaseCrawler):
    """
    Crawler for Trgocentar store prices.

    Until 2026-11-16 Trgocentar published one XML file per store (6 stores)
    on the index page. Since 2026-11-17 it publishes one ZIP per day with an
    XML file per store (95 stores), using new tag names: a single price
    (<mpc>) with the form of sale in <naz_prod>, and the anchor price in
    <bazna_cijena>. Both are read; the ZIP is used when the date has one.
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

    # Since 2026-11-17, files inside the daily ZIP:
    # <OIB>_<store_type>_<address_city>_P<store_id>_roba_<serial>_<YYYYMMDD>_<HHMM>.xml
    # Example: 84210581427_PRODAVAONA_Antuna_Mihanovića_4_Krapina_P145_roba_2026001_20261117_0937.xml
    NEW_FILENAME_PATTERN = re.compile(
        r"^(?P<oib>\d+)_"
        r"(?P<store_type>[^_]+)_"
        r"(?P<address_city>.+?)_"
        r"P(?P<store_id>\d+)_"
        r"roba_(?P<serial>\d+)_"
        r"(?P<date>\d{8})_"
        r"(?P<time>\d{4})\.xml$"
    )

    # The daily ZIP, e.g. 84210581427_cjenici_trgovine_roba_20261117_0942.zip
    ZIP_PATTERN = re.compile(r"_roba_(?P<date>\d{8})_(?P<time>\d{4})\.zip$")

    # Known cities to detect and separate from address
    CITIES = [
        "HUM NA SUTLI",
        "ZLATAR",
        "SV IVAN ZELINA",
        "SV KRIZ ZACRETJE",
        "SVETI KRIZ ZACRETJE",
        "ZABOK",
        "ZAPRESIC",
    ]

    # Applies to <c_020525> only. <bazna_cijena> gives no date, so rows read
    # from it get none (see parse_xml_product).
    ANCHOR_PRICE_DATE = datetime.date(2025, 5, 2)
    DATED_ANCHOR_TAG = "c_020525"

    # Where a tuple is given, the first tag is the old one and the second
    # the one used since 2026-11-17.
    PRICE_MAP = {
        "price": ("mpc", False),
        "unit_price": (("c_jmj", "cijena_jmj"), False),
        "special_price": ("mpc_pop", False),
        "best_price_30": ("c_najniza_30", False),
        "anchor_price": ((DATED_ANCHOR_TAG, "bazna_cijena"), False),
    }

    FIELD_MAP = {
        "product": ("naziv_art", True),
        "product_id": ("sif_art", True),
        "brand": ("marka", False),
        "quantity": (("net_kol", "neto_kolicina"), False),
        "unit": ("jmj", False),
        # <barkod> was added on 2026-11-17 but is empty; <ean_kod> has the code
        "barcode": (("ean_kod", "barkod"), False),
        "category": ("naz_kat", False),
        "special_sale_type": ("naz_prod", False),
    }

    REQUIRED_COLUMNS = [
        "mpc",
        ("c_jmj", "cijena_jmj"),
        (DATED_ANCHOR_TAG, "bazna_cijena"),
        "naziv_art",
        "sif_art",
        "marka",
        "jmj",
        ("ean_kod", "barkod"),
    ]

    # The first four aren't required by NN 101/2026, and the files since
    # 2026-11-17 don't have the last three of them. <naz_prod> was added then.
    OPTIONAL_COLUMNS = [
        ("net_kol", "neto_kolicina"),
        "naz_kat",
        "mpc_pop",
        "c_najniza_30",
        "naz_prod",
    ]

    # <naz_prod> value for a regular price, i.e. no special form of sale
    REGULAR_SALE_TYPE = "REDOVNO"

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

    def get_zip_url_for_date(self, content: str, date: datetime.date) -> str | None:
        """
        Find the daily ZIP for the date on the index page (since 2026-11-17).

        Args:
            content: HTML content of the index page
            date: The date to look for

        Returns:
            URL of the latest ZIP for the date, or None if there is none
        """
        soup = BeautifulSoup(content, "html.parser")
        date_str = f"{date:%Y%m%d}"

        candidates = []
        for link_tag in soup.select('a[href$=".zip"]'):
            href = str(link_tag.get("href"))
            match = self.ZIP_PATTERN.search(os.path.basename(href))
            if match and match.group("date") == date_str:
                candidates.append((match.group("time"), urljoin(self.INDEX_URL, href)))

        if not candidates:
            return None

        # If the chain republished during the day, take the latest one
        return max(candidates)[1]

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

    def split_address_city(self, address_city_raw: str) -> tuple[str, str]:
        """
        Split address and city from a filename used since 2026-11-17.

        These names keep their case and diacritics, e.g.
        "Antuna_Mihanovića_4_Krapina" or "Kraljevec_Hrašćina". A known city at
        the end wins; otherwise the city is whatever follows the house number
        (the last word with a digit, or "bb"); otherwise the last word.

        Args:
            address_city_raw: Raw address+city string with underscores

        Returns:
            Tuple of (street_address, city)
        """
        words = address_city_raw.replace("_", " ").split()
        folded = self.strip_diacritics(" ".join(words)).upper()

        for city in self.CITIES:
            if folded.endswith(f" {city}"):
                n = len(city.split())
                return " ".join(words[:-n]), " ".join(words[-n:])

        for i in range(len(words) - 2, -1, -1):
            if words[i].lower() == "bb" or any(ch.isdigit() for ch in words[i]):
                return " ".join(words[: i + 1]), " ".join(words[i + 1 :])

        return " ".join(words[:-1]), words[-1]

    def parse_store_info(self, xml_url: str) -> Store:
        """
        Parse store information from an XML file URL or name.

        Args:
            xml_url: URL or name of the XML file containing store/product data

        Returns:
            Store object with parsed store information
        """
        logger.debug(f"Parsing store information from Trgocentar URL: {xml_url}")

        filename = os.path.basename(xml_url)

        if match := self.FILENAME_PATTERN.match(filename):
            data = match.groupdict()
            street_address, city = self.parse_address_city(data["address_city"])
        elif match := self.NEW_FILENAME_PATTERN.match(filename):
            data = match.groupdict()
            street_address, city = self.split_address_city(data["address_city"])
        else:
            raise ValueError(f"Invalid XML filename format for Trgocentar: {filename}")

        store_type = data["store_type"].lower()
        store_id = f"P{data['store_id']}"

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
            # Recover from bad character references: one file on 2026-11-17
            # had "&#x2;" in a product name, which fails a strict parse and
            # would lose the whole store.
            root = etree.fromstring(xml_content, etree.XMLParser(recover=True))
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

    def parse_xml_product(self, elem: Any) -> Product:
        product = super().parse_xml_product(elem)

        # <bazna_cijena> gives no date, so ANCHOR_PRICE_DATE doesn't apply
        if elem.find(self.DATED_ANCHOR_TAG) is None:
            product.anchor_price_date = None

        return product

    def fix_product_data(self, data: dict[str, Any]) -> dict[str, Any]:
        # Since 2026-11-17 text values are padded, e.g. <jmj>L  </jmj>
        for field in ("product", "product_id", "brand", "quantity", "unit"):
            data[field] = (data.get(field) or "").strip()

        sale_type = (data.get("special_sale_type") or "").strip()
        if sale_type.upper() == self.REGULAR_SALE_TYPE:
            data["special_sale_type"] = None

        # Since 2026-11-17 <mpc> is the only price and holds the sale price
        # on rows with a form of sale ("AKCIJA"). Checked against 2026-11-16:
        # on 2,895 of 2,920 such rows it equals the previous <mpc_pop>.
        elif sale_type and data.get("special_price") is None:
            data["special_price"] = data.get("price")

        return super().fix_product_data(data)

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

    def get_index_urls_for_date(self, content: str, date: datetime.date) -> list[str]:
        """
        Get the per-store XML URLs for the specified date (until 2026-11-16).

        Args:
            content: HTML content of the index page
            date: The date to search for in the XML filenames (DDMMYYYY format).

        Returns:
            List of XML URLs containing data for the specified date.
        """
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

    def get_zip_stores(self, zip_url: str) -> list[Store]:
        """
        Download the daily ZIP and parse the XML file of each store in it.

        Args:
            zip_url: URL of the daily ZIP

        Returns:
            List of Store objects with their products.
        """
        logger.info(f"Fetching Trgocentar ZIP: {zip_url}")
        stores = []

        for filename, xml_content in self.get_zip_contents(zip_url, ".xml"):
            try:
                store = self.parse_store_info(filename)
                store.items = self.parse_xml(xml_content)
            except Exception as e:
                logger.error(
                    f"Error processing Trgocentar store from {filename}: {e}",
                    exc_info=True,
                )
                continue

            if not store.items:
                logger.warning(f"No products found for Trgocentar store {filename}")
                continue

            stores.append(store)

        return stores

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Main method to fetch and parse all Trgocentar store, product, and price info for a given date.

        Args:
            date: The date to search for in the price list.

        Returns:
            List of Store objects with their products.
        """
        content = self.fetch_text(self.INDEX_URL)

        if not content:
            logger.warning(
                f"No content found at Trgocentar index URL: {self.INDEX_URL}"
            )
            return []

        zip_url = self.get_zip_url_for_date(content, date)
        if zip_url:
            return self.get_zip_stores(zip_url)

        xml_urls = self.get_index_urls_for_date(content, date)

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
