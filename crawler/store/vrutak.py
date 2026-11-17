import datetime
import logging
import os
from csv import DictReader
from urllib.parse import unquote, urljoin

from bs4 import BeautifulSoup
from lxml import etree  # type: ignore

from crawler.store.models import Product, Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)


class VrutakCrawler(BaseCrawler):
    """
    Crawler for Vrutak store prices.

    The index page lists per-store price files by date. Since 2026-11-16 the
    chain publishes a CSV per location, in a separate table above the original
    XML one. The CSVs cover more locations and carry availability and anchor
    prices for every store, so they are used whenever the date has them. The
    XMLs, published only for the four original stores, are the fallback.
    """

    CHAIN = "vrutak"
    BASE_URL = "https://www.vrutak.hr"
    INDEX_URL = "https://www.vrutak.hr/cjenik-svih-artikala"

    # Store type and address by store ID. The file names spell the address
    # without spaces or diacritics, and the CSV ones call the hipermarket a
    # supermarket, so they are not usable as-is.
    STORES = {
        "001": ("hipermarket", "Vodovodna 20A"),
        "200": ("supermarket", "Ivana Rabara 1A"),
        "600": ("supermarket", "Slavoljuba Bulvana 20"),
        "900": ("supermarket", "Samoborska cesta 161A"),
        "008": ("minimarket", "Krapinska 16"),
        "400": ("minimarket", "Hrvoja Macanovića 10"),
        "500": ("minimarket", "Kaptol 1"),
        "700": ("minimarket", "Lanište 1G"),
        "007": ("webshop", "Vodovodna 20A"),
    }

    # The CSV headers are the XML tag names with spaces ("mpcijena mjera" for
    # <mpcijenamjera>). read_csv removes the spaces, so these maps serve both.
    PRICE_MAP = {
        "price": ("mpcijena", True),
        "unit_price": ("mpcijenamjera", False),
        "special_price": ("mpcijenaakcija", False),  # CSV only
        "best_price_30": ("", False),  # Not published
        "anchor_price": ("mpcijenasidrena", False),
    }

    FIELD_MAP = {
        "product": ("naziv", True),
        "product_id": ("sifra", True),
        "brand": ("marka", False),
        "quantity": ("nettokolicina", False),
        "unit": ("mjera", False),
        "barcode": ("barkod", False),
        "category": ("kategorija", False),  # XML only
        # Published per product: 02.05.2025, or the date it was listed
        "anchor_price_date": ("mpcijenasidrenadatum", False),
    }

    BOOL_MAP = {
        "available": ("raspolozivost", False),  # CSV only, DA/NE
    }

    REQUIRED_COLUMNS = [
        "mpcijena",
        "mpcijenamjera",
        "naziv",
        "sifra",
        "marka",
        "mjera",
        "barkod",
    ]

    # Not required by NN 101/2026, so the chain may drop them. The XMLs have no
    # special price or availability, and their anchor price tags are only in
    # the hipermarket's file (2026-10-23).
    OPTIONAL_COLUMNS = [
        "nettokolicina",
        "mpcijenaakcija",
        "mpcijenasidrena",
        "mpcijenasidrenadatum",
        "raspolozivost",
    ]

    def parse_index(self, content: str) -> dict[datetime.date, list[str]]:
        """
        Parse the Vrutak index page to extract price file URLs grouped by date.

        Both tables on the page (CSV and XML) share the row layout: index, date,
        then one cell per store with a download link.

        Args:
            content: HTML content of the index page

        Returns:
            Dictionary mapping dates to lists of CSV and XML file URLs
        """
        soup = BeautifulSoup(content, "html.parser")
        urls_by_date: dict[datetime.date, list[str]] = {}

        for row in soup.select("tbody tr"):
            cells = row.select("td")
            if len(cells) < 3:
                continue

            date_text = cells[1].get_text(strip=True)
            try:
                date_obj = datetime.datetime.strptime(date_text, "%d.%m.%Y.").date()
            except ValueError:
                # Non-data row
                continue

            urls = []
            for cell in cells[2:]:  # Skip index and date cells
                link = cell.select_one('a[href$=".csv"], a[href$=".xml"]')
                if link:
                    urls.append(urljoin(self.BASE_URL, str(link.get("href"))))

            if urls:
                urls_by_date.setdefault(date_obj, []).extend(urls)

        return urls_by_date

    def parse_store_info(self, url: str) -> Store:
        """
        Parse store information from a price file URL.

        Supported file names:
            `supermarket-vodovodna20a-001-20261116_073500.csv`
            `vrutak-hipermarket-vodovodna20a-001-270-20261116-072800.xml`

        Args:
            url: URL of the CSV or XML price file

        Returns:
            Store object with parsed store information
        """
        logger.debug(f"Parsing store information from Vrutak URL: {url}")

        filename = unquote(os.path.basename(url))
        stem = os.path.splitext(filename)[0].removeprefix("vrutak-")
        parts = stem.split("-")

        if len(parts) < 4:
            raise ValueError(f"Invalid file name format for Vrutak: {filename}")

        # type-address-store_id-...
        store_id = parts[2]
        if store_id in self.STORES:
            store_type, street_address = self.STORES[store_id]
        else:
            logger.warning(f"Unknown Vrutak store {store_id} in {filename}")
            store_type, street_address = parts[0], parts[1].title()

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

    def read_csv(self, text: str, delimiter: str = ",") -> DictReader:
        reader = super().read_csv(text, delimiter=delimiter)
        if reader.fieldnames:
            reader.fieldnames = [name.replace(" ", "") for name in reader.fieldnames]
        return reader

    def parse_csv_row(self, row: dict) -> Product:
        # Since 2026-11-17 <mpcijenaakcija> holds the name and period of the
        # sale ("Akcija 09.-22.11.2026.") instead of a price. The regular price
        # is then not published: <mpcijena> is the price charged during the
        # sale, so it is also the special price.
        sale = (row.get("mpcijenaakcija") or "").strip()
        if not any(ch.isalpha() for ch in sale):
            return super().parse_csv_row(row)

        row["mpcijenaakcija"] = ""
        product = super().parse_csv_row(row)
        product.special_sale_type = sale
        product.special_price = product.price
        return product

    @staticmethod
    def decode_csv(content: bytes) -> str:
        """
        Decode a CSV price file, picking the encoding by its Croatian letters.

        The CSVs were windows-1250 until 2026-11-16 and are CP852 (DOS
        Latin-2) since 2026-11-17. A decode error can't tell them apart: CP852
        accepts every byte and windows-1250 all but five. The wrong one turns
        most Croatian letters into other non-ASCII characters ("NARAN¬A" for
        "NARANČA"), so the encoding with the most Croatian letters and the
        fewest other non-ASCII characters wins.
        """
        letters = set("čćžšđČĆŽŠĐ")

        def score(text: str) -> int:
            good = sum(ch in letters for ch in text)
            bad = sum(not ch.isascii() and ch not in letters for ch in text)
            return good - bad

        # windows-1250 leaves five bytes undefined, and in CP852 they are
        # letters (0x81 is "ü"), so decode with replacement: a replacement
        # character counts against that encoding.
        texts = [
            content.decode(encoding, errors="replace")
            for encoding in ("windows-1250", "cp852")
        ]
        return max(texts, key=score)

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

    def get_products(self, url: str) -> list[Product]:
        """
        Fetch and parse the products from a CSV or XML price file.

        Args:
            url: URL of the price file

        Returns:
            List of Product objects
        """
        if url.endswith(".csv"):
            # Latin-1 maps every byte to one character, so this round trip
            # returns the raw bytes for decode_csv to pick the encoding.
            content = self.fetch_text(url, ["latin-1"]).encode("latin-1")
            return self.parse_csv(self.decode_csv(content), delimiter=";")

        return self.parse_xml(self.fetch_text(url).encode("utf-8"))

    def get_index_urls_for_date(self, date: datetime.date) -> list[str]:
        """
        Fetch and parse the Vrutak index page to get price file URLs for a date.

        Args:
            date: The date to search for in the index.

        Returns:
            CSV URLs for the date if there are any, XML URLs otherwise.
        """
        content = self.fetch_text(self.INDEX_URL)

        if not content:
            logger.warning(f"No content found at Vrutak index URL: {self.INDEX_URL}")
            return []

        urls = self.parse_index(content).get(date, [])
        if not urls:
            logger.warning(f"No Vrutak URLs found matching date {date:%Y-%m-%d}")

        csv_urls = [url for url in urls if url.endswith(".csv")]
        return csv_urls or urls

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Main method to fetch and parse all Vrutak store, product, and price info for a given date.

        Args:
            date: The date to search for in the price list.

        Returns:
            List of Store objects with their products.
        """
        urls = self.get_index_urls_for_date(date)

        if not urls:
            logger.warning(f"No Vrutak price files found for date {date.isoformat()}")
            return []

        stores = []
        for url in urls:
            try:
                store = self.parse_store_info(url)
                store.items = self.get_products(url)
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
