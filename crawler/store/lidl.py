import datetime
import logging
from typing import Optional
import re


from .base import BaseCrawler
from crawler.store.models import Store, Product

logger = logging.getLogger(__name__)


class LidlCrawler(BaseCrawler):
    """
    Crawler for Lidl store prices.

    This class handles downloading and parsing price data from Lidl's website.
    It fetches the price list index page, finds the ZIP for the specified date,
    downloads and extracts it, and parses the CSV files inside.
    """

    CHAIN = "lidl"
    BASE_URL = "https://tvrtka.lidl.hr"
    INDEX_URL = f"{BASE_URL}/cijene"
    TIMEOUT = 180.0  # Longer timeout for ZIP download
    ZIP_DATE_PATTERN = re.compile(
        r".*/Popis_cijena_po_trgovinama_na_dan_(\d{1,2})_(\d{1,2})_(\d{4})\.zip"
    )

    ANCHOR_PRICE_COLUMN = "Sidrena_cijena_na_02.05.2025"
    PRICE_MAP = {
        "price": ("MALOPRODAJNA_CIJENA", False),
        "unit_price": ("CIJENA_ZA_JEDINICU_MJERE", False),
        "special_price": ("MPC_ZA_VRIJEME_POSEBNOG_OBLIKA_PRODAJE", False),
        "anchor_price": (ANCHOR_PRICE_COLUMN, False),
    }

    FIELD_MAP = {
        "product": ("NAZIV", False),
        "product_id": ("ŠIFRA", True),
        "brand": ("MARKA", False),
        "quantity": ("NETO_KOLIČINA", False),
        "unit": ("JEDINICA_MJERE", False),
        "barcode": ("BARKOD", False),
        "category": ("KATEGORIJA_PROIZVODA", False),
    }

    ADDRESS_PATTERN = re.compile(
        r"^(Supermarket)\s+"  # 'Supermarket'
        r"(\d+)_+"  # store number (digits)
        r"([\w._\s-]+?)_+"  # address (lazy match, allows spaces, underscores, dots)
        r"(\d{5})_+"  # ZIP code (5 digits)
        r"([A-ZŠĐČĆŽ_\s-]+?)_"  # city (letters, underscores or spaces, lazy match)
        r".*\.csv",  # the rest
        re.UNICODE | re.IGNORECASE,
    )

    def parse_store_from_filename(self, filename: str) -> Optional[Store]:
        """
        Extract store information from CSV filename using filename parts.

        Args:
            filename: Name of the CSV file with store information

        Returns:
            Store object with parsed store information, or None if parsing fails
        """
        logger.debug(f"Parsing store information from filename: {filename}")

        try:
            m = self.ADDRESS_PATTERN.match(filename)
            if not m:
                logger.warning(f"Filename doesn't match expected pattern: {filename}")
                return None

            store_type, store_id, address, zipcode, city = m.groups()
            city = city.replace("_", " ")
            address = address.replace("_", " ")
            if address.startswith(city + " "):
                address = address[len(city) + 1 :]
                if address.startswith("-"):
                    address = address[1:]

            store = Store(
                chain=self.CHAIN,
                store_id=store_id,
                name=f"Lidl {city}",
                store_type=store_type.lower(),
                city=city.title(),
                street_address=address.strip().title(),
                zipcode=zipcode,
                items=[],
            )

            logger.info(
                f"Parsed store: {store.name}, {store.store_type}, {store.city}, {store.street_address}, {store.zipcode}"
            )
            return store

        except Exception as e:
            logger.error(f"Failed to parse store from filename {filename}: {str(e)}")
            return None

    def parse_csv_row(self, row: dict) -> Product:
        anchor_price = row.get(self.ANCHOR_PRICE_COLUMN, "").strip()
        if "Nije_bilo_u_prodaji" in anchor_price:
            row[self.ANCHOR_PRICE_COLUMN] = None

        return super().parse_csv_row(row)

    def get_index(self, date: datetime.date) -> str:
        content = self.fetch_text(self.INDEX_URL)
        zip_urls_by_date = self.parse_index_for_zip(content)
        others = ", ".join(f"{d:%Y-%m-%d}" for d in zip_urls_by_date)
        logger.debug(f"Available price lists: {others}")
        if date not in zip_urls_by_date:
            raise ValueError(f"No price list found for {date}")
        return zip_urls_by_date[date]

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Main method to fetch and parse all products from Lidl's price lists.

        Args:
            date: The date for which to fetch the price list

        Returns:
            Tuple with the date and the list of Store objects,
            each containing its products.

        Raises:
            ValueError: If the price list ZIP cannot be found or processed
        """
        zip_url = self.get_index(date)
        stores = []

        for filename, content in self.get_zip_contents(zip_url, ".csv"):
            logger.debug(f"Processing file: {filename}")
            store = self.parse_store_from_filename(filename)
            if not store:
                logger.warning(f"Skipping CSV {filename} due to store parsing failure")
                continue

            # Parse CSV and add products to the store
            text = content.decode("windows-1250")
            headers = text.splitlines()[0]
            if "\t" in headers:
                delimiter = "\t"
            elif ";" in headers:
                delimiter = ";"
            elif "," in headers:
                delimiter = ","
            else:
                logger.warning(f"Unknown delimiter in CSV: {filename}; ignoring")
                continue
            products = self.parse_csv(text, delimiter=delimiter)
            store.items = products
            stores.append(store)

        return stores


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = LidlCrawler()
    stores = crawler.get_all_products(datetime.date.today())
    print(stores[0])
    print(stores[0].items[0])
