import datetime
import logging
import re
from typing import Optional


from .base import BaseCrawler
from crawler.store.models import Store

logger = logging.getLogger(__name__)


class PlodineCrawler(BaseCrawler):
    """
    Crawler for Plodine store prices.

    This class handles downloading and parsing price data from Plodine's website.
    It fetches the price list index page, finds the ZIP for the specified date,
    downloads and extracts it, and parses the CSV files inside.
    """

    CHAIN = "plodine"
    BASE_URL = "https://www.plodine.hr"
    INDEX_URL = f"{BASE_URL}/info-o-cijenama"
    ZIP_DATE_PATTERN = re.compile(r".*/cjenici/cjenici_(\d{2})_(\d{2})_(\d{4})_.*\.zip")
    VERIFY_TLS_CERT = False  # Plodine uses a root CA unsupported by httpx on Debian 12

    PRICE_MAP = {
        "price": ("Maloprodajna cijena", False),
        "unit_price": ("Cijena po JM", False),
        "special_price": (
            "MPC za vrijeme posebnog oblika prodaje",
            False,
        ),
        "best_price_30": ("Najniza cijena u poslj. 30 dana", False),
        "anchor_price": ("Sidrena cijena na 2.5.2025", False),
    }

    FIELD_MAP = {
        "product": ("Naziv proizvoda", True),
        "product_id": ("Sifra proizvoda", True),
        "brand": ("Marka proizvoda", False),
        "quantity": ("Neto kolicina", False),
        "unit": ("Jedinica mjere", False),
        "barcode": ("Barkod", False),
        "category": ("Kategorija proizvoda", False),
    }

    REQUIRED_COLUMNS = [
        "Maloprodajna cijena",
        "Cijena po JM",
        "MPC za vrijeme posebnog oblika prodaje",
        "Najniza cijena u poslj. 30 dana",
        "Sidrena cijena na 2.5.2025",
        "Naziv proizvoda",
        "Sifra proizvoda",
        "Marka proizvoda",
        "Neto kolicina",
        "Jedinica mjere",
        "Barkod",
        "Kategorija proizvoda",
    ]

    def get_index(self, date: datetime.date) -> str:
        content = self.fetch_text(self.INDEX_URL)
        zip_urls_by_date = self.parse_index_for_zip(content)
        others = ", ".join(f"{d:%Y-%m-%d}" for d in zip_urls_by_date)
        logger.debug(f"Available price lists: {others}")
        if date not in zip_urls_by_date:
            raise ValueError(f"No price list found for {date}")
        return zip_urls_by_date[date]

    def parse_store_from_filename(self, filename: str) -> Optional[Store]:
        """
        Extract store information from CSV filename using regex.

        Example filename format:
            SUPERMARKET_SJEVERNA_VEZNA_CESTA_31_35000_SLAVONSKI_BROD_022_6_20052025014212.csv
            SUPERMARKET_ULICA_FRANJE_TUDJMANA_83A_10450_JASTREBARSKO_063_2_16052025020937.csv

        Args:
            filename: Name of the CSV file with store information

        Returns:
            Store object with parsed store information, or None if parsing fails
        """
        logger.debug(f"Parsing store information from filename: {filename}")

        try:
            pattern = (
                r"^(SUPERMARKET|HIPERMARKET)_(.+?)_(\d{5})_(.+)_(\d+)_\d+_\d+.*\.csv$"
            )
            match = re.match(pattern, filename)

            if not match:
                logger.warning(f"Failed to match filename pattern: {filename}")
                return None

            store_type, street_address, zipcode, city, store_id = match.groups()

            city = city.replace("_", " ").title()

            store = Store(
                chain="plodine",
                store_id=store_id,
                name=f"Plodine {city}",
                store_type=store_type.lower(),
                city=city,
                street_address=street_address.replace("_", " ").title(),
                zipcode=zipcode,
                items=[],
            )

            logger.info(
                f"Parsed store: {store.name} ({store.store_id}), {store.store_type}, {store.city}, {store.street_address}, {store.zipcode}"
            )
            return store

        except Exception as e:
            logger.error(f"Failed to parse store from filename {filename}: {str(e)}")
            return None

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Main method to fetch and parse all products from Plodine's price lists.

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
            products = self.parse_csv(content.decode("utf-8"), delimiter=";")
            store.items = products
            stores.append(store)

        return stores


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = PlodineCrawler()
    stores = crawler.get_all_products(datetime.date.today())
    print(stores[0])
    print(stores[0].items[0])
