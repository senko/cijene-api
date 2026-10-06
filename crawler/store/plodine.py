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

    Since 2026-10-02 the daily ZIP also carries the previous day's CSVs,
    including intra-day updates, so a store can have several files in one ZIP.
    Only files published on the requested date are used, latest one per store.
    """

    CHAIN = "plodine"
    BASE_URL = "https://www.plodine.hr"
    INDEX_URL = f"{BASE_URL}/info-o-cijenama"
    ZIP_DATE_PATTERN = re.compile(r".*/cjenici/cjenici_(\d{2})_(\d{2})_(\d{4})_.*\.zip")
    # Publication timestamp (ddmmyyyyHHMMSS) at the end of a CSV filename
    FILE_TIMESTAMP_PATTERN = re.compile(r"_(\d{14})\.csv$")
    VERIFY_TLS_CERT = False  # Plodine uses a root CA unsupported by httpx on Debian 12

    PRICE_MAP = {
        "price": (["Maloprodajna cijena", "MPC"], False),
        "unit_price": ("Cijena po JM", False),
        "special_price": (
            "MPC za vrijeme posebnog oblika prodaje",
            False,
        ),
        "best_price_30": ("Najniza cijena u poslj. 30 dana", False),
        "anchor_price": (["Sidrena cijena na 2.5.2025", "Sidrena cijena"], False),
    }

    # The new "Poseban oblik prodaje" column is a DA/NE flag, not a special price column.
    FIELD_MAP = {
        "product": ("Naziv proizvoda", True),
        "product_id": ("Sifra proizvoda", True),
        "brand": ("Marka proizvoda", False),
        "quantity": ("Neto kolicina", False),
        "unit": ("Jedinica mjere", False),
        "barcode": ("Barkod", False),
        "category": ("Kategorija proizvoda", False),
        "special_sale_type": ("Naziv posebnog oblika prodaje", False),
    }

    BOOL_MAP = {
        "available": ("Dostupno nedostupno", False),
    }

    REQUIRED_COLUMNS = [
        ["Maloprodajna cijena", "MPC"],
        "Cijena po JM",
        ["Sidrena cijena na 2.5.2025", "Sidrena cijena"],
        "Naziv proizvoda",
        "Sifra proizvoda",
        "Marka proizvoda",
        "Jedinica mjere",
        "Barkod",
    ]

    OPTIONAL_COLUMNS = [
        "Neto kolicina",
        "Kategorija proizvoda",
        "MPC za vrijeme posebnog oblika prodaje",
        "Najniza cijena u poslj. 30 dana",
        "Naziv posebnog oblika prodaje",
        "Dostupno nedostupno",
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

    def parse_file_timestamp(self, filename: str) -> Optional[datetime.datetime]:
        """
        Extract the publication timestamp from a CSV filename.

        Example: ..._VISKOVO_001_509_02102026035529.csv -> 2026-10-02 03:55:29

        Args:
            filename: Name of the CSV file

        Returns:
            Publication timestamp, or None if the filename doesn't carry one
        """
        match = self.FILE_TIMESTAMP_PATTERN.search(filename)
        if not match:
            return None
        try:
            return datetime.datetime.strptime(match.group(1), "%d%m%Y%H%M%S")
        except ValueError:
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
        # store_id -> (publication timestamp, store)
        stores: dict[str, tuple[datetime.datetime, Store]] = {}

        for filename, content in self.get_zip_contents(zip_url, ".csv"):
            logger.debug(f"Processing file: {filename}")

            published = self.parse_file_timestamp(filename)
            if published is None:
                logger.warning(f"Skipping CSV {filename}: no timestamp in filename")
                continue
            if published.date() != date:
                logger.debug(
                    f"Skipping CSV {filename}: published on {published:%Y-%m-%d}"
                )
                continue

            store = self.parse_store_from_filename(filename)
            if not store:
                logger.warning(f"Skipping CSV {filename} due to store parsing failure")
                continue

            existing = stores.get(store.store_id)
            if existing and existing[0] >= published:
                logger.debug(f"Skipping CSV {filename}: newer file for the same store")
                continue

            # Parse CSV and add products to the store
            try:
                products = self.parse_csv(content.decode("utf-8"), delimiter=";")
            except Exception as e:
                logger.error(f"Error processing CSV {filename}: {e}", exc_info=True)
                continue

            store.items = products
            stores[store.store_id] = (published, store)

        return [store for _, store in stores.values()]

    def fix_product_data(self, data: dict) -> dict:
        """Mirror the promotional price for the NN 101/2026 format."""
        data = super().fix_product_data(data)

        if data.get("special_sale_type") and data.get("special_price") is None:
            data["special_price"] = data.get("price")

        return data


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = PlodineCrawler()
    stores = crawler.get_all_products(datetime.date.today())
    print(stores[0])
    print(stores[0].items[0])
