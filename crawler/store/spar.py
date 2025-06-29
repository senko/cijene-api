import datetime
import logging
import re
from typing import Optional
from json import loads


from crawler.store.models import Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)


class SparCrawler(BaseCrawler):
    """
    Crawler for Spar/InterSpar store prices.

    This class handles downloading and parsing price data from Spar's website.
    It fetches the JSON index file, extracts CSV links, downloads and parses
    the CSVs, and returns a list of products.
    """

    CHAIN = "spar"
    BASE_URL = "https://www.spar.hr"
    ADDRESS_PATTERN = re.compile(
        r"^([a-zA-Z]+)_([a-zA-Z0-9_\.]+)_(\d{4,5})_([a-zA-Z_]+)_"
    )
    CITIES = [
        "varazdin",
        "valpovo",
        "sibenik",
        "zadar",
        "zagreb",
        "cakovec",
        "rijeka",
        "split",
        "kastav",
        "selce",
        "bibinje",
        "labin",
        "buje",
        "krizevci",
        "pozega",
        "jastrebarsko",
        "sesvetski_kraljevec",
        "krapinske_toplice",
        "novi_marof",
        "ivanic_grad",
        "vukovar",
        "marija_bistrica",
        "zapresic",
        "velika_gorica",
        "slavonski_brod",
        "osijek",
        "koprivnica",
        "bjelovar",
        "vinkovci",
        "dakovo",
        "orahovica",
        "pakrac",
        "suhopolje",
        "daruvar",
        "nasice",
        "pula",
        "opatija",
        "porec",
        "knin",
        "zlatar",
        "ivanec",
        "popovaca",
        "nin",
        "donja_stubica",
        "pregrada",
        "cepin",
        "ozalj",
        "dugo_selo",
        "gospic",
    ]
    PRICE_MAP = {
        "price": ("MPC (EUR)", False),
        "unit_price": ("cijena za jedinicu mjere (EUR)", False),
        "special_price": ("MPC za vrijeme posebnog oblika prodaje (EUR)", False),
        "best_price_30": ("Najniža cijena u posljednjih 30 dana (EUR)", False),
        "anchor_price": ("sidrena cijena na 2.5.2025. (EUR)", False),
    }

    FIELD_MAP = {
        "barcode": ("barkod", False),
        "product": ("naziv", True),
        "product_id": ("šifra", True),
        "brand": ("marka", False),
        "quantity": ("neto količina", False),
        "unit": ("jedinica mjere", False),
        "category": ("kategorija proizvoda", False),
    }

    # Required to detect text encoding
    CSV_PREFIX = "naziv;šifra;marka;neto količina;jedinica mjere;"

    def fetch_price_list_index(self, date: datetime.date) -> dict[str, str]:
        """
        Fetch the JSON index file with list of CSV files.

        Args:
            date: The date for which to fetch the price list index

        Returns:
            A dictionary with filename → URL mappings for CSV files.

        Raises:
            httpx.RequestError: If the request fails
        """
        url = f"{self.BASE_URL}/datoteke_cjenici/Cjenik{date:%Y%m%d}.json"
        content = self.fetch_text(url)

        json_data = loads(content)
        files = json_data.get("files")
        if not files:
            logger.error("Price list index doesn't contain any files")
            return {}

        return {info.get("name", ""): info.get("URL", "") for info in files}

    def parse_store_from_filename(self, filename: str) -> Optional[Store]:
        """
        Extract store information from CSV filename using regex.

        Supported filename pattern:
            `hipermarket_zadar_bleiburskih_zrtava_18_8701_interspar_zadar_0017_20250518_0330.csv`

        Args:
            filename: Name of the CSV file with store information

        Returns:
            Store object with parsed store information, or None if parsing fails
        """
        logger.debug(f"Parsing store information from filename: {filename}")

        match = self.ADDRESS_PATTERN.match(filename)

        if not match:
            logger.warning(f"Failed to match filename pattern: {filename}")
            return None

        store_type, city_and_address, store_id, store_name = match.groups()

        for city in self.CITIES:
            if city_and_address.lower().startswith(city):
                store_city = city
                store_address = city_and_address[len(city) + 1 :]
                break
        else:
            # Assume city is the first word
            store_city, store_address = city_and_address.split("_", 1)

        store = Store(
            chain="spar",
            store_id=store_id,
            name=store_name.replace("_", " ").title(),
            store_type=store_type.lower(),
            city=store_city.replace("_", " ").title(),
            street_address=store_address.replace("_", " ").title(),
            items=[],
        )

        logger.debug(
            f"Parsed store: {store.name} ({store.store_id}), {store.store_type}, {store.city}, {store.street_address}"
        )
        return store

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Main method to fetch and parse all products from Spar's price lists.

        Args:
            date: The date for which to fetch the price list

        Returns:
            Tuple with the date and the list of Store objects,
            each containing its products.

        Raises:
            ValueError: If the price list index cannot be fetched or parsed
        """
        # Fetch the price list index
        csv_files = self.fetch_price_list_index(date)

        logger.info(f"Found {len(csv_files)} CSV files in the price list index")

        stores = []

        for filename, url in csv_files.items():
            store = self.parse_store_from_filename(filename)
            if not store:
                logger.warning(f"Skipping CSV from {url} due to store parsing failure")
                continue

            csv_content = self.fetch_text(
                url, ["iso-8859-2", "windows-1250"], self.CSV_PREFIX
            )
            if not csv_content:
                logger.warning(f"Skipping CSV from {url} due to download failure")
                continue

            try:
                products = self.parse_csv(csv_content, ";")
                store.items = products
                stores.append(store)
            except Exception as e:
                logger.error(f"Error processing CSV from {url}: {e}", exc_info=True)
                continue

        return stores


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = SparCrawler()
    stores = crawler.get_all_products(datetime.date.today())
    print(stores[0])
    print(stores[0].items[0])
