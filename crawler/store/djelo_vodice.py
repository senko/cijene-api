import datetime
import logging
import re
from io import BytesIO
from tempfile import TemporaryFile
from urllib.parse import unquote

import openpyxl
from bs4 import BeautifulSoup

from crawler.store.models import Product, Store

from .base import BaseCrawler, CrawlerBlocked

logger = logging.getLogger(__name__)


def cell_str(val):
    """Convert a cell value to string, treating None as empty but preserving 0/0.0."""
    return "" if val is None else str(val).strip()


# Decoded file name: "#"-separated store type, street address, place, store id,
# file id and timestamp. The place is "ZIP CITY" since 2026-11-14 and only the
# city before that. Older names lost their diacritics to "?".
#   PRODAVAONICA#ČIKOLE 2#22320 DRNIŠ#02044#26318#2026-11-14T010557.xlsx
#   SUPERMARKET#PUT GA?ELEZA 5A#VODICE#02010#26317#2026-11-13T010506.xlsx
FILENAME_PATTERN = re.compile(
    r"^(?P<store_type>[^#]*)#(?P<address>[^#]*)#(?P<place>[^#]*)#"
    r"(?P<store_id>\d+)#\d+#(?P<date>\d{4}-\d{2}-\d{2})T(?P<time>\d{6})\.xlsx$"
)
PLACE_PATTERN = re.compile(r"^(?P<zipcode>\d{5})\s+(?P<city>.+)$")

# Store metadata keyed by store_id (from XLSX column A). Overrides what the file
# name says, for stores whose name lacks the address or spells it wrong.
STORES = {
    "02010": {
        "name": "Dućan 10 Vodice",
        "store_type": "supermarket",
        "city": "Vodice",
        "street_address": "Put Gaćeleza 5A",
        "zipcode": "22211",
    },
    # File name: "PUT BILOSTANA 1", no place.
    "02066": {
        "city": "Krković",
        "street_address": "Put Bilostanova 1",
        "zipcode": "22214",
    },
}

# Points of sale that publish a price list but are not shops. Their file names
# carry no address.
SKIPPED_STORES = {
    "05075": "Restoran Vodice",
    "08001": "Frizerski salon Vodice",
}


class DjeloVodiceCrawler(BaseCrawler):
    """
    Crawler for Djelo Vodice store prices.

    Djelo Vodice publishes one XLSX pricelist per store per day as a directory
    listing, and the store's address is in the file name. Until 2026-11-13 the
    listing had only the Vodice supermarket (02010). From 2026-11-14 it has
    about 38 points of sale, mostly shops around Šibenik. Each file is read by
    the column names in the header row.
    """

    CHAIN = "djelo_vodice"
    BASE_URL = "https://dv10.djelo-vodice.hr"

    # Not used — parse_excel reads the XLSX itself, so the shared map-driven
    # parser never runs here.
    PRICE_MAP = {}
    FIELD_MAP = {}

    REQUIRED_COLUMNS = [
        "Šifra prodajnog mjesta",
        "Naziv prodajnog mjesta",
        "Šifra artikla",
        "Naziv artikla",
        "Sidrena cijena",
        "Barkod",
        "MP cijena",
        "Cijena za jedinicu mj.",
        "Obr. jedinica mjere",
    ]

    # "Raspoloživo" (availability) appeared on 2026-11-14. "Kategorija" was
    # dropped the same day; it is still read from older files, but not listed
    # here, because it would warn on every file. "Akcija" (Da/Ne) is a promo
    # flag without the name of the form of sale, so it is not read.
    OPTIONAL_COLUMNS = [
        "Raspoloživo",
    ]

    def get_index(self, date: datetime.date) -> list[tuple[str, dict[str, str]]]:
        """
        Find the XLSX file of each store for the given date.

        If a store has more than one file for the date, the latest one wins.

        Args:
            date: Target date to find the pricelists for.

        Returns:
            (URL, store info) for each store, where the store info holds the
            store type, street address, city and zip code from the file name.

        Raises:
            ValueError: If no file is found for the target date.
        """
        content = self.fetch_text(f"{self.BASE_URL}/")
        soup = BeautifulSoup(content, "html.parser")

        target = date.isoformat()
        latest: dict[str, tuple[str, str, dict[str, str]]] = {}

        for link in soup.select('a[href$=".xlsx"]'):
            href = link.get("href")
            if not href:
                continue

            m = FILENAME_PATTERN.match(unquote(str(href)))
            if not m or m.group("date") != target:
                continue

            store_id = m.group("store_id")
            if store_id in SKIPPED_STORES:
                continue

            place = m.group("place").strip()
            pm = PLACE_PATTERN.match(place)
            info = {
                "store_type": m.group("store_type").strip().lower(),
                "street_address": m.group("address").strip(),
                "city": pm.group("city") if pm else place,
                "zipcode": pm.group("zipcode") if pm else "",
            }

            time = m.group("time")
            if store_id not in latest or time > latest[store_id][0]:
                latest[store_id] = (time, f"{self.BASE_URL}/{href}", info)

        if not latest:
            raise ValueError(f"No XLSX file found for date {date}")

        return [(url, info) for _, url, info in latest.values()]

    def parse_excel(
        self, excel_data: bytes, store_info: dict[str, str] | None = None
    ) -> tuple[Store, list[Product]]:
        """
        Parse an XLSX file into a Store object and list of Product objects.

        The XLSX has a single sheet "Cijene na dan" with a header row. Besides
        the columns in REQUIRED_COLUMNS and OPTIONAL_COLUMNS it has
        "Jedinica mjere" (KOMAD etc.) and "Akcija", which are not used.

        Args:
            excel_data: Raw XLSX file content as bytes.
            store_info: Store type, address, city and zip code from the file
                name. Entries in STORES take precedence.

        Returns:
            Tuple of (Store, list of Products).

        Raises:
            ValueError: If a required column is missing.
        """
        workbook = openpyxl.load_workbook(BytesIO(excel_data), data_only=True)
        worksheet = workbook.active

        if not worksheet:
            raise ValueError("No active worksheet found in the Excel file")

        rows = worksheet.iter_rows(values_only=True)
        header = [cell_str(name) for name in next(rows, ())]
        self.check_columns(header)
        index = {name.lower(): i for i, name in enumerate(header) if name}

        def cell(row: tuple, column: str) -> str:
            i = index.get(column.lower())
            return cell_str(row[i]) if i is not None and i < len(row) else ""

        products = []
        store = None

        for row_idx, row in enumerate(rows, start=2):
            store_id = cell(row, "Šifra prodajnog mjesta")
            store_name = cell(row, "Naziv prodajnog mjesta")

            if not store_id:
                continue

            # Build store from first data row
            if store is None:
                info = {**(store_info or {}), **STORES.get(store_id, {})}
                store = Store(
                    chain=self.CHAIN,
                    store_id=store_id,
                    name=info.get("name", store_name),
                    store_type=info.get("store_type", ""),
                    city=info.get("city", ""),
                    street_address=info.get("street_address", ""),
                    zipcode=info.get("zipcode", ""),
                )
                if not store.city:
                    logger.warning(
                        f"No city for store_id '{store_id}' ({store_name}), "
                        f"add it to the STORES map in djelo_vodice.py"
                    )

            try:
                data = {
                    "product_id": cell(row, "Šifra artikla"),
                    "product": cell(row, "Naziv artikla"),
                    "anchor_price": self.parse_price(
                        cell(row, "Sidrena cijena"), False
                    ),
                    "barcode": cell(row, "Barkod"),
                    "category": cell(row, "Kategorija"),
                    "price": self.parse_price(cell(row, "MP cijena"), True),
                    "unit_price": self.parse_price(
                        cell(row, "Cijena za jedinicu mj."), True
                    ),
                    "unit": cell(row, "Obr. jedinica mjere"),
                    "available": self.parse_bool(cell(row, "Raspoloživo")),
                    "brand": "",
                    "quantity": "",
                }
                data = self.fix_product_data(data)
                products.append(Product(**data))
            except Exception as e:
                row_txt = "; ".join("" if v is None else str(v) for v in row)
                logger.warning(f"Failed to parse row {row_idx}: `{row_txt}`: {e}")
                continue

        if store is None:
            raise ValueError("No data rows found in the XLSX file")

        logger.debug(f"Parsed {len(products)} products from Excel file")
        return store, products

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Fetch and parse all products for the given date.

        Args:
            date: The date to fetch the pricelists for.

        Returns:
            List of stores with their products.
        """
        files = self.get_index(date)
        logger.info(f"Found {len(files)} XLSX files for {date}")
        stores = []

        for xlsx_url, store_info in files:
            try:
                with TemporaryFile(mode="w+b") as temp_file:
                    self.fetch_binary(xlsx_url, temp_file)
                    temp_file.seek(0)
                    excel_data = temp_file.read()

                store, products = self.parse_excel(excel_data, store_info)
            except CrawlerBlocked:
                raise
            except Exception as e:
                logger.error(f"Error processing {xlsx_url}: {e}", exc_info=True)
                continue

            if not products:
                logger.warning(f"No products found in {xlsx_url}")
                continue

            store.items = products
            stores.append(store)

        return stores


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = DjeloVodiceCrawler()
    stores = crawler.crawl(datetime.date.today())
    print(stores[0])
    print(stores[0].items[0])
