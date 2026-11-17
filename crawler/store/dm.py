import datetime
import json
import logging
import re
import warnings
from collections.abc import Iterable
from io import BytesIO
from tempfile import TemporaryFile
from typing import Any, List

import openpyxl

from crawler.store.models import Product, Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)


class DmCrawler(BaseCrawler):
    """
    Crawler for DM (DrogerieMarkt) store prices.

    Until 2026-11-16 dm published one Excel file per day. Since 2026-11-17 it
    publishes a semicolon-separated CSV instead, with a single price column,
    the name of the special form of sale, and the anchor price as
    "bazna cijena". Both formats are read into the same normalized row.
    """

    CHAIN = "dm"
    BASE_URL = "https://www.dm.hr"
    CONTENT_BASE_URL = "https://content.services.dmtech.com/rootpage-dm-shop-hr-hr"
    INDEX_URL = f"{CONTENT_BASE_URL}/novo/promocije/nove-oznake-cijena-i-vazeci-cjenik-u-dm-u-2906632?mrclx=false"

    # DM has global prices, not per-store prices
    STORE_ID = "all"
    STORE_NAME = "DM"

    # Not used — parse_rows reads columns by their normalized names, so the
    # shared map-driven parser never runs here.
    PRICE_MAP = {}
    FIELD_MAP = {}

    # Column names as normalized by normalize_column. Where a tuple is given,
    # the first name is the Excel one and the second the CSV one.
    PRICE_COLUMN = ("mpc", "maloprodajna cijena")
    SPECIAL_PRICE_COLUMN = "mpc za vrijeme posebnog oblika prodaje (rasprodaja proizvoda koji izlaze iz asortimana)"
    BEST_PRICE_30_COLUMN = "najniza cijena u posljednjih 30 dana prije rasprodaje"
    ANCHOR_PRICE_COLUMN = "sidrena cijena na 2.5.2025. ili na datum ulistanja"
    NEW_ANCHOR_PRICE_COLUMN = "bazna cijena"
    SALE_TYPE_COLUMN = "poseban oblik prodaje"

    # The Excel header says "on 2.5.2025 or on the listing date", but the file
    # doesn't give listing dates, so 2.5.2025 is used for all products. The
    # CSV header gives no date, so CSV rows get none.
    ANCHOR_PRICE_DATE = datetime.date(2025, 5, 2)

    REQUIRED_COLUMNS = [
        "naziv",
        "sifra",
        "marka",
        "barkod",
        "jedinica mjere",
        "cijena za jedinicu mjere",
        PRICE_COLUMN,
        (ANCHOR_PRICE_COLUMN, NEW_ANCHOR_PRICE_COLUMN),
    ]

    # The first four aren't required by NN 101/2026, and the CSV doesn't have
    # them. The sale name was added with the CSV; the Excel files don't have it.
    OPTIONAL_COLUMNS = [
        "kategorija proizvoda",
        "neto kolicina",
        SPECIAL_PRICE_COLUMN,
        BEST_PRICE_30_COLUMN,
        SALE_TYPE_COLUMN,
    ]

    def parse_date_from_title(self, title: str) -> datetime.date:
        """
        Extract the date from the title of a price list link.

        Titles seen:
            "Važeći cjenik dm_trgovine_dm_online_shop_551_16.11.2026_05.04.45"
            "94124811986_ZA_SVA_PRODAJNA_MJESTA_ROBA_1_20261117_1034" (since
            2026-11-17: OIB, ..., date as YYYYMMDD, time as HHMM)

        Args:
            title: Title attribute of the link

        Returns:
            Extracted date object
        """
        # Match date in format DD.MM.YYYY where D or M can be single-digit
        date_match = re.search(r"(\d{1,2})\.(\d{1,2})\.(\d{4})", title)
        if date_match:
            day, month, year = map(int, date_match.groups())
            return datetime.date(year, month, day)

        # YYYYMMDD followed by _HHMM; the lookbehind keeps it out of the OIB
        date_match = re.search(r"(?<!\d)(\d{4})(\d{2})(\d{2})_\d{4}(?!\d)", title)
        if date_match:
            year, month, day = map(int, date_match.groups())
            return datetime.date(year, month, day)

        raise ValueError(f"Could not extract date from title: {title}")

    def find_price_list_url(self, json_content: str, target_date: datetime.date) -> str:
        """
        Parse the JSON data to find the price list URL for the target date.

        Args:
            json_content: JSON content from the index page
            target_date: The date to search for

        Returns:
            URL of the price list (Excel until 2026-11-16, CSV since)

        Raises:
            ValueError: If no price list is found for the target date
        """
        try:
            # Parse JSON data
            data = json.loads(json_content)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {e}")
            raise ValueError("Failed to parse JSON data from the page")

        # Find all CMDownload entries in mainData
        excel_entries = []
        for item in data.get("mainData", []):
            if item.get("type") == "CMDownload":
                excel_entries.append(item.get("data", {}))

        if not excel_entries:
            logger.warning("No Excel links found in JSON data")
            raise ValueError("No Excel links found in JSON data")

        target_date_str = f"{target_date.day}.{target_date.month}.{target_date.year}"
        logger.info(f"Looking for price list with date {target_date_str}")

        for entry in excel_entries:
            headline = entry.get("headline", "")
            link_target = entry.get("linkTarget", "")

            if not headline or not link_target:
                continue

            try:
                link_date = self.parse_date_from_title(headline)
                if link_date == target_date:
                    # Ensure URL is absolute
                    if not link_target.startswith(("http://", "https://")):
                        url = f"{self.CONTENT_BASE_URL}{link_target}"
                    else:
                        url = link_target
                    logger.info(f"Found price list with date {link_date}: {url}")
                    return url
            except Exception as e:
                logger.warning(f"Error parsing date from headline '{headline}': {e}")
                continue

        raise ValueError(f"No price list found for date {target_date_str}")

    def detect_columns(self, worksheet: Any) -> list[str]:
        """
        Detect the column ordering in the DM Excel worksheet.

        This relies on the fact that one of the columns in the header will
        always be "naziv + šifra", which is a merged cell that actually
        has two cells in the data, naziv and product ID.

        Args:
            worksheet: The active worksheet object

        Returns:
            List of column headers
        """
        for row in worksheet.iter_rows():
            row_str = [self.normalize_column(cell.value) for cell in row]
            if "naziv + sifra" in row_str:
                idx = row_str.index("naziv + sifra")
                if row_str[idx + 1] != "":
                    raise ValueError(
                        "Expected 'naziv + šifra' to be a merged cell with two parts"
                    )
                row_str[idx] = "naziv"
                row_str[idx + 1] = "sifra"
                return row_str

        raise ValueError(
            "Could not detect Excel columns, DM file format may have changed"
        )

    def normalize_column(self, name: Any) -> str:
        """
        Normalize a column name: lowercase, no diacritics, single spaces.
        """
        return " ".join(self.strip_diacritics(str(name or "").lower()).split())

    @staticmethod
    def map_columns(row: Any, columns: list) -> dict[str, Any]:
        """
        Map the row data to a dictionary using the detected columns.

        Args:
            row: The row object from the worksheet
            columns: List of column headers

        Returns:
            Dictionary mapping column names to cell values
        """
        return {col: str(row[i].value or "").strip() for i, col in enumerate(columns)}

    def parse_excel(self, excel_data: bytes) -> List[Product]:
        """
        Parse Excel file data into Product objects.

        Args:
            excel_data: Raw Excel file content

        Returns:
            List of Product objects
        """
        logger.debug("Parsing Excel file")
        products = []

        try:
            with warnings.catch_warnings(record=False):
                warnings.simplefilter("ignore", UserWarning)
                workbook = openpyxl.load_workbook(BytesIO(excel_data), data_only=True)
            worksheet = workbook.active  # Get the active worksheet

            if not worksheet:
                raise ValueError("No active worksheet found in the Excel file")

            columns = self.detect_columns(worksheet)
            logger.debug(f"Detected columns: {columns}")
            self.check_columns(columns)

            # Skip header and empty rows
            row_maps = (
                self.map_columns(row, columns)
                for row in worksheet.iter_rows()
                if len(row) == len(columns)
            )
            products = self.parse_rows(row_maps)

        except Exception as e:
            logger.error(f"Error parsing Excel file: {e}", exc_info=True)
            raise

        logger.debug(f"Parsed {len(products)} products from Excel file")
        return products

    def parse_csv_file(self, content: str) -> List[Product]:
        """
        Parse the CSV price list published since 2026-11-17.

        Args:
            content: CSV text, semicolon-separated

        Returns:
            List of Product objects
        """
        reader = self.read_csv(content, delimiter=";")
        names = list(reader.fieldnames or [])
        columns = [self.normalize_column(name) for name in names]
        self.check_columns(columns)

        row_maps = (
            {
                column: (row.get(name) or "").strip()
                for column, name in zip(columns, names)
            }
            for row in reader
        )
        products = self.parse_rows(row_maps)

        logger.debug(f"Parsed {len(products)} products from CSV file")
        return products

    def parse_rows(self, row_maps: Iterable[dict[str, str]]) -> List[Product]:
        """
        Turn normalized rows from either format into Product objects.

        Args:
            row_maps: Rows keyed by normalized column name

        Returns:
            List of Product objects; rows that fail to parse are logged and skipped
        """
        products = []

        for row_idx, row_map in enumerate(row_maps, start=1):
            if not row_map.get("sifra"):
                continue

            try:
                price_column = next(c for c in self.PRICE_COLUMN if c in row_map)
                price = self.parse_price(row_map[price_column], False)
                special_price = self.parse_price(
                    row_map.get(self.SPECIAL_PRICE_COLUMN), False
                )

                # The CSV has one price column, holding the sale price on rows
                # that name a special form of sale. Checked against the last
                # Excel file (2026-11-16): on every such row the CSV price
                # equals the previous special price.
                sale_type = row_map.get(self.SALE_TYPE_COLUMN) or None
                if sale_type and special_price is None:
                    special_price = price

                new_anchor = self.NEW_ANCHOR_PRICE_COLUMN in row_map
                anchor_column = (
                    self.NEW_ANCHOR_PRICE_COLUMN
                    if new_anchor
                    else self.ANCHOR_PRICE_COLUMN
                )

                product_data = {
                    "product": row_map["naziv"],
                    "product_id": row_map["sifra"],
                    "brand": row_map["marka"],
                    "barcode": row_map["barkod"],
                    "category": row_map.get("kategorija proizvoda", ""),
                    # The CSV writes "0,2" and "kg" where the Excel file had
                    # 0.2 and "KG"; keep the Excel form so products don't change.
                    "quantity": row_map.get("neto kolicina", "").replace(",", "."),
                    "unit": row_map["jedinica mjere"].upper(),
                    "unit_price": self.parse_price(
                        row_map["cijena za jedinicu mjere"], False
                    ),
                    "price": price,
                    "special_price": special_price,
                    "special_sale_type": sale_type,
                    "best_price_30": self.parse_price(
                        row_map.get(self.BEST_PRICE_30_COLUMN), False
                    ),
                    "anchor_price": self.parse_price(row_map[anchor_column], False),
                }

                # Apply common fixups from base class
                product_data = self.fix_product_data(product_data)

                # The CSV anchor header states no date, so ANCHOR_PRICE_DATE
                # doesn't apply to it.
                if new_anchor:
                    product_data["anchor_price_date"] = None

                products.append(Product(**product_data))
            except Exception as e:
                row_txt = "; ".join(str(v) for v in row_map.values())
                logger.warning(f"Failed to parse row {row_idx}: `{row_txt}`: {e}")
                continue

        return products

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Main method to fetch and parse all product and price info.

        Args:
            date: The date to search for in the price list.

        Returns:
            List with a single Store object containing all products.

        Raises:
            ValueError: If no price list is found for the given date.
        """
        content = self.fetch_text(self.INDEX_URL)
        if not content:
            logger.warning(f"No content found at {self.INDEX_URL}")
            return []

        # Find the price list URL for the exact target date from JSON
        url = self.find_price_list_url(content, date)

        with TemporaryFile(mode="w+b") as temp_file:
            self.fetch_binary(url, temp_file)
            temp_file.seek(0)
            data = temp_file.read()

        if url.lower().endswith(".csv"):
            products = self.parse_csv_file(data.decode("utf-8-sig"))
        else:
            products = self.parse_excel(data)

        if not products:
            logger.warning(f"No products found for date {date}")
            return []

        # Create a global store
        store = Store(
            chain=self.CHAIN,
            store_type="store",
            store_id=self.STORE_ID,
            name=self.STORE_NAME,
            street_address="",
            zipcode="",
            city="",
            items=products,
        )

        return [store]


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = DmCrawler()
    stores = crawler.crawl(datetime.date.today())
    print(stores[0])
    print(stores[0].items[0])
