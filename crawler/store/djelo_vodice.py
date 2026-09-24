import datetime
import logging
import re
from io import BytesIO
from tempfile import TemporaryFile
from urllib.parse import unquote

import openpyxl
from bs4 import BeautifulSoup

from crawler.store.models import Product, Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)


def cell_str(val):
    """Convert a cell value to string, treating None as empty but preserving 0/0.0."""
    return "" if val is None else str(val).strip()


# Regex to extract date from the decoded filename timestamp portion
# Example: ...#2025-05-19T010506.xlsx -> 2025-05-19
DATE_PATTERN = re.compile(r"(\d{4}-\d{2}-\d{2})T\d{6}\.xlsx$")

# Hardcoded store metadata keyed by store_id (from XLSX column A)
STORES = {
    "02010": {
        "name": "Dućan 10 Vodice",
        "store_type": "supermarket",
        "city": "Vodice",
        "street_address": "Put Gaćeleza 5A",
        "zipcode": "22211",
    },
}


class DjeloVodiceCrawler(BaseCrawler):
    """
    Crawler for Djelo Vodice store prices.

    Djelo Vodice is a single-store supermarket in Vodice that publishes daily
    XLSX pricelists as a directory listing. Each file contains ~7,500 products
    with fixed columns parsed by position.
    """

    CHAIN = "djelo_vodice"
    BASE_URL = "https://dv10.djelo-vodice.hr"

    # Not used — XLSX is parsed by column position, not by CSV column names
    PRICE_MAP = {}
    FIELD_MAP = {}
    REQUIRED_COLUMNS = []

    def get_index(self, date: datetime.date) -> str:
        """
        Find the XLSX file URL for the given date from the directory listing.

        Args:
            date: Target date to find the pricelist for.

        Returns:
            Full URL of the XLSX file for the given date.

        Raises:
            ValueError: If no file is found for the target date.
        """
        content = self.fetch_text(f"{self.BASE_URL}/")
        soup = BeautifulSoup(content, "html.parser")

        target = date.isoformat()

        for link in soup.select('a[href$=".xlsx"]'):
            href = link.get("href")
            if not href:
                continue

            decoded = unquote(str(href))
            m = DATE_PATTERN.search(decoded)
            if m and m.group(1) == target:
                return f"{self.BASE_URL}/{href}"

        raise ValueError(f"No XLSX file found for date {date}")

    def parse_excel(self, excel_data: bytes) -> tuple[Store, list[Product]]:
        """
        Parse an XLSX file into a Store object and list of Product objects.

        The XLSX has a single sheet "Cijene na dan" with columns:
        A=store_id, B=store_name, C=product_id, D=product_name,
        E=anchor_price, F=barcode, G=unit_type, H=category,
        I=price, J=unit_price, K=unit, L=net_quantity (always 0).

        Args:
            excel_data: Raw XLSX file content as bytes.

        Returns:
            Tuple of (Store, list of Products).
        """
        workbook = openpyxl.load_workbook(BytesIO(excel_data), data_only=True)
        worksheet = workbook.active

        if not worksheet:
            raise ValueError("No active worksheet found in the Excel file")

        products = []
        store = None

        for row_idx, row in enumerate(worksheet.iter_rows(min_row=2, values_only=True)):
            if len(row) < 11:
                continue

            store_id = str(row[0] or "").strip()
            store_name = str(row[1] or "").strip()

            if not store_id:
                continue

            # Build store from first data row
            if store is None:
                store_info = STORES.get(store_id, {})
                store = Store(
                    chain=self.CHAIN,
                    store_id=store_id,
                    name=store_info.get("name", store_name),
                    store_type=store_info.get("store_type", ""),
                    city=store_info.get("city", ""),
                    street_address=store_info.get("street_address", ""),
                    zipcode=store_info.get("zipcode", ""),
                )
                if store_id not in STORES:
                    logger.warning(
                        f"Unknown store_id '{store_id}' ({store_name}), "
                        f"add it to the STORES map in djelo_vodice.py"
                    )

            try:
                data = {
                    "product_id": cell_str(row[2]),
                    "product": cell_str(row[3]),
                    "anchor_price": self.parse_price(cell_str(row[4]), False),
                    "barcode": cell_str(row[5]),
                    "category": cell_str(row[7]),
                    "price": self.parse_price(cell_str(row[8]), True),
                    "unit_price": self.parse_price(cell_str(row[9]), True),
                    "unit": cell_str(row[10]),
                    "brand": "",
                    "quantity": "",
                }
                data = self.fix_product_data(data)
                products.append(Product(**data))  # type: ignore
            except Exception as e:
                row_txt = "; ".join("" if v is None else str(v) for v in row)
                logger.warning(f"Failed to parse row {row_idx + 2}: `{row_txt}`: {e}")
                continue

        if store is None:
            raise ValueError("No data rows found in the XLSX file")

        logger.debug(f"Parsed {len(products)} products from Excel file")
        return store, products

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Fetch and parse all products for the given date.

        Args:
            date: The date to fetch the pricelist for.

        Returns:
            List containing a single Store with all products.
        """
        xlsx_url = self.get_index(date)
        logger.info(f"Found XLSX file: {xlsx_url}")

        with TemporaryFile(mode="w+b") as temp_file:
            self.fetch_binary(xlsx_url, temp_file)
            temp_file.seek(0)
            excel_data = temp_file.read()

        store, products = self.parse_excel(excel_data)

        if not products:
            logger.warning(f"No products found for date {date}")
            return []

        store.items = products
        return [store]


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = DjeloVodiceCrawler()
    stores = crawler.crawl(datetime.date.today())
    print(stores[0])
    print(stores[0].items[0])
