from csv import DictReader
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from logging import getLogger
from tempfile import NamedTemporaryFile
from typing import Any, BinaryIO, Generator
from time import time
from zipfile import ZipFile
import datetime
from bs4 import BeautifulSoup
from re import Pattern
import unicodedata

import httpx

from .models import Product, Store

logger = getLogger(__name__)


class BaseCrawler:
    """
    Base crawler class with common functionality and interface for all crawlers.
    """

    CHAIN: str
    BASE_URL: str

    TIMEOUT = 30.0
    USER_AGENT = None
    VERIFY_TLS_CERT = True
    MAX_RETRIES = 3

    ZIP_DATE_PATTERN: Pattern | None = None

    PRICE_MAP: dict[str, tuple[str, bool]]
    """Mapping from CSV column names to price fields and whether they are required."""

    FIELD_MAP: dict[str, tuple[str, bool]]
    """Mapping from CSV column names to non-price fields and whether they are required."""

    def __init__(self):
        self.client = httpx.Client(
            timeout=30.0,
            follow_redirects=True,
            verify=self.VERIFY_TLS_CERT,
        )

    def fetch_text(
        self,
        url: str,
        encodings: list[str] | None = None,
        prefix: str | None = None,
    ) -> str:
        """
        Download a text file (web page or CSV) from the given URL.

        Args:
            url: URL to download from
            encoding: Optional encoding to decode the content. If None, uses default.

        Returns:
            The content of the file as a string, or an empty string if the download fails.
        """

        def try_decode(content: bytes) -> str:
            for encoding in encodings:  # type: ignore
                try:
                    text = content.decode(encoding)
                    if not prefix or text.startswith(prefix):
                        return text
                except UnicodeDecodeError:
                    continue
            raise ValueError(f"Error decoding {url} - tried: {encodings}")

        logger.debug(f"Fetching {url}")
        try:
            response = self.client.get(url)
            response.raise_for_status()
            if encodings:
                return try_decode(response.content)
            else:
                return response.text
        except httpx.RequestError as e:
            logger.error(f"Download from {url} failed: {e}", exc_info=True)
            raise

    def fetch_binary(self, url: str, fp: BinaryIO):
        """
        Download a binary file to a provided location.

        The location should be created using tempfile.NamedTemporaryFile

        Args:
            url: URL of the ZIP file to download

        Returns:
            Path to the downloaded ZIP file
        """

        logger.info(f"Downloading binary file from {url}")

        MB = 1024 * 1024

        t0 = time()
        with self.client.stream("GET", url) as response:
            response.raise_for_status()
            total_mb = int(response.headers.get("content-length", 0)) // MB
            logger.debug(f"File size: {total_mb} MB")

            for chunk in response.iter_bytes(chunk_size=1 * MB):
                fp.write(chunk)

        t1 = time()
        dt = int(t1 - t0)
        logger.debug(f"Downloaded {total_mb} MB in {dt}s")

    def read_csv(self, text: str, delimiter: str = ",") -> DictReader:
        return DictReader(text.splitlines(), delimiter=delimiter)  # type: ignore

    def get_zip_contents(
        self, url: str, suffix: str
    ) -> Generator[tuple[str, bytes], None, None]:
        with NamedTemporaryFile(mode="w+b") as temp_zip:
            self.fetch_binary(url, temp_zip)  # type: ignore
            temp_zip.seek(0)

            with ZipFile(temp_zip, "r") as zip_fp:
                for file_info in zip_fp.infolist():
                    if not file_info.filename.endswith(suffix):
                        continue

                    logger.debug(f"Processing file: {file_info.filename}")

                    try:
                        with zip_fp.open(file_info) as file:
                            xml_content = file.read()
                            yield (file_info.filename, xml_content)
                    except Exception as e:
                        logger.error(
                            f"Error processing file {file_info.filename}: {e}",
                            exc_info=True,
                        )

    @staticmethod
    def parse_price(
        price_str: str | None,
        required: bool = True,
    ) -> Decimal | None:
        """
        Parse a price string.

        The string may use either , or . as decimal separator, may omit leading
        zero, and may contain currency symbols "€" or "EUR".

        None is handled the same as empty string - no price information available.

        Args:
            price_str: String representing the price, or None (no price)
            required: If True (default), raises ValueError if the price is not valid
                    If False, returns None for invalid prices

        Returns:
            Parsed price as a Decimal with 2 decimal places

        Raises:
            ValueError: If required is True and the price is not valid
        """
        if price_str is None:
            price_str = ""

        if price_str and not any(c.isdigit() for c in price_str):
            price_str = ""

        # If price contains both "," and ".", assume what occurs first is the 1000s
        # separator and replace it with an empty string
        if "," in price_str and "." in price_str:
            if price_str.index(",") < price_str.index("."):
                price_str = price_str.replace(",", "")
            else:
                price_str = price_str.replace(".", "")

        price_str = (
            price_str.replace("€", "").replace("EUR", "").replace(",", ".").strip()
        )

        if not price_str:
            if required:
                raise ValueError("Price is required")
            else:
                return None

        # Handle missing leading zero
        if price_str.startswith("."):
            price_str = "0" + price_str

        try:
            # Convert to Decimal and round to 2 decimal places
            return Decimal(price_str).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        except (ValueError, TypeError, InvalidOperation):
            logger.warning(f"Failed to parse price: {price_str}")
            if required:
                raise ValueError(f"Invalid price format: {price_str}")
            else:
                return None

    @staticmethod
    def strip_diacritics(text: str) -> str:
        """
        Remove diacritics from a string.

        Args:
            text: The input string

        Returns:
            The string with diacritics removed
        """
        return "".join(
            c
            for c in unicodedata.normalize("NFD", text)
            if unicodedata.category(c) != "Mn"
        )

    def fix_product_data(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        Do any cleaning or transformation of the Product data here.

        Args:
            data: Dictionary containing the row data

        Returns:
            The cleaned or transformed data
        """
        # Common fixups for all crawlers
        if data["barcode"] == "":
            data["barcode"] = f"{self.CHAIN}:{data['product_id']}"
        data["barcode"] = data["barcode"].replace('"', "").replace("'", "").strip()

        if "special_price" not in data:
            data["special_price"] = None

        if data["price"] is None:
            if data.get("special_price") is None:
                if data.get("unit_price") is not None:
                    data["price"] = data["unit_price"]
                else:
                    raise ValueError(
                        "Price, special price, and unit price are all missing"
                    )
            else:
                data["price"] = data["special_price"]

        if data.get("anchor_price") is not None and not data.get("anchor_price_date"):
            data["anchor_price_date"] = datetime.date(2025, 5, 2).isoformat()

        if data["unit_price"] is None:
            data["unit_price"] = data["price"]

        return data

    def parse_csv_row(self, row: dict) -> Product:
        """
        Parse a single row of CSV data into a Product object.
        """
        data = {}

        for field, (column, is_required) in self.PRICE_MAP.items():
            value = row.get(column)
            try:
                data[field] = self.parse_price(value, is_required)
            except ValueError as err:
                logger.warning(
                    f"Failed to parse {field} from {column}: {err}",
                    exc_info=True,
                )
                raise

        for field, (column, is_required) in self.FIELD_MAP.items():
            value = row.get(column, "").strip()
            if not value and is_required:
                raise ValueError(f"Missing required field: {field}")
            data[field] = value

        data = self.fix_product_data(data)
        return Product(**data)  # type: ignore

    def parse_xml_product(self, elem: Any) -> Product:
        def get_text(xpath: Any, default=""):
            elements = elem.xpath(xpath)
            return elements[0] if elements and elements[0] else default

        data = {}
        for field, (tagname, is_required) in self.PRICE_MAP.items():
            value = get_text(f"{tagname}/text()")
            try:
                data[field] = self.parse_price(value, is_required)
            except ValueError as err:
                logger.warning(
                    f"Failed to parse {field} from {tagname}: {err}",
                    exc_info=True,
                )
                raise

        for field, (tagname, is_required) in self.FIELD_MAP.items():
            value = get_text(f"{tagname}/text()")
            if not value and is_required:
                raise ValueError(
                    f"Missing required field: {field} (expected <{tagname}>)"
                )
            data[field] = value

        data = self.fix_product_data(data)
        return Product(**data)  # type: ignore

    def parse_csv(self, content: str, delimiter: str = ",") -> list[Product]:
        """
        Parses CSV content into Product objects.

        Args:
            content: CSV content as a string
            delimiter: Delimiter used in the CSV file (default: ",")

        Returns:
            List of Product objects
        """
        logger.debug("Parsing CSV content")

        reader = self.read_csv(content, delimiter=delimiter)
        if not reader.fieldnames:
            raise ValueError("CSV file is missing the header row")

        # Make sure all defined columns exist in the CSV
        csv_columns = list(reader.fieldnames)
        price_columns = [column for column, _ in self.PRICE_MAP.values()]
        field_columns = [column for column, _ in self.FIELD_MAP.values()]
        for column in price_columns + field_columns:
            if column not in csv_columns:
                available = ", ".join(f'"{c}"' for c in csv_columns)
                raise ValueError(
                    f'Column "{column}" not found in CSV file. CSV columns: {available}'
                )

        products = []
        for row in reader:
            try:
                product = self.parse_csv_row(row)
            except Exception:
                logger.exception(f"Failed to parse row: {row}")
                continue
            products.append(product)

        logger.debug(f"Parsed {len(products)} products from CSV")
        return products

    def parse_index_for_zip(self, html_content: str) -> dict[datetime.date, str]:
        """
        Parse HTML and return ZIP links.

        Args:
            html_content: HTML content of the price list index page

        Returns:
            Dictionary mapping dates to ZIP file URLs
        """

        if not self.ZIP_DATE_PATTERN:
            raise NotImplementedError(
                f"{self.__class__.__name__}.ZIP_DATE_PATTERN is not defined"
            )

        soup = BeautifulSoup(html_content, "html.parser")
        zip_urls_by_date = {}

        links = soup.select('a[href$=".zip"]')
        for link in links:
            url = str(link["href"])

            m = self.ZIP_DATE_PATTERN.match(url)
            if not m:
                continue

            # Extract date from the URL
            day, month, year = m.groups()
            url_date = datetime.date(int(year), int(month), int(day))
            zip_urls_by_date[url_date] = url

        return zip_urls_by_date

    def get_all_products(self, date: datetime.date) -> list[Store]:
        raise NotImplementedError()

    def crawl(self, date: datetime.date) -> list[Store]:
        name = self.CHAIN.capitalize()
        logger.info(f"Starting {name} crawl for date: {date}")
        t0 = time()

        try:
            stores = self.get_all_products(date)
            n_prices = sum(len(store.items) for store in stores)

            t1 = time()
            dt = int(t1 - t0)

            logger.info(
                f"Completed {name} crawl for {date} in {dt}s, "
                f"found {len(stores)} stores with {n_prices} total prices"
            )
            return stores

        except Exception as e:
            logger.error(f"Error crawling {name} price list: {e}", exc_info=True)
            raise
