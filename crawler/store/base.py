import datetime
import unicodedata
from csv import DictReader
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from logging import getLogger
from re import Pattern
from tempfile import NamedTemporaryFile
from time import time
from typing import Any, BinaryIO, Generator, Iterable
from zipfile import ZipFile

import httpx
from bs4 import BeautifulSoup

from .models import Product, Store

logger = getLogger(__name__)

# A source column (or XML tag) may be known under several names, for example
# while a chain migrates from one header to another. A spec is either a single
# name or a list of accepted spellings; the first one present in the file wins.
ColumnSpec = str | list[str]

# field name -> (names as declared, names lowercased for lookup, is_required)
CompiledSpec = tuple[str, list[str], list[str], bool]


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

    PRICE_MAP: dict[str, tuple[ColumnSpec, bool]]
    """Mapping from CSV column names to price fields and whether they are required."""

    FIELD_MAP: dict[str, tuple[ColumnSpec, bool]]
    """Mapping from CSV column names to non-price fields and whether they are required."""

    BOOL_MAP: dict[str, tuple[ColumnSpec, bool]] = {}
    """Mapping from CSV column names to boolean fields and whether they are required."""

    REQUIRED_COLUMNS: list[ColumnSpec]
    """
    Columns the source file must contain. A missing one is a breaking format
    change: the file is abandoned and an error is logged.

    Every crawler declares this, like PRICE_MAP and FIELD_MAP above. The two
    crawlers that parse by column position rather than by name declare it empty.

    This deliberately repeats column names from the maps above, because it
    answers a different question than the `is_required` flag there. That flag
    says whether an individual *row* may leave the value empty. Fields like
    `price` and `barcode` are row-optional in most crawlers because
    `fix_product_data` invents a fallback, but losing their column entirely
    would silently produce wrong prices and synthetic barcodes.
    """

    OPTIONAL_COLUMNS: list[ColumnSpec] = []
    """
    Columns we expect but can process without, such as the ones dropped by
    NN 101/2026. A missing one is logged as a warning and parsing continues.
    """

    TRUE_VALUES = frozenset({"dostupno", "dostupan", "da", "d", "1", "true", "yes"})
    FALSE_VALUES = frozenset(
        {"nedostupno", "nedostupan", "ne", "n", "0", "false", "no"}
    )

    def __init__(self):
        self.client = httpx.Client(
            timeout=self.TIMEOUT,
            follow_redirects=True,
            verify=self.VERIFY_TLS_CERT,
        )

        # The maps and column lists are class attributes and nothing mutates
        # them at runtime, so the lowercased lookup keys are computed once here
        # rather than per row or per file.
        self._price_specs = self._compile_map(self.PRICE_MAP)
        self._field_specs = self._compile_map(self.FIELD_MAP)
        self._bool_specs = self._compile_map(self.BOOL_MAP)

        self._required_specs = [
            self._names_and_keys(spec) for spec in self.REQUIRED_COLUMNS
        ]
        self._optional_specs = [
            self._names_and_keys(spec) for spec in self.OPTIONAL_COLUMNS
        ]

        # Unrecognized boolean values are reported once each, so a single
        # unknown token can't produce one log line per row.
        self._unknown_bool_values: set[str] = set()

    @staticmethod
    def _names_and_keys(spec: ColumnSpec) -> tuple[list[str], list[str]]:
        """
        Split a column spec into its declared names and lowercased lookup keys.

        A bare string is wrapped rather than iterated, since a string is itself
        a sequence of characters. Empty names are dropped: a few crawlers
        declare a field the source doesn't have, such as Vrutak's anchor price.
        """
        names = [spec] if isinstance(spec, str) else list(spec)
        names = [name for name in names if name]
        return names, [name.lower() for name in names]

    @classmethod
    def _compile_map(
        cls, mapping: dict[str, tuple[ColumnSpec, bool]]
    ) -> list[CompiledSpec]:
        """Pre-compute lookup keys for a field mapping."""
        compiled: list[CompiledSpec] = []
        for field, (spec, is_required) in mapping.items():
            names, keys = cls._names_and_keys(spec)
            compiled.append((field, names, keys, is_required))
        return compiled

    def check_columns(self, available: Iterable[str], fold_case: bool = True) -> None:
        """
        Validate the columns (or XML tags) a source file provides.

        Args:
            available: Column or tag names found in the file.
            fold_case: Compare case-insensitively. True for CSV, where lookups
                are already case-insensitive; False for XML, where tag lookups
                are case-sensitive and a lenient check here would pass for
                structures the parser then can't read.

        Raises:
            ValueError: If a required column is missing.
        """
        present = {name.lower() if fold_case else name for name in available if name}

        def missing(specs) -> list[str]:
            return [
                "/".join(names)
                for names, keys in specs
                if not any(name in present for name in (keys if fold_case else names))
            ]

        absent = missing(self._required_specs)
        if absent:
            found = ", ".join(name for name in available if name)
            raise ValueError(
                f"Missing required column(s): {', '.join(absent)}. Found: {found}"
            )

        absent = missing(self._optional_specs)
        if absent:
            logger.warning(
                f"{self.CHAIN}: expected column(s) missing, "
                f"continuing without them: {', '.join(absent)}"
            )

    def parse_bool(self, value: str | None) -> bool | None:
        """
        Parse a boolean-ish source value such as dostupno/nedostupno.

        Unrecognized values return None and are logged once per distinct value,
        so the vocabulary can be extended from the crawl logs without one odd
        token producing a log line per row.

        Args:
            value: Raw value, or None if the column is absent.

        Returns:
            True, False, or None when the value is empty or unrecognized.
        """
        if value is None:
            return None

        text = str(value).strip().lower()
        if not text:
            return None

        if text in self.TRUE_VALUES:
            return True
        if text in self.FALSE_VALUES:
            return False

        if text not in self._unknown_bool_values:
            self._unknown_bool_values.add(text)
            logger.warning(f"{self.CHAIN}: unknown boolean value {value!r}")
        return None

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

        if data["price"] is None or data["price"] == 0:
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

        # Distinguish "no promotion" from "chain publishes a name for it"
        if data.get("special_sale_type") is not None:
            data["special_sale_type"] = str(data["special_sale_type"]).strip() or None

        if data["unit_price"] is None:
            data["unit_price"] = data["price"]

        return data

    def parse_csv_row(self, row: dict) -> Product:
        """
        Parse a single row of CSV data into a Product object.
        """
        row = {k.lower(): v for k, v in row.items()}
        # Heterogeneous by design: prices, strings and flags all feed Product()
        data: dict[str, Any] = {}

        # Candidate keys are tried in order and matched on presence, so an empty
        # value in the first one doesn't fall through and read another column.
        for field, names, keys, is_required in self._price_specs:
            value = next((row[key] for key in keys if key in row), None)
            try:
                data[field] = self.parse_price(value, is_required)
            except ValueError as err:
                logger.warning(
                    f"Failed to parse {field} from {'/'.join(names)}: {err}",
                    exc_info=True,
                )
                raise

        for field, names, keys, is_required in self._field_specs:
            text = (next((row[key] for key in keys if key in row), None) or "").strip()
            if not text and is_required:
                raise ValueError(f"Missing required field: {field}")
            data[field] = text

        for field, _names, keys, is_required in self._bool_specs:
            flag = self.parse_bool(next((row[key] for key in keys if key in row), None))
            if flag is None and is_required:
                raise ValueError(f"Missing required field: {field}")
            data[field] = flag

        data = self.fix_product_data(data)
        return Product(**data)  # type: ignore

    @staticmethod
    def _xml_text(elem: Any, names: list[str]) -> str:
        """
        Return the text of the first candidate child tag that is present.

        A present but empty tag yields "" and stops the search, so an empty
        value can't fall through and read an alternative tag instead.
        """
        for name in names:
            texts = elem.xpath(f"{name}/text()")
            if texts:
                return texts[0] or ""
            if elem.xpath(name):
                return ""
        return ""

    def check_xml_columns(self, elements: list) -> None:
        """
        Validate the tag structure of an XML price list.

        Only the first product element is inspected; the rest of the file is
        assumed to share its structure. Call this once per file, before
        iterating, so a breaking change abandons the file instead of raising
        once per product.

        Args:
            elements: Product elements found in the file; empty is a no-op.

        Raises:
            ValueError: If a required tag is missing.
        """
        if not elements:
            return

        # Comments and processing instructions have a callable tag, not a name
        tags = [child.tag for child in elements[0] if isinstance(child.tag, str)]
        self.check_columns(tags, fold_case=False)

    def parse_xml_product(self, elem: Any) -> Product:
        # Heterogeneous by design: prices, strings and flags all feed Product()
        data: dict[str, Any] = {}
        for field, names, _keys, is_required in self._price_specs:
            value = self._xml_text(elem, names)
            try:
                data[field] = self.parse_price(value, is_required)
            except ValueError as err:
                tagname = "/".join(names) or field
                logger.warning(
                    f"Failed to parse {field} from {tagname}: {err}",
                    exc_info=True,
                )
                raise

        for field, names, _keys, is_required in self._field_specs:
            value = self._xml_text(elem, names)
            if not value and is_required:
                tagname = "/".join(names) or field
                raise ValueError(
                    f"Missing required field: {field} (expected <{tagname}>)"
                )
            data[field] = value

        for field, names, _keys, is_required in self._bool_specs:
            flag = self.parse_bool(self._xml_text(elem, names))
            if flag is None and is_required:
                raise ValueError(f"Missing required field: {field}")
            data[field] = flag

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

        Raises:
            ValueError: If the header row is missing or a required column is
                absent, in which case the file should be abandoned.
        """
        logger.debug("Parsing CSV content")

        reader = self.read_csv(content, delimiter=delimiter)
        if not reader.fieldnames:
            raise ValueError("CSV file is missing the header row")

        self.check_columns(reader.fieldnames)

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
