import datetime
import unicodedata
from csv import DictReader
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from email.utils import parsedate_to_datetime
from logging import getLogger
from random import uniform
from re import Pattern
from tempfile import NamedTemporaryFile
from time import monotonic, sleep, time
from typing import Any, BinaryIO, Callable, Generator, Iterable, TypeVar
from urllib.parse import urlsplit
from zipfile import ZipFile

import httpx
from bs4 import BeautifulSoup

from .models import Product, Store

logger = getLogger(__name__)

T = TypeVar("T")


class CrawlerBlocked(Exception):
    """
    Upstream is refusing our requests as a policy, not failing at random.

    Raised on a BLOCKED_STATUS, and on a 429 asking for longer than
    RETRY_AFTER_MAX. Neither is retried: further requests would only extend the
    block.

    A crawler should end the chain on this rather than swallow it per store, and
    return whatever it had already collected. `blocked` then tells crawl_chain
    the chain is incomplete.
    """


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

    MAX_RETRIES = 6
    """Total attempts per request, not retries after the first; 3 recovers ~96%."""

    REQUEST_DELAY = 0.0
    """
    Minimum seconds between two requests to the same host.

    Zero so chains that work today are unaffected; chains that throttle or ban
    us override it. See KonzumCrawler and BosoCrawler.
    """

    RETRY_BACKOFF_BASE = 1.0
    """Wait before the 2nd attempt, doubling per attempt, jittered +/-50%."""

    RETRY_BACKOFF_MAX = 30.0
    """Ceiling on that doubling; only engages if MAX_RETRIES or the base grows."""

    RETRY_AFTER_MAX = 120.0
    """Longest Retry-After we wait out; past this we treat it as being blocked."""

    RETRY_STATUS = frozenset({408, 425, 429})
    """
    Non-5xx statuses worth another attempt. Every 5xx is retried as well.

    404 is not in here. For most chains it means the file is not published yet,
    so retrying costs the full backoff ladder per URL and recovers nothing.
    Chains that serve a 404 for a file they are concurrently advertising add it
    themselves; see KonzumCrawler.
    """

    BLOCKED_STATUS = frozenset({403})
    """Statuses that mean we are banned; see CrawlerBlocked."""

    PERMANENT_ERRORS: tuple[type[httpx.HTTPError], ...] = (
        httpx.UnsupportedProtocol,
        httpx.LocalProtocolError,
        httpx.TooManyRedirects,
    )
    """
    Transport failures an identical request cannot get past: a scheme httpx will
    not speak, a request we built wrong, a redirect loop. Every other transport
    error is treated as transient.
    """

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

        self._last_request: dict[str, float] = {}
        self._cooldown: dict[str, float] = {}

        self.blocked = False
        """
        Set once upstream returns a BLOCKED_STATUS, and never cleared.

        Checked before every request so a ban costs exactly one 403 no matter
        how many per-store `except Exception` handlers swallow the error, and
        checked again by crawl_chain so the chain is reported as failed.
        """

        self.requests_made = 0
        """Attempts sent for this chain, retries included. Konzum caps on it."""

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

    def _throttle(self, url: str) -> None:
        """
        Wait until this host may be requested again.

        Two clocks, whichever is later: REQUEST_DELAY since the last request,
        and any cooldown a 429 asked for. Keyed per host, because several
        crawlers read their index from one host and the price lists from another
        and would otherwise pay for both.

        Backoff and pacing do not stack: the wait before a retry is
        max(REQUEST_DELAY, backoff), since the backoff sleep already counts
        towards the interval measured here.
        """
        host = urlsplit(url).netloc
        now = monotonic()
        ready = now

        last = self._last_request.get(host)
        if last is not None and self.REQUEST_DELAY > 0:
            ready = max(ready, last + self.REQUEST_DELAY)

        cooldown = self._cooldown.get(host)
        if cooldown is not None:
            ready = max(ready, cooldown)

        if ready > now:
            logger.debug(f"Pacing {host}: sleeping {ready - now:.2f}s")
            sleep(ready - now)
            now = monotonic()

        self._last_request[host] = now

    def is_retryable(self, err: httpx.HTTPError) -> bool:
        """
        Whether another attempt at a failed request could plausibly succeed.

        Callers that retry in passes of their own (see KonzumCrawler) ask this
        before queueing a URL again, so a failure this class refuses to retry is
        not retried behind its back.
        """
        if isinstance(err, self.PERMANENT_ERRORS):
            return False

        if not isinstance(err, httpx.HTTPStatusError):
            return True  # timeouts, resets and dropped connections

        status = err.response.status_code
        return status >= 500 or status in self.RETRY_STATUS

    @staticmethod
    def _retry_after(response: httpx.Response) -> float | None:
        """
        Parse a Retry-After header into seconds, honouring both formats.

        Returns None when the header is absent or unparseable, which the caller
        treats as "fall back to ordinary backoff". Capping is the caller's job.
        """
        header = response.headers.get("retry-after")
        if not header:
            return None

        # str() keeps the type concrete: httpx types Headers.get as Any, and
        # parsedate_to_datetime is overloaded on None.
        value = str(header).strip()

        try:
            return max(0.0, float(value))
        except ValueError:
            pass

        try:
            target = parsedate_to_datetime(value)
            now = datetime.datetime.now(tz=target.tzinfo)
            return max(0.0, (target - now).total_seconds())
        except (TypeError, ValueError):
            logger.warning(f"Unparseable Retry-After: {value!r}")
            return None

    def _with_retry(
        self,
        url: str,
        send: Callable[[], T],
        attempts: int | None = None,
    ) -> T:
        """
        Run a single request with pacing, retries and block detection.

        Every network access in the crawler goes through here, so a chain cannot
        opt out of pacing by looping over URLs itself.

        Args:
            url: URL being requested, for pacing and log messages.
            send: Performs the request and raises httpx errors on failure.
            attempts: Total attempts, defaulting to MAX_RETRIES.

        Raises:
            CrawlerBlocked: Upstream is refusing us; no further request is made
                for the rest of this crawler's life.
            httpx.HTTPError: The last attempt failed, or the failure is not
                worth retrying.
        """
        if self.blocked:
            raise CrawlerBlocked(
                f"{self.CHAIN}: refusing to request {url}, already blocked"
            )

        total = self.MAX_RETRIES if attempts is None else attempts

        for attempt in range(1, total + 1):
            self._throttle(url)
            self.requests_made += 1
            try:
                return send()
            except httpx.HTTPError as err:
                wait = self._handle_request_error(url, err, attempt, total)
                if wait is None:
                    raise
                logger.debug(
                    f"Attempt {attempt}/{total} of {url} failed: {err}; "
                    f"retrying in {wait:.1f}s"
                )
                sleep(wait)

        raise ValueError(f"Invalid attempt count for {url}: {total}")

    def _handle_request_error(
        self,
        url: str,
        err: httpx.HTTPError,
        attempt: int,
        total: int,
    ) -> float | None:
        """
        Classify a failed request and return the wait before retrying it.

        Returns None when the caller should stop and re-raise. Sets a 429
        cooldown as a side effect.

        Raises:
            CrawlerBlocked: The status says we are banned.
        """
        retry_after = None

        if isinstance(err, httpx.HTTPStatusError):
            status = err.response.status_code

            if status in self.BLOCKED_STATUS:
                server = err.response.headers.get("server", "unknown")
                self.blocked = True
                raise CrawlerBlocked(
                    f"{self.CHAIN}: blocked with HTTP {status} by "
                    f"{urlsplit(url).netloc} (server: {server}) on {url}"
                ) from err

            if status == 429:
                retry_after = self._retry_after(err.response)

                if retry_after is not None and retry_after > self.RETRY_AFTER_MAX:
                    self.blocked = True
                    raise CrawlerBlocked(
                        f"{self.CHAIN}: {urlsplit(url).netloc} asks us to wait "
                        f"{retry_after:.0f}s, over the "
                        f"{self.RETRY_AFTER_MAX:.0f}s cap; abandoning the chain"
                    ) from err

                if retry_after is not None:
                    host = urlsplit(url).netloc
                    self._cooldown[host] = monotonic() + retry_after
                    logger.info(
                        f"{self.CHAIN}: holding off {host} for {retry_after:.0f}s"
                    )

        if not self.is_retryable(err):
            return None

        if attempt >= total:
            return None

        delay = min(
            self.RETRY_BACKOFF_MAX,
            self.RETRY_BACKOFF_BASE * 2 ** (attempt - 1),
        ) * uniform(0.5, 1.5)

        if retry_after is not None:
            delay = max(delay, retry_after)

        return delay

    def fetch_text(
        self,
        url: str,
        encodings: list[str] | None = None,
        prefix: str | None = None,
        attempts: int | None = None,
    ) -> str:
        """
        Download a text file (web page or CSV) from the given URL.

        Args:
            url: URL to download from
            encodings: Optional encodings to decode the content. If None, uses default.
            prefix: Optional text the decoded content must start with, used to
                pick between candidate encodings.
            attempts: Total attempts, defaulting to MAX_RETRIES.

        Returns:
            The content of the file as a string.

        Raises:
            CrawlerBlocked: Upstream is refusing our requests.
            httpx.HTTPError: The download failed and is not worth retrying.
            ValueError: The content could not be decoded.
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

        def send() -> str:
            logger.debug(f"Fetching {url}")
            response = self.client.get(url)
            response.raise_for_status()
            if encodings:
                return try_decode(response.content)
            return response.text

        return self._with_retry(url, send, attempts)

    def post_form(
        self,
        url: str,
        data: dict[str, Any],
        headers: dict[str, str] | None = None,
        attempts: int | None = None,
    ) -> httpx.Response:
        """
        POST a form and return the response, paced and retried like a download.

        Exists so the one crawler that talks to an AJAX endpoint (Boso) does not
        have to bypass the pacing by calling self.client directly.
        """

        def send() -> httpx.Response:
            logger.debug(f"Posting to {url}")
            response = self.client.post(url, data=data, headers=headers)
            response.raise_for_status()
            return response

        return self._with_retry(url, send, attempts)

    def fetch_binary(self, url: str, fp: BinaryIO, attempts: int | None = None):
        """
        Download a binary file to a provided location.

        The location should be created using tempfile.NamedTemporaryFile

        Args:
            url: URL of the ZIP file to download
            fp: Open file to write to. Truncated before each attempt.
            attempts: Total attempts, defaulting to MAX_RETRIES.

        Raises:
            CrawlerBlocked: Upstream is refusing our requests.
            httpx.HTTPError: The download failed and is not worth retrying.
        """

        logger.info(f"Downloading binary file from {url}")

        MB = 1024 * 1024

        def send() -> None:
            # A dropped connection leaves partial bytes; a retry would append
            # to them and yield a corrupt ZIP.
            fp.seek(0)
            fp.truncate()

            t0 = time()
            with self.client.stream("GET", url) as response:
                response.raise_for_status()
                total_mb = int(response.headers.get("content-length", 0)) // MB
                logger.debug(f"File size: {total_mb} MB")

                for chunk in response.iter_bytes(chunk_size=1 * MB):
                    fp.write(chunk)

            dt = int(time() - t0)
            logger.debug(f"Downloaded {total_mb} MB in {dt}s")

        self._with_retry(url, send, attempts)

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
