import datetime
import logging
import re
from io import BytesIO
from tempfile import NamedTemporaryFile
from typing import Generator, Optional
from urllib.parse import unquote, urljoin
from zipfile import BadZipFile, ZipFile

import httpx
from bs4 import BeautifulSoup

from crawler.store.models import Product, Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)


class LidlCrawler(BaseCrawler):
    """
    Crawler for Lidl store prices.

    This class handles downloading and parsing price data from Lidl's website.
    It fetches the price list index pages, finds the ZIP for the specified date,
    downloads and extracts it, and parses the CSV files inside.

    Since 2026-07-14 the ZIPs are uploaded manually with ad-hoc names (often
    without a year), sometimes with the CSVs nested in a subdirectory or in a
    ZIP within the ZIP, and are sometimes linked only from a sub-page. The date
    parsed from the ZIP name is treated as a hint and verified against the
    dates embedded in the CSV filenames inside.
    """

    CHAIN = "lidl"
    BASE_URL = "https://tvrtka.lidl.hr"
    INDEX_URLS = [
        f"{BASE_URL}/cijene",
        f"{BASE_URL}/cijene/cijene-u-trgovinama",
    ]
    TIMEOUT = 180.0  # Longer timeout for ZIP download

    # Matches a date in an ad-hoc ZIP name, e.g. "Popis_..._na_dan_13_07_2026",
    # "Cijene_14.07.", "CJENICI_g.a. 15.07.2026", "cijene za 16.07" (year optional)
    ZIP_NAME_DATE_PATTERN = re.compile(
        r"(\d{1,2})[._\s-]+(\d{1,2})(?:[._\s-]+(\d{4}))?"
    )

    # Matches the date embedded in CSV filenames, e.g. "..._16.07.2026_7.15h.csv"
    CSV_DATE_PATTERN = re.compile(r"_(\d{1,2})\.(\d{1,2})\.(\d{4})_")

    ANCHOR_PRICE_COLUMN = "Sidrena_cijena_na_02.05.2025"
    PRICE_MAP = {
        "price": ("MALOPRODAJNA_CIJENA", False),
        "unit_price": ("CIJENA_ZA_JEDINICU_MJERE", False),
        "special_price": ("MPC_ZA_VRIJEME_POSEBNOG_OBLIKA_PRODAJE", False),
        "anchor_price": (ANCHOR_PRICE_COLUMN, False),
        "best_price_30": ("NAJNIZA_CIJENA_U_POSLJ._30_DANA", False),
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
        filename = filename.rsplit("/", 1)[-1]
        logger.debug(f"Parsing store information from filename: {filename}")

        try:
            m = self.ADDRESS_PATTERN.match(filename)
            if not m:
                logger.warning(f"Filename doesn't match expected pattern: {filename}")
                return None

            store_type, store_id, address, zipcode, city = m.groups()
            city = city.replace("_", " ")
            address = address.replace("_", " ")

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

    def parse_zip_link_date(self, href: str) -> Optional[tuple[int, int, int | None]]:
        """
        Extract a (day, month, year-or-None) date hint from a ZIP link.

        Only the URL-decoded basename is considered, so the numeric segments
        in the download path (e.g. /content/download/162508/) can't misfire.
        """
        name = unquote(href).rsplit("/", 1)[-1]
        name = name.removesuffix(".zip")

        for m in self.ZIP_NAME_DATE_PATTERN.finditer(name):
            day, month, year = m.groups()
            day, month = int(day), int(month)
            year = int(year) if year else None
            if not (1 <= day <= 31 and 1 <= month <= 12):
                continue
            if year is not None and not (2020 <= year <= 2100):
                continue
            return (day, month, year)

        return None

    def date_from_csv_filename(self, filename: str) -> Optional[datetime.date]:
        """Extract the date embedded in a CSV filename, if present."""
        m = self.CSV_DATE_PATTERN.search(filename)
        if not m:
            return None
        day, month, year = (int(g) for g in m.groups())
        try:
            return datetime.date(year, month, day)
        except ValueError:
            return None

    def get_index(self, date: datetime.date) -> list[str]:
        """
        Return candidate ZIP URLs for the given date, best match first.

        ZIP names since 2026-07-14 are ad-hoc and often lack a year, so
        links matching the requested date's day and month are candidates,
        with explicit-year matches ordered before year-less ones. Actual
        content verification happens later against CSV filenames.
        """
        year_matches: list[str] = []
        yearless_matches: list[str] = []
        available: set[str] = set()
        seen: set[str] = set()

        for index_url in self.INDEX_URLS:
            try:
                content = self.fetch_text(index_url)
            except httpx.HTTPError as e:
                logger.warning(f"Failed to fetch index page {index_url}: {e}")
                continue

            soup = BeautifulSoup(content, "html.parser")
            for link in soup.select('a[href$=".zip"]'):
                url = urljoin(index_url, str(link["href"]))
                if url in seen:
                    continue
                seen.add(url)

                hint = self.parse_zip_link_date(url)
                if hint is None:
                    logger.debug(f"No date found in ZIP link, skipping: {url}")
                    continue

                day, month, year = hint
                available.add(f"{day:02d}.{month:02d}.{year or '????'}")
                if day != date.day or month != date.month:
                    continue
                if year is None:
                    yearless_matches.append(url)
                elif year == date.year:
                    year_matches.append(url)

        candidates = year_matches + yearless_matches
        if not candidates:
            raise ValueError(
                f"No price list found for {date} (available: {sorted(available)})"
            )

        logger.info(f"Found {len(candidates)} price list candidate(s) for {date}")
        return candidates

    def get_zip_contents(
        self, url: str, suffix: str
    ) -> Generator[tuple[str, bytes], None, None]:
        """
        Download a ZIP and yield (basename, content) for matching files.

        Unlike the base implementation, this handles files nested in
        subdirectories (by yielding basenames) and unwraps one level of
        ZIP-in-ZIP packaging, both seen in Lidl uploads since 2026-07-14.
        """
        with NamedTemporaryFile(mode="w+b") as temp_zip:
            self.fetch_binary(url, temp_zip)
            temp_zip.seek(0)

            with ZipFile(temp_zip, "r") as zip_fp:
                yield from self.yield_zip_files(zip_fp, suffix)

    def yield_zip_files(
        self, zip_fp: ZipFile, suffix: str, depth: int = 0
    ) -> Generator[tuple[str, bytes], None, None]:
        for file_info in zip_fp.infolist():
            if file_info.filename.endswith("/"):
                continue

            basename = file_info.filename.rsplit("/", 1)[-1]

            if basename.lower().endswith(".zip") and depth == 0:
                logger.info(f"Unpacking nested ZIP: {file_info.filename}")
                try:
                    with ZipFile(BytesIO(zip_fp.read(file_info))) as inner_zip:
                        yield from self.yield_zip_files(inner_zip, suffix, depth + 1)
                except BadZipFile as e:
                    logger.error(f"Invalid nested ZIP {file_info.filename}: {e}")
                continue

            if not basename.endswith(suffix):
                continue

            logger.debug(f"Processing file: {file_info.filename}")
            try:
                yield (basename, zip_fp.read(file_info))
            except Exception as e:
                logger.error(
                    f"Error processing file {file_info.filename}: {e}",
                    exc_info=True,
                )

    def process_zip(self, zip_url: str, date: datetime.date) -> Optional[list[Store]]:
        """
        Download and parse one price list ZIP, verifying it's for the right date.

        Returns the parsed stores, or None if the ZIP turns out to contain
        data for a different date (dates are taken from CSV filenames) or
        yields no parseable stores.
        """
        stores = []
        verified = 0

        for filename, content in self.get_zip_contents(zip_url, ".csv"):
            csv_date = self.date_from_csv_filename(filename)
            if csv_date is not None and csv_date != date:
                logger.warning(
                    f"CSV {filename} is dated {csv_date}, expected {date}; "
                    f"discarding ZIP {zip_url}"
                )
                return None
            if csv_date is not None:
                verified += 1
            else:
                logger.debug(f"No date found in CSV filename: {filename}")

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

        if not stores:
            logger.warning(f"No stores parsed from ZIP {zip_url}")
            return None

        if verified == 0:
            logger.warning(
                f"Could not verify date of any CSV in {zip_url}; "
                f"assuming it's for {date}"
            )

        return stores

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Main method to fetch and parse all products from Lidl's price lists.

        Args:
            date: The date for which to fetch the price list

        Returns:
            List of Store objects, each containing its products.

        Raises:
            ValueError: If the price list ZIP cannot be found or processed
        """
        for zip_url in self.get_index(date):
            stores = self.process_zip(zip_url, date)
            if stores:
                return stores

        raise ValueError(f"No valid price list found for {date}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = LidlCrawler()
    stores = crawler.get_all_products(datetime.date.today())
    print(stores[0])
    print(stores[0].items[0])
