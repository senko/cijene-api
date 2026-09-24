import datetime
import logging
import re
import time
from typing import Optional
from urllib.parse import quote, unquote, urljoin

import httpx
from bs4 import BeautifulSoup

from crawler.store.models import Product, Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)


class LidlCrawler(BaseCrawler):
    """
    Crawler for Lidl store prices.

    Lidl publishes one CSV per store per day on the price list page on
    www.lidl.hr, keeping roughly the last 30 days. The date and the store
    information are both encoded in the CSV filename, e.g.
    "Supermarket 104_Ulica Dr. Franje Tuđmana_30_10450_Jastrebarsko_1_24.09.2026_7.15h.csv".

    Until August 2026 the lists were published as daily ZIPs on
    tvrtka.lidl.hr/cijene; that page is no longer updated.
    """

    CHAIN = "lidl"
    BASE_URL = "https://www.lidl.hr"
    INDEX_URL = f"{BASE_URL}/c/cijene/s10073252"

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

    REQUIRED_COLUMNS = [
        "MALOPRODAJNA_CIJENA",
        "CIJENA_ZA_JEDINICU_MJERE",
        "MPC_ZA_VRIJEME_POSEBNOG_OBLIKA_PRODAJE",
        "Sidrena_cijena_na_02.05.2025",
        "NAJNIZA_CIJENA_U_POSLJ._30_DANA",
        "NAZIV",
        "ŠIFRA",
        "MARKA",
        "NETO_KOLIČINA",
        "JEDINICA_MJERE",
        "BARKOD",
        "KATEGORIJA_PROIZVODA",
    ]

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
        Return the URLs of all per-store CSV files for the given date.

        The price list page links each store's CSV individually, for
        roughly the last 30 days. The date is taken from the CSV filename.
        """
        content = self.fetch_text(self.INDEX_URL)
        soup = BeautifulSoup(content, "html.parser")

        urls: list[str] = []
        available: set[datetime.date] = set()
        seen: set[str] = set()

        for link in soup.select('a[href$=".csv"]'):
            # Hrefs contain raw spaces and diacritics; normalize the encoding
            href = quote(unquote(str(link["href"])))
            url = urljoin(self.INDEX_URL, href)
            if url in seen:
                continue
            seen.add(url)

            csv_date = self.date_from_csv_filename(unquote(href))
            if csv_date is None:
                logger.debug(f"No date found in CSV link, skipping: {url}")
                continue

            available.add(csv_date)
            if csv_date == date:
                urls.append(url)

        if not urls:
            raise ValueError(
                f"No price list found for {date} "
                f"(available: {[d.isoformat() for d in sorted(available)]})"
            )

        logger.info(f"Found {len(urls)} store price lists for {date}")
        return urls

    def get_store_prices(self, url: str) -> Optional[Store]:
        """
        Download and parse a single store's CSV price list.

        Returns the store with its products, or None if the store info
        can't be parsed from the filename or the download fails.
        """
        filename = unquote(url).rsplit("/", 1)[-1]
        store = self.parse_store_from_filename(filename)
        if not store:
            logger.warning(f"Skipping CSV {filename} due to store parsing failure")
            return None

        text = None
        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                text = self.fetch_text(url, encodings=["utf-8-sig", "windows-1250"])
                break
            except httpx.HTTPError as e:
                # The server occasionally drops connections mid-crawl
                logger.warning(
                    f"Download attempt {attempt}/{self.MAX_RETRIES} of {url} failed: {e}"
                )
                if attempt < self.MAX_RETRIES:
                    time.sleep(2**attempt)
            except Exception as e:
                logger.error(f"Failed to download {url}: {e}", exc_info=True)
                return None

        if text is None:
            logger.error(f"Giving up on {url} after {self.MAX_RETRIES} attempts")
            return None

        headers = text.splitlines()[0] if text else ""
        if "\t" in headers:
            delimiter = "\t"
        elif ";" in headers:
            delimiter = ";"
        elif "," in headers:
            delimiter = ","
        else:
            logger.warning(f"Unknown delimiter in CSV: {filename}; ignoring")
            return None

        store.items = self.parse_csv(text, delimiter=delimiter)
        return store

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Main method to fetch and parse all products from Lidl's price lists.

        Args:
            date: The date for which to fetch the price list

        Returns:
            List of Store objects, each containing its products.

        Raises:
            ValueError: If no price lists are found or none can be parsed
        """
        stores = []
        for url in self.get_index(date):
            store = self.get_store_prices(url)
            if store:
                stores.append(store)

        if not stores:
            raise ValueError(f"No valid price list found for {date}")

        return stores


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = LidlCrawler()
    stores = crawler.get_all_products(datetime.date.today())
    print(stores[0])
    print(stores[0].items[0])
