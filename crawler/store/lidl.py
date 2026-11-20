import datetime
import logging
import re
from typing import Optional
from urllib.parse import quote, unquote, urljoin

from bs4 import BeautifulSoup

from crawler.store.models import Product, Store

from .base import BaseCrawler, CrawlerBlocked

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
    ANCHOR_PRICE_DATE = datetime.date(2025, 5, 2)

    # Since 2026-11-17 the anchor column has no date in its name, and each
    # value carries its own date: "MPC na 10.9.2026: 5,99€". The spelling
    # varies from row to row ("MPC na 2.5.2025.:1,99€", "MPC na 4.11.2026..
    # 1,89€", "MPC na 10.9.2026: 3,€", "MPC 2.5. 2,29€"), so the date and the
    # price are matched separately. Renamed to "BAZNA_CIJENA_ROBE" on 2026-11-20.
    NEW_ANCHOR_PRICE_COLUMNS = (
        "SIDRENA_CIJENA",
        "BAZNA_CIJENA_ROBE",
        "BAZNA_CIJENA",
    )
    ANCHOR_DATE_PATTERN = re.compile(r"(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4}|\d{2})(?!\d)")
    ANCHOR_VALUE_PATTERN = re.compile(
        r"(\d{1,3}(?:\.\d{3})+|\d+)\s*([.,]\s*\d{0,2})?\s*(?:€|EUR)?\s*$"
    )

    # The 2026-11-17 file renamed several columns, one with a typo
    # ("JEDINICIN"), and the 2026-11-20 file suffixed most columns with "_ROBE".
    PRICE_MAP = {
        "price": (["MALOPRODAJNA_CIJENA", "MALOPRODAJNA_CIJENA_ROBE"], False),
        "unit_price": (
            [
                "CIJENA_ZA_JEDINICU_MJERE",
                "CIJENA_ZA_JEDINICIN_MJERE",
                "CIJENA_ZA_JEDINICIN_MJERE_ROBE",
                "CIJENA_ZA_JEDINICU_MJERE_ROBE",
            ],
            False,
        ),
        "special_price": ("MPC_ZA_VRIJEME_POSEBNOG_OBLIKA_PRODAJE", False),
        "anchor_price": (
            [ANCHOR_PRICE_COLUMN, *NEW_ANCHOR_PRICE_COLUMNS],
            False,
        ),
        "best_price_30": (
            ["NAJNIZA_CIJENA_U_POSLJ._30_DANA", "NAJNIZA_CIJENA_U_POSLJ_30_DANA"],
            False,
        ),
    }

    FIELD_MAP = {
        "product": (["NAZIV", "NAZIV_ROBE"], False),
        "product_id": (["ŠIFRA", "SIFRA_ROBE", "ŠIFRA_ROBE"], True),
        "brand": (["MARKA", "MARKA_ROBE"], False),
        "quantity": (["NETO_KOLIČINA", "NETO_KOLICINA"], False),
        "unit": ("JEDINICA_MJERE", False),
        "barcode": (["BARKOD", "BARKOD_ROBE", "EAN_BARKOD_ROBE"], False),
        "category": ("KATEGORIJA_PROIZVODA", False),
    }

    REQUIRED_COLUMNS = [
        ["MALOPRODAJNA_CIJENA", "MALOPRODAJNA_CIJENA_ROBE"],
        [
            "CIJENA_ZA_JEDINICU_MJERE",
            "CIJENA_ZA_JEDINICIN_MJERE",
            "CIJENA_ZA_JEDINICIN_MJERE_ROBE",
            "CIJENA_ZA_JEDINICU_MJERE_ROBE",
        ],
        [ANCHOR_PRICE_COLUMN, *NEW_ANCHOR_PRICE_COLUMNS],
        ["NAZIV", "NAZIV_ROBE"],
        ["ŠIFRA", "SIFRA_ROBE", "ŠIFRA_ROBE"],
        ["MARKA", "MARKA_ROBE"],
        "JEDINICA_MJERE",
        ["BARKOD", "BARKOD_ROBE", "EAN_BARKOD_ROBE"],
    ]

    # Not required by NN 101/2026, so the chain may drop them when it switches.
    OPTIONAL_COLUMNS = [
        ("NETO_KOLIČINA", "NETO_KOLICINA"),
        "KATEGORIJA_PROIZVODA",
        "MPC_ZA_VRIJEME_POSEBNOG_OBLIKA_PRODAJE",
        ("NAJNIZA_CIJENA_U_POSLJ._30_DANA", "NAJNIZA_CIJENA_U_POSLJ_30_DANA"),
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
        anchor_price = (row.get(self.ANCHOR_PRICE_COLUMN) or "").strip()
        if "Nije_bilo_u_prodaji" in anchor_price:
            row[self.ANCHOR_PRICE_COLUMN] = None

        anchor_col = next(
            (col for col in self.NEW_ANCHOR_PRICE_COLUMNS if col in row),
            None,
        )
        if anchor_col is None:
            return super().parse_csv_row(row)

        price, anchor_date = self.parse_anchor_value(row[anchor_col] or "")
        row[anchor_col] = price
        product = super().parse_csv_row(row)

        # The new header states no date, so ANCHOR_PRICE_DATE doesn't apply:
        # use the date from the value, or none.
        product.anchor_price_date = (
            anchor_date if product.anchor_price is not None else None
        )
        return product

    def parse_anchor_value(self, value: str) -> tuple[str, str | None]:
        """
        Split an anchor value such as "MPC na 10.9.2026: 5,99€".

        Args:
            value: Value of the SIDRENA_CIJENA column

        Returns:
            Tuple of (price string for parse_price, or "" if none; ISO date,
            or None if the value has no recognizable date)
        """
        # A few values repeat: "MPC na 2.5.2025: 1,05€ | MPC na 2.5.2025: 1,05€"
        value = value.split("|")[0].strip()

        anchor_date = None
        date_match = self.ANCHOR_DATE_PATTERN.search(value)
        if date_match:
            anchor_date = self.parse_anchor_date(".".join(date_match.groups())) or None
            value = value[date_match.end() :]

        price_match = self.ANCHOR_VALUE_PATTERN.search(value)
        if not price_match:
            return "", None

        whole, fraction = price_match.groups()
        return whole.replace(".", "") + (fraction or "").replace(" ", ""), anchor_date

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

        Returns the store with its products, or None if the store info can't be
        parsed from the filename, the download fails, or the CSV can't be parsed.

        Raises:
            CrawlerBlocked: Upstream is refusing our requests.
        """
        filename = unquote(url).rsplit("/", 1)[-1]
        store = self.parse_store_from_filename(filename)
        if not store:
            logger.warning(f"Skipping CSV {filename} due to store parsing failure")
            return None

        try:
            text = self.fetch_text(url, encodings=["utf-8-sig", "windows-1250"])
        except CrawlerBlocked:
            raise
        except Exception as e:
            logger.error(f"Failed to download {url}: {e}", exc_info=True)
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

        try:
            store.items = self.parse_csv(text, delimiter=delimiter)
        except Exception as e:
            logger.error(f"Failed to parse {url}: {e}", exc_info=True)
            return None

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
            try:
                store = self.get_store_prices(url)
            except CrawlerBlocked as e:
                logger.error(f"Stopping the Lidl crawl: {e}")
                break
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
