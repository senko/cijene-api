import datetime
import logging
import re
from typing import Optional

from crawler.store.models import Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)


class DukatCrawler(BaseCrawler):
    """
    Crawler for Dukat discount store prices.

    Fetches the index page at /diskonti, extracts per-store CSV links
    (one CSV per store per day), filters by date, and parses each CSV.
    Store metadata (address, zipcode, city, store_id) is encoded in the
    filename, not in the CSV body.
    """

    CHAIN = "dukat"
    BASE_URL = "https://dukat.hr"
    INDEX_URL = f"{BASE_URL}/diskonti"

    PRICE_MAP = {
        "price": ("MALOPRODAJNA CIJENA", False),
        "unit_price": ("CIJENA ZA JEDINICU MJERE", False),
        "special_price": ("MPC ZA VRIJEME POSEBNOG OBLIKA PRODAJE", False),
        "best_price_30": ("NAJNIžA CIJENA U POSLJEDNIH 30 DANA", False),
        "anchor_price": ("SIDRENA CIJENA NA 2.5.2025", False),
    }

    FIELD_MAP = {
        "product": ("NAZIV PROIZVODA", True),
        "product_id": ("ŠIFRA PROIZVODA", True),
        "brand": ("MARKA PROIZVODA", False),
        "quantity": ("NETO KOLIčINA", False),
        "unit": ("JEDINICA MJERE", False),
        "barcode": ("BARKOD", False),
        "category": ("KATEGORIJA PROIZVODA", False),
    }

    LINK_PATTERN = re.compile(
        r"/media/\d+/trgovina-[^\"'<>\s]+\.csv",
        re.IGNORECASE,
    )

    FILENAME_PATTERN = re.compile(
        r"trgovina-(?P<address>.+?)-(?P<zipcode>\d{5})-(?P<city>.+?)-"
        r"(?P<store_id>\d{6})-\d+-"
        r"(?P<day>\d{2})-(?P<month>\d{2})-(?P<year>\d{4})-"
        r"\d{2}-\d{2}\.csv$",
        re.IGNORECASE,
    )

    def parse_index(self, html: str, date: datetime.date) -> list[str]:
        """Find all CSV links on the index page that match the requested date."""
        seen: set[str] = set()
        urls: list[str] = []
        for path in self.LINK_PATTERN.findall(html):
            if path in seen:
                continue
            seen.add(path)

            m = self.FILENAME_PATTERN.search(path)
            if not m:
                logger.debug(f"Skipping link, filename did not match pattern: {path}")
                continue

            link_date = datetime.date(
                int(m.group("year")), int(m.group("month")), int(m.group("day"))
            )
            if link_date != date:
                continue

            urls.append(self.BASE_URL + path)

        if not urls:
            raise ValueError(f"No price list found for {date}")

        return urls

    def parse_store_from_url(self, url: str) -> Optional[Store]:
        """Extract store information from the CSV URL filename."""
        m = self.FILENAME_PATTERN.search(url)
        if not m:
            logger.warning(f"URL doesn't match expected pattern: {url}")
            return None

        address = m.group("address").replace("-", " ").title()
        city = m.group("city").replace("-", " ").title()
        zipcode = m.group("zipcode")
        store_id = m.group("store_id")

        store = Store(
            chain=self.CHAIN,
            store_id=store_id,
            name=f"Dukat {city}",
            store_type="diskont",
            city=city,
            street_address=address,
            zipcode=zipcode,
            items=[],
        )

        logger.info(
            f"Parsed store: {store.name} ({store.store_id}), "
            f"{store.store_type}, {store.city}, {store.street_address}, {store.zipcode}"
        )
        return store

    def get_all_products(self, date: datetime.date) -> list[Store]:
        html = self.fetch_text(self.INDEX_URL)
        urls = self.parse_index(html, date)
        logger.debug(f"Found {len(urls)} CSV URLs for {date}")

        stores: list[Store] = []
        for url in urls:
            store = self.parse_store_from_url(url)
            if not store:
                logger.warning(f"Skipping CSV {url} due to store parsing failure")
                continue

            text = self.fetch_text(url, encodings=["windows-1250"])
            store.items = self.parse_csv(text, delimiter=",")
            stores.append(store)

        return stores


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = DukatCrawler()
    stores = crawler.get_all_products(datetime.date.today())
    print(stores[0])
    print(stores[0].items[0])
