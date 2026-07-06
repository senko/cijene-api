import datetime
import logging
import re
import urllib.parse
from collections import defaultdict

from bs4 import BeautifulSoup
from crawler.store.models import Product, Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)


class ZabacCrawler(BaseCrawler):
    """Crawler for Žabac store prices."""

    CHAIN = "zabac"
    BASE_URL = "https://zabacfoodoutlet.hr/cjenik/"

    # Mapping for price fields from CSV columns
    PRICE_MAP = {
        # field: (column_name, is_required)
        "price": ("MPC", False),
        "unit_price": ("MPC", False),  # Use same as price
        "best_price_30": ("Najniža cijena u posljednjih 30 dana", False),
        "anchor_price": ("Sidrena cijena na 2.5.2025", False),
    }

    # Mapping for other product fields from CSV columns
    FIELD_MAP = {
        "product_id": ("Šifra artikla", True),
        "barcode": ("Barcode", False),
        "product": ("Naziv artikla", True),
        "brand": ("Marka", False),
        "quantity": ("Gramaža", False),
        "category": ("Naziv grupe artikala", False),
    }

    # Store pages on the Žabac website, keyed by the ?store= query parameter
    # value the site uses to switch between stores, mapped to store metadata.
    # The store_id values are stable identifiers and must not change, so that
    # price history stays connected to the same store across site redesigns
    # (the June 2026 redesign replaced the old ?lokacija= parameter).
    LOCATIONS = {
        "Dubec, Dubrava": {
            "store_id": "PJ-7",
            "name": "Žabac PJ-7",
            "store_type": "Supermarket",
            "street_address": "Dubrava 256L",
            "city": "Zagreb",
            "zipcode": "10000",
        },
        "Velika Gorica": {
            "store_id": "PJ-VG",
            "name": "Žabac PJ-VG",
            "store_type": "Supermarket",
            "street_address": "Trg Grada Vukovara 8",
            "city": "Velika Gorica",
            "zipcode": "10410",
        },
    }

    # Matches a dd.mm.yyyy date embedded in a price list row title, e.g.
    # "Supermarket,Dubrava 256L, Zagreb 10000, 02.07.2026, 7.00h - C302".
    DATE_RE = re.compile(r"(\d{1,2})\.(\d{1,2})\.(\d{4})")

    def parse_index(self, content: str) -> list[tuple[str, str]]:
        """
        Parse a Žabac store page to extract CSV links and their titles.

        Every CSV link on the page — both the "Aktualno izdanje" featured
        entry (which carries the most recent, current-day price list) and the
        archive rows below it — is preceded by an <h3> heading whose text
        carries the store address and the price list date, e.g.:

            Supermarket,Dubrava 256L, Zagreb 10000, 02.07.2026, 7.00h - C302

        Args:
            content: HTML content of the store page

        Returns:
            List of unique (title, csv_url) tuples found on the page.
        """
        soup = BeautifulSoup(content, "html.parser")
        # Deduplicate by URL while keeping each link's heading text.
        titles_by_url: dict[str, str] = {}

        for link_tag in soup.select('a[href$=".csv"]'):
            href = str(link_tag.get("href"))
            heading = link_tag.find_previous("h3")
            title = heading.get_text(strip=True) if heading else ""
            titles_by_url.setdefault(href, title)

        return [(title, href) for href, title in titles_by_url.items()]

    def log_unknown_locations(self, content: str) -> None:
        """
        Warn about store tabs on the page we have no metadata for.

        The page exposes a filter tab per store via ?store=<name> links. If
        Žabac adds a new store we won't have it in LOCATIONS and would
        silently skip it, so surface it in the logs.

        Args:
            content: HTML content of the store page
        """
        soup = BeautifulSoup(content, "html.parser")
        for tab in soup.select('a[href*="?store="]'):
            href = str(tab.get("href"))
            query = urllib.parse.urlparse(href).query
            for value in urllib.parse.parse_qs(query).get("store", []):
                if value not in self.LOCATIONS:
                    logger.warning(
                        f"Žabac page lists unknown store {value!r} "
                        f"(link {href}) with no crawler metadata"
                    )

    def get_store_prices(self, csv_url: str) -> list[Product]:
        """
        Fetch and parse store prices from a Žabac CSV URL.

        Args:
            csv_url: URL to the CSV file containing prices

        Returns:
            List of Product objects
        """
        try:
            # The CSVs are UTF-8 with a BOM; decode with utf-8-sig so the BOM
            # doesn't get glued onto the first ("Šifra artikla") header name.
            content = self.fetch_text(csv_url, encodings=["utf-8-sig"])
            return self.parse_csv(content)
        except Exception as e:
            logger.error(
                f"Failed to get Žabac store prices from {csv_url}: {e}",
                exc_info=True,
            )
            return []

    def get_index(self, date: datetime.date) -> list[tuple[str, str]]:
        """
        Fetch and parse all Žabac store pages to get CSV URLs for given date.

        The CSV URLs no longer contain the date (they are opaque hashed
        names), so we parse the date and store address out of each link's
        <h3> title instead of filtering the URL by a date substring.

        Args:
            date: The date parameter

        Returns:
            List of (csv_url, location_key) tuples for the given date.
        """
        results = []

        for location_key, loc in self.LOCATIONS.items():
            page_url = f"{self.BASE_URL}?store={urllib.parse.quote(location_key)}"
            content = self.fetch_text(page_url)
            if not content:
                logger.warning(f"No content at {page_url}")
                continue

            self.log_unknown_locations(content)

            for title, url in self.parse_index(content):
                match = self.DATE_RE.search(title)
                if not match:
                    continue
                day, month, year = (int(g) for g in match.groups())
                try:
                    title_date = datetime.date(year, month, day)
                except ValueError:
                    logger.warning(f"Invalid date in Žabac title: {title!r}")
                    continue
                if title_date != date:
                    continue
                # Guard against the server silently serving the default store
                # page when it doesn't recognise the ?store= value: only accept
                # rows whose title matches this store's address.
                if loc["street_address"] not in title:
                    logger.warning(
                        f"Žabac CSV for {date} at {page_url} has title "
                        f"{title!r} not matching expected address "
                        f"{loc['street_address']!r}, skipping"
                    )
                    continue
                results.append((url, location_key))

        return results

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Main method to fetch and parse all Žabac store, product, and price info.

        Args:
            date: The date parameter

        Returns:
            List of Store objects with their products.
        """
        csv_links = self.get_index(date)

        if not csv_links:
            logger.warning("No Žabac CSV links found")
            return []

        # Group URLs by location to create one Store per location
        location_urls: dict[str, list[str]] = defaultdict(list)
        for url, location_key in csv_links:
            location_urls[location_key].append(url)

        stores = []
        for location_key, urls in location_urls.items():
            loc = self.LOCATIONS[location_key]
            store = Store(
                chain=self.CHAIN,
                store_type=loc["store_type"],
                store_id=loc["store_id"],
                name=loc["name"],
                street_address=loc["street_address"],
                zipcode=loc["zipcode"],
                city=loc["city"],
                items=[],
            )

            for url in urls:
                products = self.get_store_prices(url)
                store.items.extend(products)

            if not store.items:
                logger.warning(f"No products for {store.name}, skipping")
                continue

            stores.append(store)

        return stores

    def fix_product_data(self, data: dict) -> dict:
        """
        Clean and fix Žabac-specific product data.

        Args:
            data: Dictionary containing the row data

        Returns:
            The cleaned data
        """
        if "product" in data and data["product"]:
            data["product"] = data["product"].strip()

        # Unit is not available in the CSV
        data["unit"] = ""

        return super().fix_product_data(data)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = ZabacCrawler()
    stores = crawler.crawl(datetime.date.today())
    for store in stores:
        print(store)
        print(store.items[0])
