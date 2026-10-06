import datetime
import logging
import re
import time
import urllib.parse
from typing import List

import httpx
from bs4 import BeautifulSoup

from crawler.store.models import Product, Store

from .base import BaseCrawler, CrawlerBlocked

logger = logging.getLogger(__name__)


class KonzumCrawler(BaseCrawler):
    """Crawler for Konzum store prices."""

    CHAIN = "konzum"
    BASE_URL = "https://www.konzum.hr"
    INDEX_URL = f"{BASE_URL}/cjenici"

    # Konzum bans the crawler's IP for over an hour when hit unpaced, and fails
    # ~60% of price list requests at random rather than per URL. Repeated slow
    # passes recover them; deep retries on one URL do not.

    REQUEST_DELAY = 1.0
    """1 req/s: 4.7 req/s earned an hour-long ban, 1.6 req/s did not."""

    RETRY_STATUS = BaseCrawler.RETRY_STATUS | {404}
    """
    Konzum answers 404 for price lists its own index is advertising, and the
    same URL succeeds on a later attempt. Dropping 404 here halves the chain.
    The index is still listed verbatim when this happens, so it is a transient
    on their side rather than a stale link on ours.
    """

    CSV_ATTEMPTS = 2
    """Attempts per price list per round; SWEEP_ROUNDS supplies the depth."""

    SWEEP_ROUNDS = 6
    """Extra passes over the price lists still missing; 6 reached 188/188."""

    SWEEP_PAUSE = 15.0
    """Seconds between sweep rounds, to spread the retries out in time."""

    REQUEST_BUDGET = 800
    """Runaway guard on price list requests per run; a full recovery costs ~490."""

    # Mapping for price fields
    PRICE_MAP = {
        # field: (column, is_required)
        "price": ("MALOPRODAJNA CIJENA", False),
        "unit_price": ("CIJENA ZA JEDINICU MJERE", True),
        "special_price": ("MPC ZA VRIJEME POSEBNOG OBLIKA PRODAJE", False),
        "best_price_30": ("NAJNIŽA CIJENA U POSLJEDNIH 30 DANA", False),
        "anchor_price": ("SIDRENA CIJENA NA 2.5.2025", False),
    }

    # Mapping for other fields
    FIELD_MAP = {
        "product": ("NAZIV PROIZVODA", True),
        "product_id": ("ŠIFRA PROIZVODA", True),
        "brand": ("MARKA PROIZVODA", False),
        "quantity": ("NETO KOLIČINA", False),
        "unit": ("JEDINICA MJERE", False),
        "barcode": ("BARKOD", False),
        "category": ("KATEGORIJA PROIZVODA", False),
    }

    REQUIRED_COLUMNS = [
        "MALOPRODAJNA CIJENA",
        "CIJENA ZA JEDINICU MJERE",
        "SIDRENA CIJENA NA 2.5.2025",
        "NAZIV PROIZVODA",
        "ŠIFRA PROIZVODA",
        "MARKA PROIZVODA",
        "JEDINICA MJERE",
        "BARKOD",
    ]

    # Not required by NN 101/2026, so the chain may drop them when it switches.
    OPTIONAL_COLUMNS = [
        "NETO KOLIČINA",
        "KATEGORIJA PROIZVODA",
        "MPC ZA VRIJEME POSEBNOG OBLIKA PRODAJE",
        "NAJNIŽA CIJENA U POSLJEDNIH 30 DANA",
    ]

    ADDRESS_PATTERN = re.compile(r"(.*) (\d{5}) (.*)")

    def parse_index(self, content: str) -> list[str]:
        """
        Parse the Konzum index page to extract the price date and CSV links.

        Args:
            content: HTML content of the index page

        Returns:
            List of CSV urls on the page
        """

        soup = BeautifulSoup(content, "html.parser")

        urls = []
        csv_links = soup.select("a[format='csv']")

        for link in csv_links:
            href = link.get("href")
            if href:
                urls.append(f"{self.BASE_URL}{href}")

        return list(set(urls))

    def parse_store_info(self, url: str) -> Store:
        """
        Extracts store information from a CSV download URL.

        Args:
            url: CSV download URL with store information in the query parameters

        Returns:
            Store object with parsed store information, or None if parsing fails
        """

        logger.debug(f"Parsing store information from URL: {url}")

        parsed_url = urllib.parse.urlparse(url)
        query_params = urllib.parse.parse_qs(parsed_url.query)
        title = urllib.parse.unquote(query_params.get("title", [""])[0])
        title = title.replace("_", " ")

        if not title:
            raise ValueError(f"No title parameter found in URL: {url}")

        logger.debug(f"Decoded title: {title}")

        parts = [part.strip() for part in title.split(",")]
        if len(parts) < 6:  # Ensure we have the expected number of parts
            raise ValueError(f"Invalid CSV title format: {title}")

        # Extract store type
        store_type = (parts[0]).lower()
        store_id = parts[2] if len(parts) == 6 else parts[3]

        # Format:
        # SUPERMARKET,REPUBLIKE 1 31300 BELI MANASTIR,0904,1629,21.05.2025, 05-22.CSV
        # SUPERMARKET,CARLOTTA GRISI 5, SVETI ANTON 52466 NOVIGRAD,3274,1332,19.05.2025, 05-52.CSV
        m = self.ADDRESS_PATTERN.match(
            parts[1] if len(parts) == 6 else f"{parts[1]} {parts[2]}"
        )
        if not m:
            raise ValueError(f"Could not parse address from: {parts[1]}")

        # Extract address components
        street_address = m.group(1).strip().title()
        zipcode = m.group(2).strip()
        city = m.group(3).strip().title()

        store = Store(
            chain=self.CHAIN,
            store_type=store_type,
            store_id=store_id,
            name=f"{self.CHAIN.capitalize()} {city}",
            street_address=street_address,
            zipcode=zipcode,
            city=city,
            items=[],
        )

        logger.info(
            f"Parsed store: {store.store_type}, {store.street_address}, {store.zipcode}, {store.city}"
        )
        return store

    def get_index(self, date: datetime.date) -> list[str]:
        """
        Collect price list URLs from every page of the index.

        An unreadable page costs its ~20 stores, not the whole chain.

        Raises:
            CrawlerBlocked: Upstream is refusing our requests.
            ValueError: Not one page could be read.
        """
        url = f"{self.INDEX_URL}?date={date:%Y-%m-%d}"

        csv_urls = []
        pages_read = 0
        pages_failed = 0

        for page in range(1, 10):
            page_url = f"{url}&page={page}"
            try:
                content = self.fetch_text(page_url)
            except httpx.HTTPError as e:
                logger.error(f"Index page {page} failed, continuing: {e}")
                pages_failed += 1
                continue

            csv_urls_on_page = self.parse_index(content)
            if not csv_urls_on_page:
                break

            pages_read += 1
            csv_urls.extend(csv_urls_on_page)

        if not pages_read:
            raise ValueError(f"No index page could be read for {date}")

        if pages_failed:
            logger.warning(
                f"Read {pages_read} index page(s) and lost {pages_failed}; "
                f"the stores on the failed page(s) are missing from this run"
            )

        return csv_urls

    def get_store_prices(self, csv_url: str) -> List[Product]:
        """
        Download and parse one store's price list.

        Failures are not swallowed here: only the caller knows whether a URL is
        worth another sweep round, and it needs the exception to decide.

        Raises:
            CrawlerBlocked: Upstream is refusing our requests.
            httpx.HTTPError: The download failed.
            ValueError: The file arrived but could not be parsed.
        """
        content = self.fetch_text(csv_url, attempts=self.CSV_ATTEMPTS)
        return self.parse_csv(content)

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Main method to fetch and parse all store, product and price info.

        Walks the index once, then sweeps the price lists that came back empty
        in further rounds. Roughly half of Konzum's price lists fail per pass,
        at random per request rather than per URL, so what recovers them is
        repeated passes rather than deeper retries within one pass.

        Args:
            date: The date to search for in the price list.

        Returns:
            List of Store objects with their products.

        Raises:
            ValueError: If no price list is found for the given date.
        """

        csv_links = self.get_index(date)
        stores: list[Store] = []

        pending: list[tuple[str, Store]] = []
        for url in csv_links:
            try:
                pending.append((url, self.parse_store_info(url)))
            except Exception as e:
                logger.error(f"Error processing store from {url}: {e}", exc_info=True)

        sweeps = 0
        dropped = 0
        index_requests = self.requests_made

        while pending:
            if sweeps:
                logger.info(
                    f"Sweep {sweeps}/{self.SWEEP_ROUNDS}: retrying "
                    f"{len(pending)} price list(s) after {self.SWEEP_PAUSE:.0f}s"
                )
                time.sleep(self.SWEEP_PAUSE)

            stop = False
            missing: list[tuple[str, Store]] = []
            for i, (url, store) in enumerate(pending):
                if self.requests_made - index_requests >= self.REQUEST_BUDGET:
                    missing.extend(pending[i:])
                    stop = True
                    logger.error(
                        f"Request budget of {self.REQUEST_BUDGET} exhausted "
                        f"with {len(missing)} price list(s) unresolved"
                    )
                    break

                try:
                    products = self.get_store_prices(url)
                except CrawlerBlocked as e:
                    missing.extend(pending[i:])
                    stop = True
                    logger.error(f"Stopping the sweep: {e}")
                    break
                except httpx.HTTPError as e:
                    if not self.is_retryable(e):
                        logger.warning(f"Not retrying {url}: {e}")
                        dropped += 1
                        continue
                    logger.debug(f"No price list from {url} this round: {e}")
                    missing.append((url, store))
                    continue
                except Exception as e:
                    logger.error(
                        f"Failed to parse price list from {url}: {e}", exc_info=True
                    )
                    dropped += 1
                    continue

                if not products:
                    logger.warning(f"Dropping {url}: parsed no products")
                    dropped += 1
                    continue

                store.items = products
                stores.append(store)

            pending = missing
            if stop or sweeps >= self.SWEEP_ROUNDS:
                break
            sweeps += 1

        spent = self.requests_made - index_requests
        if pending:
            logger.error(
                f"Gave up on {len(pending)} of {len(csv_links)} price list(s) "
                f"after {sweeps} sweep(s) and {spent} price list requests"
            )
        if dropped:
            logger.error(
                f"Dropped {dropped} of {len(csv_links)} price list(s) as "
                f"unrecoverable; they were not retried"
            )

        logger.info(
            f"Collected {len(stores)} of {len(csv_links)} store(s) "
            f"in {spent} price list requests"
        )
        return stores


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = KonzumCrawler()
    stores = crawler.crawl(datetime.date.today())
    print(stores[0])
    print(stores[0].items[0])
