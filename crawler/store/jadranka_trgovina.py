import datetime
import logging
import re

from bs4 import BeautifulSoup
from crawler.store.models import Product, Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)


class JadrankaTrgovinaCrawler(BaseCrawler):
    """
    Crawler for Jadranka Trgovina store prices.

    Jadranka Trgovina publishes daily CSV price lists for a single store location
    (Market Maxi Dražica 5, Mali Lošinj). Files follow the pattern:
    MARKET_MAXI_DRAZICA5_MALILOSINJ_607_DDMMYYYY_0800.csv
    """

    CHAIN = "jadranka_trgovina"
    BASE_URL = "https://jadranka-trgovina.com"
    INDEX_URL = "https://jadranka-trgovina.com/cjenici/"

    # Regex to match CSV filenames and extract date
    # Format: MARKET_MAXI_DRAZICA5_MALILOSINJ_607_DDMMYYYY_0800.csv
    CSV_FILENAME_PATTERN = re.compile(
        r"MARKET_MAXI_DRAZICA5_MALILOSINJ_607_(\d{2})(\d{2})(\d{4})_0800\.csv"
    )

    # Mapping for price fields from CSV columns
    # CSV columns in Croatian:
    # NAZIV PROIZVODA, ŠIFRA PROIZVODA, MARKA PROIZVODA, NETO KOLIČINA,
    # JEDINICA MJERE, MALOPRODAJNA CIJENA, CIJENA ZA JEDINICU MJERE,
    # MPC ZA VRIJEME POSEBNOG OBLIKA PRODAJE, NAJNIŽA CIJENA U POSLJEDNIH 30 DANA,
    # SIDRENA CIJENA NA 2.5.2025, BARKOD, KATEGORIJA PROIZVODA
    PRICE_MAP = {
        # field: (column_name, is_required)
        # Note: Many products have empty retail price but filled special price
        # Some products also have empty unit_price
        "price": ("MALOPRODAJNA CIJENA", False),
        "unit_price": ("CIJENA ZA JEDINICU MJERE", False),
        "special_price": ("MPC ZA VRIJEME POSEBNOG OBLIKA PRODAJE", False),
        "best_price_30": ("NAJNIŽA CIJENA U POSLJEDNIH 30 DANA", False),
        "anchor_price": ("SIDRENA CIJENA NA 2.5.2025", False),
    }

    # Mapping for other product fields from CSV columns
    FIELD_MAP = {
        "product_id": ("ŠIFRA PROIZVODA", True),
        "product": ("NAZIV PROIZVODA", True),
        "brand": ("MARKA PROIZVODA", False),
        "barcode": ("BARKOD", False),
        "category": ("KATEGORIJA PROIZVODA", False),
        "quantity": ("NETO KOLIČINA", False),
        "unit": ("JEDINICA MJERE", False),
    }

    def parse_index(self, content: str) -> list[str]:
        """
        Parse the Jadranka Trgovina index page to extract CSV links.

        Args:
            content: HTML content of the index page

        Returns:
            List of absolute CSV URLs found on the page
        """
        soup = BeautifulSoup(content, "html.parser")
        urls = []

        # Find all links ending with .csv
        for link_tag in soup.select('a[href$=".csv"]'):
            href = str(link_tag.get("href"))
            # Make absolute URL if needed
            if not href.startswith("http"):
                href = (
                    f"{self.BASE_URL}{href}"
                    if href.startswith("/")
                    else f"{self.BASE_URL}/{href}"
                )
            urls.append(href)

        return urls

    def get_index(self, date: datetime.date) -> str | None:
        """
        Fetch the index page and find the CSV URL for the specified date.

        Args:
            date: The date for which to fetch the price list

        Returns:
            CSV URL for the specified date, or None if not found
        """
        content = self.fetch_text(self.INDEX_URL)
        if not content:
            logger.warning(
                f"No content found at Jadranka Trgovina index URL: {self.INDEX_URL}"
            )
            return None

        urls = self.parse_index(content)

        # Format date as DDMMYYYY to match filename pattern
        date_str = f"{date.day:02d}{date.month:02d}{date.year}"

        # Find URL matching the requested date
        for url in urls:
            if date_str in url:
                logger.info(f"Found Jadranka Trgovina CSV for {date}: {url}")
                return url

        logger.warning(f"No Jadranka Trgovina CSV found for date {date}")
        return None

    def parse_store_info(self) -> Store:
        """
        Create store information for the single Jadranka Trgovina location.

        Jadranka Trgovina only has one location that publishes prices:
        Market Maxi Dražica 5, Mali Lošinj (Store ID: 607)

        Returns:
            Store object with the fixed store information
        """
        return Store(
            chain=self.CHAIN,
            store_id="607",
            name="Jadranka Trgovina Market Maxi",
            store_type="market",
            city="Mali Lošinj",
            street_address="Dražica 5",
            zipcode="",
            items=[],
        )

    def get_store_prices(self, csv_url: str) -> list[Product]:
        """
        Fetch and parse store prices from a CSV URL.

        Args:
            csv_url: URL to the CSV file containing prices

        Returns:
            List of Product objects parsed from the CSV
        """
        try:
            content = self.fetch_text(csv_url, encodings=["windows-1250", "utf-8"])
            return self.parse_csv(content, delimiter=";")
        except Exception as e:
            logger.error(
                f"Failed to get Jadranka Trgovina prices from {csv_url}: {e}",
                exc_info=True,
            )
            return []

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Main method to fetch and parse Jadranka Trgovina store and price data.

        Args:
            date: The date for which to fetch price data

        Returns:
            List containing a single Store object with products, or empty list if unavailable
        """
        csv_url = self.get_index(date)

        if not csv_url:
            logger.warning(f"No Jadranka Trgovina data available for {date}")
            return []

        try:
            store = self.parse_store_info()
            products = self.get_store_prices(csv_url)
        except Exception as e:
            logger.error(f"Error processing Jadranka Trgovina: {e}", exc_info=True)
            return []

        if not products:
            logger.warning("No products found for Jadranka Trgovina")
            return []

        store.items = products
        logger.info(f"Jadranka Trgovina: {len(products)} products found")
        return [store]


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = JadrankaTrgovinaCrawler()
    stores = crawler.crawl(datetime.date.today())
    if stores:
        print(stores[0])
        if stores[0].items:
            print(stores[0].items[0])
    else:
        print("No stores found")
