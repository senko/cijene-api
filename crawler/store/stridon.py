import datetime
import logging
import os
from urllib.parse import quote_plus, unquote

from bs4 import BeautifulSoup

from crawler.store.models import Product, Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)


class StridonCrawler(BaseCrawler):
    """Crawler for Stridon store prices."""

    CHAIN = "stridon"
    BASE_URL = "https://stridon.hr/hr/supermarketi"

    PRICE_MAP = {
        "price": ("MALOPRODAJNA_CIJENA", False),
        "unit_price": ("CIJENA_ZA_JEDINICU_MJERE", False),
        "special_price": ("MPC_POSEBNI_OBLIK_PRODAJE", False),
        "best_price_30": ("NAJNIZA_CIJENA_ZADNJI_30_DANA", False),
        "anchor_price": ("SIDRENA_CIJENA_02_05_25", False),
    }

    FIELD_MAP = {
        "product": ("NAZIV_PROIZVODA", True),
        "product_id": ("SIFRA_PROIZVODA", True),
        "brand": ("MARKA", False),
        "quantity": ("NETO_KOLICINA", False),
        "unit": ("JEDINICA_MJERE", False),
        "barcode": ("EAN", False),
        "category": ("KATEGORIJA", False),
    }

    def parse_index(self, content: str) -> list[str]:
        """Extract CSV URLs from a Stridon index or archive page."""
        soup = BeautifulSoup(content, "html.parser")
        urls = []
        for link_tag in soup.select('a[href$=".csv"]'):
            href = str(link_tag.get("href"))
            if href:
                urls.append(href)
        return list(dict.fromkeys(urls))

    def get_store_list(self) -> list[str]:
        """Return all `Prod.XX` location IDs from the archive dropdown."""
        content = self.fetch_text(self.BASE_URL)
        if not content:
            logger.warning(f"No content found at Stridon index URL: {self.BASE_URL}")
            return []

        soup = BeautifulSoup(content, "html.parser")
        stores = []
        for option in soup.select("option[value]"):
            value = str(option.get("value", "")).strip()
            if value.startswith("Prod."):
                stores.append(value)

        logger.info(f"Found {len(stores)} Stridon stores: {'; '.join(stores)}")
        return stores

    def get_historical_csv_for_date(
        self,
        store_id: str,
        target_date: datetime.date,
    ) -> str | None:
        """Find the CSV link for `store_id` on `target_date` from the archive page."""
        archive_url = f"{self.BASE_URL}?pageName=archeive&archive_file_name={quote_plus(store_id)}"
        logger.debug(f"Fetching Stridon archive page for {store_id}: {archive_url}")

        try:
            content = self.fetch_text(archive_url)
        except Exception as e:
            logger.error(
                f"Error fetching archive page for {store_id}: {e}", exc_info=True
            )
            return None

        if not content:
            logger.warning(f"No content found at archive URL: {archive_url}")
            return None

        target_suffix = f"_{target_date:%d%m%Y}.csv"
        for url in self.parse_index(content):
            if unquote(url).endswith(target_suffix):
                logger.info(
                    f"Found historical CSV for {store_id} on {target_date:%Y-%m-%d}: {url}"
                )
                return url

        logger.debug(
            f"No historical data found for {store_id} on {target_date:%Y-%m-%d}"
        )
        return None

    def parse_store_info(self, url: str) -> Store:
        """Extract Store metadata from a Stridon CSV URL.

        Filename format: `{location_id}_{market_type}_{address...}_{city}_{DDMMYYYY}.csv`
        where address may contain underscores.
        """
        filename = unquote(os.path.basename(url)).removesuffix(".csv")
        parts = filename.split("_")
        if len(parts) < 5:
            raise ValueError(f"Invalid Stridon CSV filename format: {filename}")

        location_id, market_type, *address_parts, city, datestr = parts
        try:
            datetime.datetime.strptime(datestr, "%d%m%Y")
        except ValueError as err:
            raise ValueError(
                f"Invalid date in Stridon CSV filename {filename}: {datestr}"
            ) from err

        return Store(
            chain=self.CHAIN,
            store_type=market_type.lower(),
            store_id=location_id,
            name=f"Stridon {city}",
            street_address=" ".join(address_parts),
            zipcode="",
            city=city,
            items=[],
        )

    def get_store_prices(self, csv_url: str) -> list[Product]:
        """Fetch and parse a Stridon CSV (windows-1250, semicolon-delimited)."""
        try:
            content = self.fetch_text(csv_url, encodings=["windows-1250"])
            return self.parse_csv(content, delimiter=";")
        except Exception as e:
            logger.error(
                f"Failed to get Stridon store prices from {csv_url}: {e}",
                exc_info=True,
            )
            return []

    def get_index(self, date: datetime.date) -> list[str]:
        """Get all Stridon CSV URLs for the given date."""
        today = datetime.date.today()

        if date == today:
            logger.info(f"Fetching current Stridon CSV files for {date:%Y-%m-%d}")
            content = self.fetch_text(self.BASE_URL)
            if not content:
                logger.warning(
                    f"No content found at Stridon index URL: {self.BASE_URL}"
                )
                return []
            urls = self.parse_index(content)
            if not urls:
                logger.warning("No Stridon CSV URLs found on index page")
            return urls

        logger.info(f"Fetching historical Stridon CSV files for {date:%Y-%m-%d}")
        stores = self.get_store_list()
        if not stores:
            logger.warning("No Stridon stores found in dropdown")
            return []

        urls = []
        for store_id in stores:
            csv_url = self.get_historical_csv_for_date(store_id, date)
            if csv_url:
                urls.append(csv_url)

        if not urls:
            raise ValueError(f"No Stridon stores found for date {date:%Y-%m-%d}")

        logger.info(
            f"Found {len(urls)} historical Stridon CSV files for {date:%Y-%m-%d}"
        )
        return urls

    def get_all_products(self, date: datetime.date) -> list[Store]:
        csv_links = self.get_index(date)
        if not csv_links:
            logger.warning(f"No Stridon CSV links found for date {date:%Y-%m-%d}")
            return []

        stores = []
        for url in csv_links:
            try:
                store = self.parse_store_info(url)
                products = self.get_store_prices(url)
            except ValueError as ve:
                logger.error(
                    f"Skipping Stridon store due to parsing error from URL {url}: {ve}",
                    exc_info=False,
                )
                continue
            except Exception as e:
                logger.error(
                    f"Error processing Stridon store from {url}: {e}", exc_info=True
                )
                continue

            if not products:
                logger.warning(
                    f"No products found for Stridon store at {url}, skipping."
                )
                continue

            store.items = products
            stores.append(store)

        return stores

    # Lengths of legitimate retail barcode formats we accept in the EAN column.
    # Anything else (short internal SKUs, store-pack codes, non-numeric junk) is
    # cleared so the base class synthesizes a `stridon:<product_id>` fallback.
    _VALID_BARCODE_LENGTHS = (8, 11, 12, 13, 14)

    def fix_product_data(self, data: dict) -> dict:
        # Stridon prefixes product IDs with an Excel "force string" apostrophe (e.g. `'000237`).
        if data.get("product_id"):
            data["product_id"] = data["product_id"].lstrip("'")

        barcode = data.get("barcode", "") or ""
        if (
            not barcode.isdigit()
            or len(barcode) not in self._VALID_BARCODE_LENGTHS
            or barcode == data.get("product_id")
        ):
            data["barcode"] = ""

        return super().fix_product_data(data)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = StridonCrawler()
    stores = crawler.crawl(datetime.date.today())
    print(stores[0])
    print(stores[0].items[0])
