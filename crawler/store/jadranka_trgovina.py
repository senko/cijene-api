import datetime
import logging
import re

from bs4 import BeautifulSoup
from crawler.store.models import Product, Store

from .base import BaseCrawler, CrawlerBlocked

logger = logging.getLogger(__name__)

# Store info keyed by the store ID in the file name. The file names spell the
# address without spaces or diacritics ("BRACEVIDULICA20", "MALILOSINJ").
STORES = {
    "607": ("Market Maxi", "Dražica 5", "Mali Lošinj"),
    "501": ("Market Centar", "Braće Vidulića 20", "Mali Lošinj"),
    "531": ("Market Zagazine", "Trg Zagazinjine 1", "Mali Lošinj"),
    "504": ("Market Škverić", "Lošinjskih brodograditelja 14", "Mali Lošinj"),
    "201": ("Market Čikat", "Čikat 6a", "Mali Lošinj"),
    "558": ("Market Priko", "Priko 46", "Mali Lošinj"),
    "503": ("Market Veli Lošinj", "Vladimira Nazora 25", "Veli Lošinj"),
    "540": ("Market Riva", "Obala maršala Tita 4", "Veli Lošinj"),
    "507": ("Market Nerezine", "Nerezinskih pomoraca 22a", "Nerezine"),
    "539": ("Market Bučanje", "Bučanje 115", "Nerezine"),
    "233": ("Prodavaonica Punta Križa", "Punta Križa 20a", "Punta Križa"),
    "234": ("Market Baldarin", "Bokinić 75", "Punta Križa"),
    "213": ("Market Slatina", "Martinšćica 92", "Martinšćica"),
    "051": ("Market Zaglav", "Zaglav 31", "Martinšćica"),
    "510": ("Market Susak", "Susak 56", "Susak"),
    "511": ("Prodavaonica Osor", "Osor 48", "Osor"),
    "554": ("Market Bijar", "Osor bb", "Osor"),
    "513": ("Market Belej", "Belej 50", "Belej"),
    "508": ("Prodavaonica Ćunski", "Ćunski 42", "Ćunski"),
    "WEB": ("Webshop", "Dražica 5", "Mali Lošinj"),
}

# City as spelled in the file name, for stores not in STORES.
CITIES = {
    "MALILOSINJ": "Mali Lošinj",
    "VELILOSINJ": "Veli Lošinj",
    "NEREZINE": "Nerezine",
    "PUNTAKRIZA": "Punta Križa",
    "MARTINSCICA": "Martinšćica",
    "SUSAK": "Susak",
    "OSOR": "Osor",
    "BELEJ": "Belej",
    "CUNSKI": "Ćunski",
}

# File name prefix (type of point of sale) -> store type. Only markets,
# prodavaonice and the web shop are crawled.
STORE_TYPES = {
    "MARKET": "market",
    "PRODAVAONA": "prodavaonica",
    "WEBSHOP": "webshop",
}

# Other points of sale that publish price lists: cafés, pastry shops, a kiosk,
# a bakery, a gourmet shop and a home and garden shop.
SKIPPED_TYPES = (
    "KAFIC",
    "DULCINEA",
    "KIOSK",
    "PEKARA",
    "GOURMET_BOUTIQUE",
    "HOME_GARDEN",
)


class JadrankaTrgovinaCrawler(BaseCrawler):
    """
    Crawler for Jadranka Trgovina store prices.

    Jadranka Trgovina publishes one CSV per point of sale per day. File names:
    TYPE[_NAME]_ADDRESS_CITY_STOREID_FILEID_DDMMYYYY_HHMM.csv, e.g.
    MARKET_CENTAR_BRACEVIDULICA20_MALILOSINJ_501_132_17112026_0800.csv.

    Until 2026-11-16 only Market Maxi (607) published, without the file ID:
    MARKET_MAXI_DRAZICA5_MALILOSINJ_607_16112026_0800.csv.
    """

    CHAIN = "jadranka_trgovina"
    BASE_URL = "https://jadranka-trgovina.com"
    INDEX_URL = "https://jadranka-trgovina.com/cjenici/"

    FILENAME_DATE_PATTERN = re.compile(r"_(\d{2})(\d{2})(\d{4})_\d{4}\.csv$")

    # First day whose file names have a file ID after the store ID.
    FILE_ID_SINCE = datetime.date(2026, 11, 17)

    # Applies only to the old anchor column, whose header states the date.
    ANCHOR_PRICE_DATE = datetime.date(2025, 5, 2)

    OLD_ANCHOR_PRICE_COLUMN = "SIDRENA CIJENA NA 2.5.2025"
    NEW_ANCHOR_PRICE_COLUMN = "SIDRENA CIJENA"

    # Since 2026-11-17 the only price column is "MPC ZA VRIJEME POSEBNOG
    # OBLIKA PRODAJE". It is filled on every row and holds the shelf price,
    # so it is read as the price, not as a special price.
    PRICE_COLUMN = "MALOPRODAJNA CIJENA"
    SPECIAL_PRICE_COLUMN = "MPC ZA VRIJEME POSEBNOG OBLIKA PRODAJE"

    PRICE_MAP = {
        # field: (column_name, is_required)
        # Note: Many products have empty retail price but filled special price
        # Some products also have empty unit_price
        "price": (PRICE_COLUMN, False),
        "unit_price": ("CIJENA ZA JEDINICU MJERE", False),
        "special_price": (SPECIAL_PRICE_COLUMN, False),
        "best_price_30": ("NAJNIŽA CIJENA U POSLJEDNIH 30 DANA", False),
        "anchor_price": (
            (OLD_ANCHOR_PRICE_COLUMN, NEW_ANCHOR_PRICE_COLUMN),
            False,
        ),
    }

    FIELD_MAP = {
        "product_id": ("ŠIFRA PROIZVODA", True),
        "product": ("NAZIV PROIZVODA", True),
        "brand": ("MARKA PROIZVODA", False),
        "barcode": ("BARKOD", False),
        "category": ("KATEGORIJA PROIZVODA", False),
        "quantity": ("NETO KOLIČINA", False),
        "unit": ("JEDINICA MJERE", False),
    }

    BOOL_MAP = {
        "available": ("DOSTUPNOST", False),
    }

    REQUIRED_COLUMNS = [
        (PRICE_COLUMN, SPECIAL_PRICE_COLUMN),
        "CIJENA ZA JEDINICU MJERE",
        (OLD_ANCHOR_PRICE_COLUMN, NEW_ANCHOR_PRICE_COLUMN),
        "ŠIFRA PROIZVODA",
        "NAZIV PROIZVODA",
        "MARKA PROIZVODA",
        "BARKOD",
        "JEDINICA MJERE",
    ]

    # The first four aren't required by NN 101/2026, so the chain may drop
    # them. DOSTUPNOST was added on 2026-11-17; older files don't have it.
    OPTIONAL_COLUMNS = [
        "NETO KOLIČINA",
        "KATEGORIJA PROIZVODA",
        SPECIAL_PRICE_COLUMN,
        "NAJNIŽA CIJENA U POSLJEDNIH 30 DANA",
        "DOSTUPNOST",
    ]

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

    def get_index(self, date: datetime.date) -> list[str]:
        """
        Fetch the index page and find the CSV URLs for the specified date.

        Args:
            date: The date for which to fetch the price lists

        Returns:
            CSV URLs for the specified date
        """
        content = self.fetch_text(self.INDEX_URL)
        if not content:
            logger.warning(
                f"No content found at Jadranka Trgovina index URL: {self.INDEX_URL}"
            )
            return []

        urls = []
        for url in self.parse_index(content):
            m = self.FILENAME_DATE_PATTERN.search(url)
            if m and m.groups() == (
                f"{date.day:02d}",
                f"{date.month:02d}",
                str(date.year),
            ):
                urls.append(url)

        if not urls:
            logger.warning(f"No Jadranka Trgovina CSV found for date {date}")
        return urls

    def parse_store_info(self, csv_url: str) -> Store | None:
        """
        Get the store from a CSV file name.

        Args:
            csv_url: URL of the CSV file

        Returns:
            Store without products, or None for a point of sale that isn't
            a shop
        """
        filename = csv_url.rsplit("/", 1)[-1]
        prefix = next((t for t in STORE_TYPES if filename.startswith(f"{t}_")), None)
        if prefix is None:
            if not filename.startswith(SKIPPED_TYPES):
                logger.warning(f"Unknown Jadranka Trgovina point of sale: {filename}")
            return None

        # ..._ADDRESS_CITY_STOREID_FILEID_DATE_TIME.csv; until 2026-11-16 the
        # file ID was missing, so the token before the date was the store ID
        # and the one before that the city. Store IDs are numbers, except the
        # web shop's "WEB", so the file date tells the two formats apart.
        tokens = filename.removesuffix(".csv").split("_")
        file_date = datetime.datetime.strptime(tokens[-2], "%d%m%Y").date()
        idx = len(tokens) - (4 if file_date >= self.FILE_ID_SINCE else 3)
        store_id = tokens[idx]

        if store_id in STORES:
            name, street_address, city = STORES[store_id]
        else:
            logger.warning(
                f"Unknown Jadranka Trgovina store_id {store_id!r} ({filename}), "
                f"add it to the STORES map in jadranka_trgovina.py"
            )
            name = " ".join(tokens[: idx - 2]).title()
            street_address = tokens[idx - 2]
            city = CITIES.get(tokens[idx - 1], tokens[idx - 1].title())

        return Store(
            chain=self.CHAIN,
            store_id=store_id,
            name=f"Jadranka Trgovina {name}",
            store_type=STORE_TYPES[prefix],
            city=city,
            street_address=street_address,
            zipcode="",
            items=[],
        )

    def parse_csv(self, content: str, delimiter: str = ",") -> list[Product]:
        # A few product names contain the delimiter and aren't quoted, e.g.
        # "BUKET GERBER 30 CM; CRVENA;1104409;...". Quote the name so the
        # other columns don't shift.
        lines = content.splitlines()
        if lines:
            width = lines[0].count(delimiter)
            for i, line in enumerate(lines[1:], start=1):
                extra = line.count(delimiter) - width
                if extra > 0:
                    parts = line.split(delimiter)
                    name = delimiter.join(parts[: extra + 1]).replace('"', '""')
                    lines[i] = delimiter.join([f'"{name}"', *parts[extra + 1 :]])
            content = "\n".join(lines)

        return super().parse_csv(content, delimiter)

    def parse_csv_row(self, row: dict) -> Product:
        if self.PRICE_COLUMN not in row:
            row[self.PRICE_COLUMN] = row.pop(self.SPECIAL_PRICE_COLUMN, "")

        product = super().parse_csv_row(row)

        # The new anchor header states no date, so ANCHOR_PRICE_DATE doesn't
        # apply to it.
        if self.NEW_ANCHOR_PRICE_COLUMN in row:
            product.anchor_price_date = None

        return product

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
        except CrawlerBlocked:
            raise
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
            List of Store objects with products
        """
        stores = []

        for csv_url in self.get_index(date):
            store = self.parse_store_info(csv_url)
            if store is None:
                continue

            products = self.get_store_prices(csv_url)
            if not products:
                logger.warning(f"No products found in {csv_url}")
                continue

            store.items = products
            stores.append(store)

        logger.info(f"Jadranka Trgovina: {len(stores)} stores found")
        return stores


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
