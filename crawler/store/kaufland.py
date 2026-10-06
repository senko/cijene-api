import datetime
import logging
import re
from typing import List
from json import loads

from bs4 import BeautifulSoup
from crawler.store.models import Product, Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)


class KauflandCrawler(BaseCrawler):
    """Crawler for Kaufland store prices."""

    CHAIN = "kaufland"
    BASE_URL = "https://www.kaufland.hr"
    INDEX_URL = f"{BASE_URL}/akcije-novosti/popis-mpc.html"

    # Mapping for price fields
    PRICE_MAP = {
        # field: (column, is_required)
        "price": ("maloprod.cijena(EUR)", False),
        "unit_price": ("cijena jed.mj.(EUR)", False),
        "special_price": ("MPC poseb.oblik prod", False),
        "best_price_30": ("Najniža MPC u 30dana", False),
        "anchor_price": ("Sidrena cijena", False),
    }

    # Mapping for other fields
    FIELD_MAP = {
        "product": ("naziv proizvoda", True),
        "product_id": ("šifra proizvoda", True),
        "brand": ("marka proizvoda", False),
        "quantity": ("neto količina(KG)", False),
        "unit": ("jedinica mjere", False),
        "barcode": ("barkod", False),
        "category": ("kategorija proizvoda", False),
    }

    BOOL_MAP = {
        "available": ("Dostupan/Nedostupan", False),
    }

    REQUIRED_COLUMNS = [
        "maloprod.cijena(EUR)",
        "cijena jed.mj.(EUR)",
        "Sidrena cijena",
        "naziv proizvoda",
        "šifra proizvoda",
        "marka proizvoda",
        "jedinica mjere",
        "barkod",
    ]

    # Availability was added on 2026-09-24 for NN 101/2026. The chain's promo
    # marker stays the pre-existing "akc.cijena, A=akcija" flag, which carries
    # no name for the form of sale, so special_sale_type is left unmapped.
    OPTIONAL_COLUMNS = [
        "neto količina(KG)",
        "kategorija proizvoda",
        "MPC poseb.oblik prod",
        "Najniža MPC u 30dana",
        "Dostupan/Nedostupan",
    ]

    CITIES = [
        "Zagreb Blato",
        "Zagreb",
        "Karlovac",
        "Velika Gorica",
        "Zapresic",
        "Zadar",
        "Cakovec",
        "Đakovo",
        "Sisak",
        "Koprivnica",
        "Slavonski Brod",
        "Nova Gradiska",
        "Sinj",
        "Rovinj",
        "Osijek",
        "Virovitica",
        "Biograd",
        "Dugo Selo",
        "Sibenik",
        "Pula",
        "Porec",
        "Makarska",
        "Kutina",
        "Split",
        "Vinkovci",
        "Rijeka",
        "Bjelovar",
        "Ivanec",
        "Trogir",
        "Umag",
        "Vukovar",
        "Zabok",
        "Cibaca",
        "Pozega",
        "Dakovo",
        "Vodice",
        "Varazdin",
        "Samobor",
    ]

    # Pattern to parse store information from filename
    # Format: Supermarket_Put_Gaceleza_1D_Vodice_6730_15_05_2025_7_30.csv
    ADDRESS_PATTERN = re.compile(r"(Supermarket|Hipermarket)_(.+?)_(\d{4})_")

    def get_index(self, date: datetime.date) -> dict[str, str]:
        """
        Get all CSV links from the Kaufland index page.

        Args:
            date: Date to get prices for

        Returns:
            Dictionary with title → URL mappings for CSV files.
        """

        # 0. Fetch the Kaufland index page

        content = self.fetch_text(self.INDEX_URL)
        if not content:
            raise ValueError("Failed to fetch Kaufland index page")

        soup = BeautifulSoup(content, "html.parser")

        # 1. Locate the Vue AssetList component
        list_el = soup.select_one("div[data-component=AssetList]")
        if not list_el:
            raise ValueError("Failed to find CSV links in Kaufland index page")

        # 2. Extract the AssetList component settings from a prop attrib
        vue_props = loads(str(list_el.get("data-props")))

        json_url = self.BASE_URL + vue_props.get("settings", {}).get("dataUrlAssets")
        if not json_url:
            raise ValueError("Failed to find JSON URL in Kaufland index page")

        # 3. Fetch the JSON data from the URL
        logger.debug(f"Fetching JSON data from {json_url}")
        json_content = self.fetch_text(json_url)
        if not json_content:
            raise ValueError("Failed to fetch JSON data from Kaufland index page")

        # 4. Parse the JSON data to extract CSV URLs
        json_data = loads(json_content)

        urls = {}
        date_str = date.strftime("_%d_%m_%Y_")
        date_str2 = date.strftime("_%d%m%Y_")
        for item in json_data:
            label = item.get("label")
            url = item.get("path")
            if not label or not url:
                continue
            if date_str not in label and date_str2 not in label:
                continue
            urls[label] = f"{self.BASE_URL}{url}"

        return urls

    def parse_store_info(self, title: str) -> Store:
        """
        Extract store information from the CSV title.

        Args:
            title: Title of the CSV file

        Returns:
            Store object with parsed information
        """
        # Format example: Supermarket_Put_Gaceleza_1D_Vodice_6730_15_05_2025_7_30.csv
        match = self.ADDRESS_PATTERN.search(title)
        if not match:
            raise ValueError(f"Could not parse store info from filename: {title}")

        store_type, address_part, store_id = match.groups()

        store_type = store_type.lower()
        street_address = address_part.replace("_", " ").title()
        city = ""

        # Look for cities in the address
        for city_name in self.CITIES:
            if self.strip_diacritics(street_address).endswith(city_name):
                city = city_name
                street_address = street_address[: -len(city_name)].strip()
                break

        # Create store object
        store = Store(
            chain=self.CHAIN,
            store_type=store_type,
            store_id=store_id,
            name=f"{self.CHAIN.capitalize()} {city}",
            street_address=street_address,
            city=city,
            zipcode="",
            items=[],
        )

        logger.info(
            f"Parsed store: {store.store_type} ({store.store_id}), {store.street_address}, {store.city}"
        )
        return store

    def get_store_prices(self, csv_url: str) -> List[Product]:
        """
        Get and parse prices from a store's CSV file.

        Args:
            csv_url: URL of the CSV file

        Returns:
            List of Product objects
        """
        try:
            content = self.fetch_text(
                csv_url, encodings=["utf-8-sig", "windows-1250", "utf-8"]
            )
            return self.parse_csv(content, delimiter="\t")
        except Exception as e:
            logger.error(
                f"Failed to get store prices from {csv_url}: {e}",
                exc_info=True,
            )
            return []

    def parse_csv(self, content: str, delimiter: str = ",") -> list[Product]:
        # Some Kaufland stores use "WG" instead of "kategorija proizvoda"
        # as the column header. Normalize before parsing.
        lines = content.split("\n")
        if lines and "kategorija proizvoda" not in lines[0] and "\tWG" in lines[0]:
            lines[0] = lines[0].replace("\tWG", "\tkategorija proizvoda")
            content = "\n".join(lines)
        return super().parse_csv(content, delimiter)

    def parse_csv_row(self, row: dict) -> Product:
        # "Sidrena cijena" (anchor price) contains an encoded date + price.
        # Variants seen in the wild:
        #   "MPC 2.5.2025=7,99€"      — standard format
        #   "MPC28.10.2025=3,99€"      — no space after MPC
        #   "MPC 9.9.2025 = 6,49"      — spaces around =, no € suffix
        #   "MPC 2.5.2025.=9,49€"      — extra trailing dot in date
        #   "MPC18.10..2025=5,59€"     — double dot in date
        #   "MPC 05082025=40,21€"      — no dots in date (ddmmYYYY)
        #   "MPC 26.09.205=3,39€"      — truncated year (unparseable, price still captured)
        #   "500 g", "Miss Dream"       — garbage from shifted columns (discarded)
        anchor_price = row.get("Sidrena cijena", "").strip()
        anchor_date = ""

        if anchor_price and anchor_price.startswith("MPC") and "=" in anchor_price:
            # Split into date part and price part on "="
            date_part, price_str = anchor_price.split("=", 1)
            date_part = date_part[3:].strip()  # Remove "MPC" prefix
            price_str = price_str.strip().rstrip("€").strip()

            # Always use the price if we got one
            row["Sidrena cijena"] = price_str

            # Try to parse the date (best-effort)
            anchor_date = self._parse_anchor_date(date_part)
        elif anchor_price and not anchor_price.startswith("MPC"):
            # Garbage value (shifted column data) — discard
            row["Sidrena cijena"] = ""

        product = super().parse_csv_row(row)

        if anchor_date:
            product.anchor_price_date = anchor_date

        return product

    def _parse_anchor_date(self, date_part: str) -> str:
        """Try to parse a date from the anchor price date string. Returns ISO date or ''."""
        # Clean up common noise: trailing dots, double dots
        date_part = date_part.rstrip(".")
        date_part = date_part.replace("..", ".")

        for fmt in ("%d.%m.%Y", "%d.%m.%y", "%d%m%Y"):
            try:
                return (
                    datetime.datetime.strptime(date_part, fmt)
                    .date()
                    .strftime("%Y-%m-%d")
                )
            except ValueError:
                continue

        return ""

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Main method to fetch and parse all store, product and price info.

        Args:
            date: The date to search for in the price list.

        Returns:
            List of Store objects with their products.
        """
        csv_links = self.get_index(date)
        stores = []

        for title, url in csv_links.items():
            try:
                store = self.parse_store_info(title)
                products = self.get_store_prices(url)
            except Exception as e:
                logger.error(f"Error processing store from {url}: {e}", exc_info=True)
                continue

            if not products:
                logger.warning(f"No products found for {url}, skipping")
                continue

            store.items = products
            stores.append(store)

        return stores


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = KauflandCrawler()
    stores = crawler.crawl(datetime.date.today() - datetime.timedelta(days=1))
    print(stores[0])
    print(stores[0].items[0])
