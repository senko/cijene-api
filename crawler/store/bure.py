import datetime
import logging
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from lxml import etree  # type: ignore

from crawler.store.models import Product, Store

from .base import BaseCrawler

logger = logging.getLogger(__name__)


# Hardcoded store metadata keyed by <Oznaka> code from the XML.
# The XML's <Adresa> field is uppercase and lacks postal codes, so we resolve
# clean metadata from this map. Source: https://www.bure.hr/cash-carry-centri
STORES = {
    "06": {
        "name": "Bure Murter",
        "street_address": "Butina 22",
        "zipcode": "22243",
        "city": "Murter",
    },
    "08": {
        "name": "Bure Ugljan",
        "street_address": "Fortoška ul. 4",
        "zipcode": "23275",
        "city": "Ugljan",
    },
    "20": {
        "name": "Bure Biograd",
        "street_address": "Odranska 15",
        "zipcode": "23210",
        "city": "Biograd na Moru",
    },
}


class BureCrawler(BaseCrawler):
    """
    Crawler for Bure d.o.o. (cash & carry wholesale, Zadar region).

    Bure publishes daily pricelists at /cjenici-arhiva as a paginated HTML
    table. Each day's row contains per-store XML links and a bundled ZIP.
    The crawler downloads the ZIP (1 request) and parses each XML inside,
    resolving store metadata from the hardcoded STORES map keyed on the
    <Oznaka> store code.
    """

    CHAIN = "bure"
    BASE_URL = "https://www.bure.hr"
    INDEX_URL = "https://www.bure.hr/cjenici-arhiva"

    # All prices are non-required: products on special often have an empty
    # <MaloprodajnaCijena>, and BaseCrawler.fix_product_data falls back to
    # special_price / unit_price.
    PRICE_MAP = {
        "price": ("MaloprodajnaCijena", False),
        "unit_price": ("CijenaZaJedinicuMjere", False),
        "special_price": ("MaloprodajnaCijenaAkcija", False),
        "best_price_30": ("NajnizaCijena", False),
        "anchor_price": ("SidrenaCijena", False),
    }

    FIELD_MAP = {
        "product": ("NazivProizvoda", True),
        "product_id": ("SifraProizvoda", True),
        "brand": ("MarkaProizvoda", False),
        "quantity": ("NetoKolicina", False),
        "unit": ("JedinicaMjere", False),
        "barcode": ("Barkod", False),
        "category": ("KategorijeProizvoda", False),
    }

    def get_zip_url(self, date: datetime.date) -> str:
        """
        Find the bundled ZIP URL for the given date from the archive index.

        The archive paginates older dates as ?page=N. Iterates pages until
        the row is found, the page is empty, or no row on the page is older
        than the target (target is older than archive retention).

        Args:
            date: Target date to find the pricelist for.

        Returns:
            Absolute URL of the /preuzmi-zip endpoint for the given date.

        Raises:
            ValueError: If no row is found for the target date in the archive.
        """
        date_str = f"{date:%d.%m.%Y}"

        page = 1
        while True:
            url = self.INDEX_URL if page == 1 else f"{self.INDEX_URL}?page={page}"
            content = self.fetch_text(url)
            soup = BeautifulSoup(content, "html.parser")

            rows = soup.select("tr.pricelist-row[data-date]")
            if not rows:
                raise ValueError(
                    f"No Bure pricelist row found for date {date_str} "
                    f"(reached empty page {page})"
                )

            for row in rows:
                row_date_str = str(row.get("data-date", ""))
                if row_date_str == date_str:
                    zip_link = row.select_one('a[href$="/preuzmi-zip"]')
                    if zip_link is None or not zip_link.get("href"):
                        raise ValueError(
                            f"Bure row for {date_str} has no /preuzmi-zip link"
                        )
                    return urljoin(self.INDEX_URL, str(zip_link["href"]))

            # Rows are sorted newest first. If the oldest row on this page is
            # still newer than the target, the target may be on a later page.
            # If it's already older, the target is past archive retention.
            try:
                oldest = datetime.datetime.strptime(
                    str(rows[-1].get("data-date", "")), "%d.%m.%Y"
                ).date()
            except ValueError:
                raise ValueError(
                    f"Bure archive page {page} has unparseable data-date "
                    f"on its last row"
                )

            if oldest < date:
                raise ValueError(
                    f"Bure pricelist for date {date_str} not in archive "
                    f"(oldest available: {oldest:%d.%m.%Y})"
                )

            page += 1

    def build_store(self, root: etree._Element) -> Store:
        """
        Build a Store object from the <ProdajniObjekt> block in the XML.

        Resolves address/zipcode/city from the hardcoded STORES map, since the
        XML's <Adresa> is uppercase and lacks the postal code.

        Args:
            root: Parsed XML root element (<Proizvodi>).

        Returns:
            Store with empty items list.

        Raises:
            ValueError: If <ProdajniObjekt> or <Oznaka> is missing.
        """
        store_elem = root.find(".//ProdajniObjekt")
        if store_elem is None:
            raise ValueError("No <ProdajniObjekt> element found in XML")

        oznaka_elem = store_elem.find("Oznaka")
        store_id = (
            oznaka_elem.text.strip()
            if oznaka_elem is not None and oznaka_elem.text
            else ""
        )
        if not store_id:
            raise ValueError("Missing <Oznaka> in <ProdajniObjekt>")

        oblik_elem = store_elem.find("Oblik")
        oblik = (
            oblik_elem.text.strip()
            if oblik_elem is not None and oblik_elem.text
            else ""
        )
        # "CASH&CARRY" -> "cash_and_carry"
        store_type = (
            oblik.lower().replace("&", "_and_").replace(" ", "_")
            if oblik
            else "cash_and_carry"
        )

        info = STORES.get(store_id)
        if info is None:
            adresa_elem = store_elem.find("Adresa")
            adresa = (
                adresa_elem.text.strip()
                if adresa_elem is not None and adresa_elem.text
                else ""
            )
            logger.warning(
                f"Unknown Bure store_id {store_id!r} (Adresa: {adresa!r}), "
                f"add it to the STORES map in bure.py"
            )
            info = {
                "name": f"Bure {store_id}",
                "street_address": adresa.title(),
                "zipcode": "",
                "city": "",
            }

        return Store(
            chain=self.CHAIN,
            store_id=store_id,
            store_type=store_type,
            name=info["name"],
            street_address=info["street_address"],
            zipcode=info["zipcode"],
            city=info["city"],
            items=[],
        )

    def parse_xml(self, xml_content: bytes) -> tuple[Store, list[Product]]:
        """
        Parse one Bure XML file into store info and a list of products.

        Args:
            xml_content: Raw XML file content as bytes.

        Returns:
            Tuple of (Store, list of Products).
        """
        root = etree.fromstring(xml_content)
        store = self.build_store(root)

        products = []
        for product_elem in root.xpath("//Proizvod"):
            try:
                product = self.parse_xml_product(product_elem)
            except Exception as e:
                logger.warning(
                    f"Failed to parse Bure product: "
                    f"{etree.tostring(product_elem, encoding='unicode')}: {e}"
                )
                continue
            products.append(product)

        logger.debug(f"Parsed {len(products)} products for store {store.store_id}")
        return store, products

    def fix_product_data(self, data):
        """
        Bure's <Barkod> tag is consistently empty, but <SifraProizvoda> holds
        a numeric EAN-shaped code. Promote it to barcode when it looks like
        an EAN (8/12/13/14 digits) so cross-chain product matching works.
        """
        if not data.get("barcode"):
            pid = data.get("product_id", "")
            if pid.isdigit() and len(pid) in (8, 12, 13, 14):
                data["barcode"] = pid
        return super().fix_product_data(data)

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Fetch the bundled ZIP for the given date and parse all 3 store XMLs.

        Args:
            date: The date to fetch the pricelist for.

        Returns:
            List of Store objects, each populated with its products.
        """
        zip_url = self.get_zip_url(date)
        logger.info(f"Found Bure ZIP for {date}: {zip_url}")

        stores = []
        for filename, content in self.get_zip_contents(zip_url, ".xml"):
            try:
                store, products = self.parse_xml(content)
            except Exception as e:
                logger.error(f"Failed to parse Bure XML {filename}: {e}", exc_info=True)
                continue

            if not products:
                logger.warning(
                    f"No products parsed from Bure XML {filename}, skipping store"
                )
                continue

            store.items = products
            stores.append(store)

        return stores


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = BureCrawler()
    stores = crawler.crawl(datetime.date.today())
    print(stores[0])
    print(stores[0].items[0])
