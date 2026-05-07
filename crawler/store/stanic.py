import datetime
import json
import logging
import os
import re
from io import BytesIO
from tempfile import TemporaryFile
from typing import Any
from urllib.parse import quote

import openpyxl
from dotenv import load_dotenv

from crawler.store.models import Product, Store

from .base import BaseCrawler

# Crawler is invoked outside service/, so .env isn't auto-loaded — do it here.
load_dotenv()

logger = logging.getLogger(__name__)


def cell_str(val: Any) -> str:
    """Convert an XLSX cell value to string, treating None as empty."""
    return "" if val is None else str(val).strip()


# Filename pattern from the Drive folder, e.g.
#   MP_131_DISKONT ŽITNJAK 1_01-05-2026-050002.xlsx
FILENAME_PATTERN = re.compile(
    r"^MP_(?P<store_id>\d+)_(?P<store_name>.+?)_"
    r"(?P<day>\d{2})-(?P<month>\d{2})-(?P<year>\d{4})-\d+\.xlsx$"
)

FOLDER_MIME = "application/vnd.google-apps.folder"


# Pre-populated from the file listing. Add street_address / zipcode as
# they become known (currently only city is reliably derivable from the
# store name).
STORES: dict[str, dict[str, str]] = {
    "131": {"name": "Diskont Žitnjak 1", "city": "Zagreb"},
    "133": {"name": "Diskont Veletržnica", "city": "Zagreb"},
    "134": {"name": "Diskont Samoborska", "city": "Zagreb"},
    "136": {"name": "Diskont Grabovac", "city": "Grabovac"},
    "140": {"name": "Diskont Karlovac 3", "city": "Karlovac"},
    "146": {"name": "Diskont Dubrava", "city": "Zagreb"},
    "147": {"name": "Diskont Koprivnica", "city": "Koprivnica"},
    "149": {"name": "Diskont Bjelovar 2", "city": "Bjelovar"},
    "164": {"name": "Diskont Čakovec", "city": "Čakovec"},
    "165": {"name": "Diskont Sesvete", "city": "Sesvete"},
    "431": {"name": "Diskont Opatija", "city": "Opatija"},
    "432": {"name": "Diskont Rijeka", "city": "Rijeka"},
    "533": {"name": "Diskont Sl.Brod 2", "city": "Slavonski Brod"},
    "536": {"name": "Diskont Vinkovci", "city": "Vinkovci"},
    "538": {"name": "Diskont Požega", "city": "Požega"},
    "731": {"name": "Diskont Poreč", "city": "Poreč"},
    "732": {"name": "Diskont Vodnjan", "city": "Vodnjan"},
    "733": {"name": "Diskont Umag", "city": "Umag"},
    "734": {"name": "Diskont Pula", "city": "Pula"},
    "739": {"name": "Diskont Pula 4", "city": "Pula"},
    "835": {"name": "Diskont Split", "city": "Split"},
    "837": {"name": "Diskont Šibenik", "city": "Šibenik"},
    "863": {"name": "Diskont Dubrovnik", "city": "Dubrovnik"},
    "932": {"name": "Diskont Vodice", "city": "Vodice"},
    "933": {"name": "Diskont Zadar 2", "city": "Zadar"},
    "935": {"name": "Diskont Biograd", "city": "Biograd"},
    "1301": {
        "name": "Maloprodaja pokretna trgovina",
        "city": "",
        "store_type": "pokretna trgovina",
    },
    "7302": {"name": "Diskont Medulin 2", "city": "Medulin"},
    "7307": {"name": "Diskont Rovinj", "city": "Rovinj"},
    "7308": {"name": "Diskont Labin 2", "city": "Labin"},
    "8301": {"name": "Diskont Stari Grad", "city": "Stari Grad"},
    "8303": {"name": "Diskont Trogir", "city": "Trogir"},
    "8307": {"name": "Diskont Kaštel Gomilica", "city": "Kaštel Gomilica"},
}


class StanicCrawler(BaseCrawler):
    """
    Crawler for Diskont Stanić store prices.

    Stanić publishes daily XLSX pricelists (one file per store per day) in a
    public Google Drive folder. The current day's files live at the folder
    root; older days are moved into a `dd.mm.yyyy`-named subfolder. The
    crawler lists the folder via the Drive v3 REST API (using GOOGLE_API_KEY)
    and downloads each store's XLSX as raw bytes.
    """

    CHAIN = "stanic"
    BASE_URL = "https://drive.google.com"

    DRIVE_API = "https://www.googleapis.com/drive/v3"
    FOLDER_ID = "1dY49AwffUEfDjuBjRxGPE9Tvyq_gQblE"

    DEFAULT_STORE_TYPE = "diskont"

    PRICE_MAP = {
        "price": ("MP cijena", True),
        "unit_price": ("Cijena za jedinicu mjere", False),
        "special_price": ("MPC u vrij. pos. obl. prodaje", False),
        "anchor_price": ("Sidrena cijena na dan", False),
        "best_price_30": ("Najniža cijena 30 dana", False),
    }

    FIELD_MAP = {
        "product": ("Naziv", True),
        "product_id": ("Šifra", True),
        "barcode": ("Barkod", False),
        "brand": ("Marka", False),
        "quantity": ("Neto količina", False),
        "unit": ("Jedinica mjere", False),
        "category": ("Kategorija proizvoda", False),
    }

    def _list_drive_folder(self, folder_id: str, api_key: str) -> list[dict]:
        """List immediate children of a Drive folder via the v3 REST API."""
        q = quote(f"'{folder_id}' in parents and trashed=false", safe="")
        fields = quote("files(id,name,mimeType)", safe="")
        url = (
            f"{self.DRIVE_API}/files?q={q}&key={api_key}&fields={fields}&pageSize=1000"
        )
        body = self.fetch_text(url)
        data = json.loads(body)
        return data.get("files", [])

    @staticmethod
    def _date_folder_names(date: datetime.date) -> set[str]:
        """Names a Drive folder may use for a given date (zero-padded or not)."""
        return {
            date.strftime("%d.%m.%Y"),
            f"{date.day}.{date.month}.{date.year}",
        }

    def _find_date_subfolder(
        self, entries: list[dict], date: datetime.date
    ) -> str | None:
        """Find a subfolder named like the target date (e.g. '30.04.2026' or '1.5.2026')."""
        candidates = self._date_folder_names(date)
        for entry in entries:
            if entry.get("mimeType") != FOLDER_MIME:
                continue
            if entry.get("name") in candidates:
                return entry["id"]
        return None

    def _filter_files_for_date(
        self, entries: list[dict], date: datetime.date
    ) -> list[tuple[str, str, str]]:
        """
        Filter file entries to those matching the target date by filename.

        If the same store appears more than once, the lexicographically
        latest filename wins (timestamp suffix is HHMMSS, sorts correctly).

        Returns:
            List of (file_id, store_id, store_name) tuples.
        """
        per_store: dict[str, tuple[str, str, str]] = {}
        for entry in entries:
            if entry.get("mimeType") == FOLDER_MIME:
                continue
            name = entry.get("name", "")
            m = FILENAME_PATTERN.match(name)
            if not m:
                continue
            file_date = datetime.date(
                int(m.group("year")), int(m.group("month")), int(m.group("day"))
            )
            if file_date != date:
                continue
            store_id = m.group("store_id")
            store_name = m.group("store_name").strip()
            existing = per_store.get(store_id)
            if existing is None or name > existing[0]:
                per_store[store_id] = (name, entry["id"], store_name)

        return [
            (file_id, store_id, store_name)
            for store_id, (_name, file_id, store_name) in per_store.items()
        ]

    def _collect_file_entries(self, date: datetime.date, api_key: str) -> list[dict]:
        """
        List Drive entries that may contain price files for the given date.

        Lookup order:
          1. Files at the folder root (today's and most recent days).
          2. A ``dd.mm.yyyy`` subfolder of the root (recent older days).
          3. A ``dd.mm.yyyy`` subfolder one level deep, inside any
             non-date-named root subfolder (e.g. ``Arhiva``). This covers
             the bulk of historical data.
        """
        root = self._list_drive_folder(self.FOLDER_ID, api_key)
        if any(
            self._matches_date(e.get("name", ""), date)
            for e in root
            if e.get("mimeType") != FOLDER_MIME
        ):
            return root

        sub_id = self._find_date_subfolder(root, date)
        if sub_id is not None:
            logger.info(f"Found {date} in root subfolder")
            return self._list_drive_folder(sub_id, api_key)

        date_names = self._date_folder_names(date)
        for entry in root:
            if entry.get("mimeType") != FOLDER_MIME:
                continue
            if entry.get("name") in date_names:
                continue  # already handled above
            logger.debug(f"Searching {entry['name']!r} for {date}")
            nested = self._list_drive_folder(entry["id"], api_key)
            nested_id = self._find_date_subfolder(nested, date)
            if nested_id is not None:
                logger.info(f"Found {date} in {entry['name']!r} subfolder")
                return self._list_drive_folder(nested_id, api_key)

        return []

    @staticmethod
    def _matches_date(filename: str, date: datetime.date) -> bool:
        m = FILENAME_PATTERN.match(filename)
        if not m:
            return False
        return (
            datetime.date(
                int(m.group("year")), int(m.group("month")), int(m.group("day"))
            )
            == date
        )

    def _download_file(self, file_id: str, fp) -> None:
        """
        Download a Drive file's bytes to the provided BinaryIO.

        Uses the public ``drive.google.com/uc?export=download`` endpoint
        rather than the Drive API ``alt=media`` path. Both require the
        file to be readable by "anyone with the link", but the public
        endpoint behaves more consistently for files inherited from a
        shared parent folder — the API path 403s on some such files even
        when the listing succeeds. No API key is needed for this URL.
        """
        url = f"{self.BASE_URL}/uc?export=download&id={file_id}"
        self.fetch_binary(url, fp)

    def _make_store(self, store_id: str, fallback_name: str) -> Store:
        info = STORES.get(store_id)
        if info is None:
            logger.warning(
                f"Unknown store_id '{store_id}' ({fallback_name}), "
                f"add it to the STORES map in stanic.py"
            )
            info = {}

        return Store(
            chain=self.CHAIN,
            store_id=store_id,
            name=info.get("name") or fallback_name.title(),
            store_type=info.get("store_type") or self.DEFAULT_STORE_TYPE,
            city=info.get("city", ""),
            street_address=info.get("street_address", ""),
            zipcode=info.get("zipcode", ""),
        )

    def parse_excel(self, excel_data: bytes) -> list[Product]:
        """
        Parse a Stanić XLSX into Product objects.

        The sheet has a single header row with column names matching
        PRICE_MAP / FIELD_MAP keys; subsequent rows carry one product each.
        Each row is converted to a `{header: stringified_value}` dict and
        handed to `BaseCrawler.parse_csv_row`, which applies the maps.
        """
        wb = openpyxl.load_workbook(BytesIO(excel_data), data_only=True)
        ws = wb.active
        if ws is None:
            raise ValueError("No active worksheet found in the Excel file")

        rows = ws.iter_rows(values_only=True)
        try:
            header_row = next(rows)
        except StopIteration:
            raise ValueError("XLSX file is empty")

        columns = [cell_str(c) for c in header_row]
        required = [c for c, _ in self.PRICE_MAP.values()] + [
            c for c, _ in self.FIELD_MAP.values()
        ]
        missing = [c for c in required if c not in columns]
        if missing:
            raise ValueError(
                f"Missing expected XLSX columns: {missing}. Got: {columns}"
            )

        products: list[Product] = []
        for row_idx, row in enumerate(rows, start=2):
            if row is None or all(c is None for c in row):
                continue
            row_dict = {col: cell_str(val) for col, val in zip(columns, row)}
            if not row_dict.get("Šifra") and not row_dict.get("Naziv"):
                continue
            try:
                products.append(self.parse_csv_row(row_dict))
            except Exception as e:
                row_txt = "; ".join(cell_str(v) for v in row)
                logger.warning(f"Failed to parse row {row_idx}: `{row_txt}`: {e}")
                continue

        logger.debug(f"Parsed {len(products)} products from XLSX")
        return products

    def get_all_products(self, date: datetime.date) -> list[Store]:
        api_key = os.getenv("GOOGLE_API_KEY", "").strip()
        if not api_key:
            logger.warning("No Google API key found for Google Drive access")
            return []

        entries = self._collect_file_entries(date, api_key)
        matches = self._filter_files_for_date(entries, date)
        if not matches:
            logger.info(f"No price lists found for {date}")
            return []

        logger.info(f"Found {len(matches)} XLSX files for {date}")

        stores: list[Store] = []
        for file_id, store_id, store_name in matches:
            try:
                with TemporaryFile(mode="w+b") as tf:
                    self._download_file(file_id, tf)
                    tf.seek(0)
                    excel_data = tf.read()
                store = self._make_store(store_id, store_name)
                store.items = self.parse_excel(excel_data)
            except Exception as e:
                logger.error(
                    f"Failed to fetch/parse XLSX for store {store_id}: {e}",
                    exc_info=True,
                )
                continue
            if not store.items:
                logger.warning(f"No products parsed for store {store_id}")
                continue
            stores.append(store)

        return stores


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = StanicCrawler()
    stores = crawler.crawl(datetime.date.today())
    print(f"Found {len(stores)} stores")
    if stores and stores[0].items:
        print(stores[0])
        print(stores[0].items[0])
