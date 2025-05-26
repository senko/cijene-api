import csv
import datetime
import io
from json import loads
import logging
import re
from decimal import Decimal
from typing import List, Optional, Tuple


from crawler.store.base import BaseCrawler
from crawler.store.models import Product, Store
from crawler.store.utils import parse_price, to_camel_case

logger = logging.getLogger(__name__)


class TommyCrawler(BaseCrawler):
    """
    Crawler for Tommy store prices.

    This class handles downloading and parsing price data from Tommy's API.
    It retrieves JSON data about available store price tables and processes
    the corresponding CSV files for product information.
    """

    CHAIN = "tommy"
    BASE_URL = "https://spiza.tommy.hr/api/v2"

    def fetch_stores_list(self, date: datetime.date) -> dict[str, str]:
        """
        Fetch the list of store price tables for a specific date.

        Args:
            date: The date for which to fetch the price tables

        Returns:
            List of dictionaries containing store price table information

        Raises:
            httpx.RequestError: If the API request fails
            ValueError: If the response cannot be parsed
        """
        url = (
            f"{self.BASE_URL}/shop/store-prices-tables"
            f"?date={date:%Y-%m-%d}&page=1&itemsPerPage=200&channelCode=general"
        )
        content = self.fetch_text(url)
        data = loads(content)
        store_list = data.get("hydra:member", [])

        stores = {}
        for store in store_list:
            csv_id = store.get("@id")
            filename = store.get("fileName", "Unknown")
            if not csv_id or not filename:
                logger.warning(
                    f"Skipping store with missing CSV ID or filename: {store}"
                )
                continue
            if csv_id.startswith("/api/v2"):
                csv_id = csv_id[len("/api/v2") :]

            stores[filename] = self.BASE_URL + csv_id

        return stores

    def parse_date_string(self, date_str: str) -> Optional[datetime.date]:
        """
        Parse date string from CSV (format DD.MM.YYYY. HH:MM:SS).

        Args:
            date_str: The date string to parse (e.g., "16.5.2025. 0:00:00")

        Returns:
            datetime.date object or None if parsing fails
        """
        if not date_str or date_str.strip() == "":
            return None

        try:
            # Use regex to extract day, month, and year
            # The pattern handles both single and double-digit day/month
            match = re.match(r"(\d{1,2})\.(\d{1,2})\.(\d{4})\.", date_str)

            if match:
                day, month, year = map(int, match.groups())
                return datetime.date(year, month, day)
            else:
                logger.warning(f"Date string format not recognized: {date_str}")
                return None

        except (ValueError, IndexError) as e:
            logger.warning(f"Failed to parse date string '{date_str}': {e}")
            return None

    def parse_csv(self, csv_content: str) -> List[Product]:
        """
        Parse CSV content and extract product information.

        Args:
            csv_content: Content of the CSV file

        Returns:
            List of Product objects

        CSV format:
            BARKOD_ARTIKLA,SIFRA_ARTIKLA,NAZIV_ARTIKLA,BRAND,ROBNA_STRUKTURA,
            JEDINICA_MJERE,NETO_KOLICINA,MPC,MPC_POSEBNA_PRODAJA,CIJENA_PO_JM,
            MPC_NAJNIZA_30,MPC_020525,DATUM_ULASKA_NOVOG_ARTIKLA,PRVA_CIJENA_NOVOG_ARTIKLA
        """
        logger.debug("Parsing CSV content")

        products = []
        success_count = 0
        error_count = 0

        try:
            # Read CSV content using StringIO and DictReader
            csv_file = io.StringIO(csv_content)
            reader = csv.DictReader(csv_file)  # type: ignore

            if not reader.fieldnames:
                logger.warning("CSV file has no header row")
                return products

            logger.debug(f"CSV header: {reader.fieldnames}")

            # Define expected field names
            field_map = {
                "barcode": "BARKOD_ARTIKLA",
                "product_id": "SIFRA_ARTIKLA",
                "product_name": "NAZIV_ARTIKLA",
                "brand": "BRAND",
                "category": "ROBNA_STRUKTURA",
                "unit": "JEDINICA_MJERE",
                "quantity": "NETO_KOLICINA",
                "price": "MPC",
                "special_price": "MPC_POSEBNA_PRODAJA",
                "unit_price": "CIJENA_PO_JM",
                "lowest_price_30days": "MPC_NAJNIZA_30",
                "anchor_price": "MPC_020525",
                "date_added": "DATUM_ULASKA_NOVOG_ARTIKLA",
                "initial_price": "PRVA_CIJENA_NOVOG_ARTIKLA",
            }

            row_count = 0
            for row in reader:
                row_count += 1

                try:
                    # Extract mandatory fields from the row
                    barcode = row.get(field_map["barcode"], "").strip()
                    product_id = row.get(field_map["product_id"], "").strip()
                    product_name = row.get(field_map["product_name"], "").strip()
                    brand = row.get(field_map["brand"], "").strip()
                    category = row.get(field_map["category"], "").strip()
                    unit = row.get(field_map["unit"], "").strip()
                    quantity = row.get(field_map["quantity"], "").strip()

                    # Parse price fields with proper error handling
                    try:
                        price = parse_price(row.get(field_map["price"], "0"))
                    except Exception as e:
                        logger.warning(f"Failed to parse price in row {row_count}: {e}")
                        price = Decimal("0.00")

                    try:
                        unit_price = parse_price(row.get(field_map["unit_price"], "0"))
                    except Exception as e:
                        logger.warning(
                            f"Failed to parse unit_price in row {row_count}: {e}"
                        )
                        unit_price = Decimal("0.00")

                    # Parse optional price fields
                    special_price = None
                    lowest_price_30days = None
                    anchor_price = None
                    initial_price = None
                    date_added = None

                    special_price_str = row.get(field_map["special_price"], "")
                    if special_price_str.strip():
                        try:
                            special_price = parse_price(special_price_str)
                        except Exception:
                            pass

                    lowest_price_30days_str = row.get(
                        field_map["lowest_price_30days"], ""
                    )
                    if lowest_price_30days_str.strip():
                        try:
                            lowest_price_30days = parse_price(lowest_price_30days_str)
                        except Exception:
                            pass

                    anchor_price_str = row.get(field_map["anchor_price"], "")
                    if anchor_price_str.strip():
                        try:
                            anchor_price = parse_price(anchor_price_str)
                        except Exception:
                            pass

                    date_added_str = row.get(field_map["date_added"], "")
                    if date_added_str.strip():
                        date_added = self.parse_date_string(date_added_str)

                    initial_price_str = row.get(field_map["initial_price"], "")
                    if initial_price_str.strip():
                        try:
                            initial_price = parse_price(initial_price_str)
                        except Exception:
                            pass

                    # Create product if we have the minimum required fields
                    if product_name and (price or unit_price):
                        # If one price is missing but the other exists, use the existing one for both
                        if price and not unit_price:
                            unit_price = price
                        elif unit_price and not price:
                            price = unit_price

                        product = Product(
                            product=product_name,
                            product_id=product_id,
                            barcode=barcode,
                            brand=brand,
                            category=category,
                            unit=unit,
                            quantity=quantity,
                            price=price,
                            special_price=special_price,
                            unit_price=unit_price,
                            best_price_30=lowest_price_30days,  # Map lowest_price_30days to best_price_30
                            anchor_price=anchor_price,
                            date_added=date_added,
                            initial_price=initial_price,
                        )
                        products.append(product)
                        success_count += 1
                    else:
                        logger.warning(
                            f"Skipping product in row {row_count} with missing required fields: {row}"
                        )
                        error_count += 1

                except Exception as e:
                    logger.error(f"Error parsing product row {row_count}: {e}")
                    logger.debug(f"Problematic row: {row}")
                    error_count += 1

            logger.info(
                f"Parsed {len(products)} products from CSV (total rows: {row_count}, errors: {error_count})"
            )
            return products

        except Exception as e:
            logger.error(f"Error parsing CSV: {e}")
            return []

    def parse_store_from_filename(
        self, filename: str
    ) -> Tuple[str, str, str, str, str]:
        """
        Parse store information from the filename.

        Args:
            filename: The filename from the API

        Returns:
            Tuple of (store_type, address, zipcode, city)

        Example:
            "SUPERMARKET, ANTE STARČEVIĆA 6, 20260 KORČULA, 10180, 2, 20250516 0530"
            Will return:
            ("supermarket", "10180", "Ante Starčevića 6", "20260", "Korčula")
        """
        try:
            # Split by commas
            parts = filename.split(",")

            if len(parts) < 3:
                logger.warning(f"Filename doesn't have enough parts: {filename}")
                raise ValueError(f"Unparseable filename: {filename}")

            # Extract store type (first part)
            store_type = parts[0].strip().lower()

            # Extract address (second part)
            address = to_camel_case(parts[1].strip())

            # Extract zipcode and city (third part)
            location_part = parts[2].strip()

            # Use regex to extract zipcode and city
            # Pattern looks for 5 digits followed by any text
            match = re.match(r"(\d{5})\s+(.+)", location_part)

            if match:
                zipcode = match.group(1)
                city = to_camel_case(match.group(2))
            else:
                logger.warning(
                    f"Could not extract zipcode and city from: {location_part}"
                )
                zipcode = ""
                # Try to extract just the city if no zipcode pattern found
                city = to_camel_case(location_part)

            store_id = parts[3].strip()

            logger.debug(
                f"Parsed store info: type={store_type}, address={address}, zipcode={zipcode}, city={city}"
            )

            return (store_type, store_id, address, zipcode, city)

        except Exception as e:
            logger.error(f"Error parsing store from filename {filename}: {e}")
            raise

    def get_all_products(self, date: datetime.date) -> list[Store]:
        """
        Main method to fetch and parse all products from Tommy's price lists.

        Args:
            date: The date for which to fetch the price list

        Returns:
            Tuple with the date and the list of Store objects,
            each containing its products.

        Raises:
            ValueError: If the price list cannot be fetched or parsed
        """

        store_map = self.fetch_stores_list(date)
        if not store_map:
            logger.warning(f"No stores found for date {date}")
            return []

        stores = []
        for filename, url in store_map.items():
            # Extract store information
            store_type, store_id, address, zipcode, city = (
                self.parse_store_from_filename(filename)
            )

            store = Store(
                chain="tommy",
                name=f"Tommy {store_type.title()} {address}",
                store_type=store_type,
                store_id=store_id,
                city=city,
                street_address=address,
                zipcode=zipcode,
                items=[],
            )

            csv_content = self.fetch_text(url)
            products = self.parse_csv(csv_content)

            store.items = products
            stores.append(store)

        return stores


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = TommyCrawler()
    current_date = datetime.date.today() - datetime.timedelta(days=1)
    stores = crawler.get_all_products(current_date)
    print(stores[0])
    print(stores[0].items[0])
