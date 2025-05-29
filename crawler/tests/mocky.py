# -*- coding: utf-8 -*-
import datetime
import logging
from decimal import Decimal
from typing import List, Optional

from crawler.store.models import Product, Store
from crawler.store.base import BaseCrawler

logger = logging.getLogger(__name__)

# The simulation starts from SIMULATION_START_DATE (January 1, 2025).
# Price changes (ups, downs, stability, fluctuations) are calculated based on
# the number of days elapsed between SIMULATION_START_DATE and the requested date.

# STABLE_NO_EAN_NO_EAN is a initially a product without a valid EAN (barcode) for testing purposes.

# TEST DATES:

# 2025-06-01: New store (Mocky 3) added with all 4 products (including STABLE_NO_EAN with no EAN)
# 2025-06-01: STABLE_NO_EAN now gets a valid EAN (barcode) for testing purposes in Mocky 2 store
# 2025-07-01: STABLE_NO_EAN gets a valid EAN (barcode) for testing purposes in all stores


class MockCrawler(BaseCrawler):
    """
    Crawler for mock store data with simulated price changes.
    Price simulation starts from SIMULATION_START_DATE.
    """

    CHAIN = "mocky"
    DATE_SIM_START: datetime.date = datetime.date(2025, 1, 1)
    DATE_MOCKY3_STORE_ADDED: datetime.date = datetime.date(2025, 6, 1)
    VALID_EAN = "111000111001"
    DATE_EAN_FIX_IN_MOCKY2: datetime.date = datetime.date(2025, 6, 1)
    DATE_PRODUCT_EAN_FIX_IN_ALL_STORES: datetime.date = datetime.date(2025, 7, 1)

    def _get_base_products(self) -> List[Product]:
        """
        Returns a list of products with their initial state as of SIMULATION_START_DATE.
        The 'price' and 'unit_price' here are the initial values on SIMULATION_START_DATE.
        'initial_price' field is also set to this starting price.
        'date_added' is set to SIMULATION_START_DATE.
        """
        return [
            Product(
                product="Increasing Price Product (Milk)",
                product_id="INCREASING",
                brand="MockFarms",
                quantity="1L",
                unit="L",
                price=Decimal("1.99"),  # Price on 2025-01-01
                unit_price=Decimal("1.99"),  # Unit price on 2025-01-01
                barcode="111000111001",
                category="Dairy & Chilled",
                initial_price=Decimal("1.99"),
                date_added=self.DATE_SIM_START,
                packaging="Carton",
                anchor_price=Decimal("2.05"),
                anchor_price_date=self.DATE_SIM_START.strftime("%Y-%m-%d"),
                best_price_30=Decimal("1.99"),  # Initial best price
            ),
            Product(
                product="Fluctuating Price Product (Bread)",
                product_id="FLUCTUATING",
                brand="MockBakery",
                quantity="750g",
                unit="g",
                price=Decimal("3.49"),  # Price on 2025-01-01
                unit_price=Decimal("4.65"),  # Unit price (per kg) on 2025-01-01
                barcode="222000222002",
                category="Bakery",
                initial_price=Decimal("3.49"),
                date_added=self.DATE_SIM_START,
                packaging="Paper Bag",
                anchor_price=Decimal("3.59"),
                anchor_price_date=self.DATE_SIM_START.strftime("%Y-%m-%d"),
                best_price_30=Decimal("3.49"),  # Initial best price
            ),
            Product(
                product="Stable Price Product (Eggs)",
                product_id="STABLE_NO_EAN",
                brand="Happy Mock Hens",
                quantity="6 pack",
                unit="pack",
                price=Decimal("2.79"),  # Price on 2025-01-01
                unit_price=Decimal("0.47"),  # Unit price (per egg) on 2025-01-01
                barcode="NO_CODE",  # EAN NOT VALID (len > 8), we will later use '{chain_slug}:{product_id}' instead
                category="Dairy & Chilled",
                initial_price=Decimal("2.79"),
                date_added=self.DATE_SIM_START,
                packaging="Carton",
                # No anchor price for this item initially
                best_price_30=Decimal("2.79"),  # Initial best price
            ),
            Product(
                product="Decreasing Price Product (Water)",
                product_id="DECREASING",
                brand="MockSprings",
                quantity="1.5L",
                unit="L",
                price=Decimal("0.89"),  # Price on 2025-01-01
                unit_price=Decimal("0.59"),  # Unit price (per L) on 2025-01-01
                barcode="444000444004",
                category="Drinks",
                initial_price=Decimal("0.89"),
                date_added=self.DATE_SIM_START,
                packaging="Plastic Bottle",
                anchor_price=Decimal("0.95"),  # Using initial price as base for anchor
                anchor_price_date=self.DATE_SIM_START.strftime("%Y-%m-%d"),
                best_price_30=Decimal("0.89"),  # Initial best price
            ),
        ]

    def _simulate_product_state(
        self, base_product: Product, days_since_start: int
    ) -> Product:
        """Applies price and related fields simulation logic to a product."""
        simulated_product = base_product.model_copy(deep=True)

        # Ensure initial_price is set from the base product's price (as of SIMULATION_START_DATE)
        initial_price = base_product.price  # This is the price at SIMULATION_START_DATE
        initial_unit_price = base_product.unit_price

        new_price = initial_price  # Default to no change
        current_special_price: Optional[Decimal] = None

        if simulated_product.product_id == "INCREASING":  # Steady Increase
            price_increases = days_since_start // 15  # Increases every 15 days
            new_price = initial_price + (Decimal("0.02") * price_increases)
        elif simulated_product.product_id == "FLUCTUATING":  # Fluctuation
            cycle_day = days_since_start % 7  # Weekly cycle
            if cycle_day in [0, 1]:  # Mon-Tue: slightly higher
                new_price = initial_price + Decimal("0.10")
            elif cycle_day in [4, 5]:  # Fri-Sat: special discount
                new_price = initial_price - Decimal("0.15")
                current_special_price = new_price
            # Wed, Thu, Sun: new_price remains initial_price or small variation
            elif cycle_day == 6:  # Sunday
                new_price = initial_price + Decimal("0.05")
            else:  # Wed, Thu
                new_price = initial_price
        elif simulated_product.product_id == "STABLE_NO_EAN":  # Stable
            new_price = initial_price
        elif simulated_product.product_id == "DECREASING":  # Slow Decrease
            price_decreases = days_since_start // 30  # Decreases every 30 days
            new_price = initial_price - (Decimal("0.01") * price_decreases)
            new_price = max(new_price, Decimal("0.50"))  # Floor price

        simulated_product.price = new_price.quantize(Decimal("0.01"))
        simulated_product.special_price = (
            current_special_price.quantize(Decimal("0.01"))
            if current_special_price
            else None
        )

        # Simulate unit_price proportionally
        if (
            initial_unit_price is not None
            and initial_price is not None
            and initial_price > 0
        ):
            ratio = simulated_product.price / initial_price
            simulated_product.unit_price = (initial_unit_price * ratio).quantize(
                Decimal("0.01")
            )
        elif initial_unit_price is not None:  # initial_price is 0 or None
            simulated_product.unit_price = (
                initial_unit_price if simulated_product.price > 0 else Decimal("0.00")
            )

        # Simulate best_price_30 (simplified)
        # This is a heuristic and not a true "lowest in last 30 days"
        if current_special_price:
            simulated_product.best_price_30 = current_special_price
        elif simulated_product.product_id == "DECREASING":  # Decreasing
            simulated_product.best_price_30 = simulated_product.price
        elif simulated_product.product_id == "STABLE_NO_EAN":  # Stable
            simulated_product.best_price_30 = simulated_product.price
        else:  # Milk (increasing), Bread (fluctuating, not on special)
            # Heuristic: slightly lower than current, but not lower than initial if recent
            potential_best_price = simulated_product.price * Decimal("0.97")
            if days_since_start < 30:
                simulated_product.best_price_30 = min(
                    initial_price, potential_best_price
                ).quantize(Decimal("0.01"))
            else:
                simulated_product.best_price_30 = potential_best_price.quantize(
                    Decimal("0.01")
                )

        # Ensure best_price_30 is not higher than current price
        if (
            simulated_product.best_price_30 is not None
            and simulated_product.best_price_30 > simulated_product.price
        ):
            simulated_product.best_price_30 = simulated_product.price

        return simulated_product

    def get_all_products(self, date: datetime.date) -> List[Store]:
        """
        Returns a list of mock stores with products whose prices are simulated
        based on the provided date relative to SIMULATION_START_DATE.
        """
        logger.info(f"Generating mock data for chain '{self.CHAIN}' for date {date}")
        logger.info(f"Price simulation reference start date: {self.DATE_SIM_START}.")

        if date < self.DATE_SIM_START:
            logger.warning(
                f"Requested date {date} is before simulation start date {self.DATE_SIM_START}. "
                f"Prices will be shown as of {self.DATE_SIM_START}."
            )
            days_since_start = 0
        else:
            days_since_start = (date - self.DATE_SIM_START).days

        logger.info(f"Simulating prices for {days_since_start} days after start date.")

        base_products = self._get_base_products()

        simulated_products_map = {
            p.product_id: self._simulate_product_state(p, days_since_start)
            for p in base_products
        }

        # --- Store 1 (Mocky 1) ---
        # Mocky 1 does not have STABLE_NO_EAN
        store1_items = [
            simulated_products_map["INCREASING"],
            simulated_products_map["FLUCTUATING"],
            simulated_products_map["DECREASING"],
        ]
        store1 = Store(
            chain=self.CHAIN,
            store_id="MOCK_S1",
            name="Mocky 1",
            store_type="supermarket",
            city="Mockcity",
            street_address="Test Street 1",
            zipcode="10000",
            items=store1_items,
        )

        # --- Store 2 (Mocky 2) ---
        # Mocky 2 has STABLE_NO_EAN; its EAN might change based on the date.
        stable_product_mocky2 = simulated_products_map["STABLE_NO_EAN"].model_copy(
            deep=True
        )
        if date >= self.DATE_PRODUCT_EAN_FIX_IN_ALL_STORES:
            stable_product_mocky2.barcode = self.VALID_EAN
        elif date >= self.DATE_EAN_FIX_IN_MOCKY2:
            stable_product_mocky2.barcode = self.VALID_EAN
        # Else - original barcode from _get_base_products.

        store2_items = [
            simulated_products_map["INCREASING"],
            stable_product_mocky2,  # Use the (potentially modified) STABLE_NO_EAN
            simulated_products_map["DECREASING"],
        ]
        store2 = Store(
            chain=self.CHAIN,
            store_id="MOCK_S2",
            name="Mocky 2",
            store_type="minimarket",
            city="Mockvillage",
            street_address="Test Avenue 12",
            zipcode="21000",
            items=store2_items,
        )

        stores_list = [store1, store2]

        # --- Store 3 (Mocky 3) ---
        # Added on MOCKY3_STORE_ADDITION_DATE.
        # Contains all 4 products. STABLE_NO_EAN initially has its original (invalid) EAN,
        # then gets a valid EAN on STABLE_PRODUCT_EAN_FIX_ALL_STORES_DATE.
        if date >= self.DATE_MOCKY3_STORE_ADDED:
            stable_product_mocky3 = simulated_products_map["STABLE_NO_EAN"].model_copy(
                deep=True
            )
            # STABLE_NO_EAN in Mocky 3 gets a valid EAN only on/after STABLE_PRODUCT_EAN_FIX_ALL_STORES_DATE
            if date >= self.DATE_PRODUCT_EAN_FIX_IN_ALL_STORES:
                stable_product_mocky3.barcode = self.VALID_EAN
            # Else, it keeps its original invalid barcode

            store3_items = [
                simulated_products_map["INCREASING"].model_copy(deep=True),
                simulated_products_map["FLUCTUATING"].model_copy(deep=True),
                stable_product_mocky3,  # Use the (potentially modified) STABLE_NO_EAN for Mocky 3
                simulated_products_map["DECREASING"].model_copy(deep=True),
            ]
            store3 = Store(
                chain=self.CHAIN,
                store_id="MOCK_S3",
                name="Mocky 3",
                store_type="hypermarket",
                city="Mockcity",
                street_address="Test Boulevard 33",
                zipcode="10000",
                items=store3_items,
            )
            stores_list.append(store3)

        logger.info(
            f"Generated {len(stores_list)} mock stores with a total of {sum(len(s.items) for s in stores_list)} simulated products."
        )
        return stores_list


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    )

    mock_crawler = MockCrawler()

    dates_to_test = [
        MockCrawler.DATE_SIM_START,
        MockCrawler.DATE_SIM_START
        + datetime.timedelta(days=1),  # Day 1 (Mon for bread if start is Mon)
        MockCrawler.DATE_SIM_START
        + datetime.timedelta(days=5),  # Day 5 (Fri for bread)
        MockCrawler.DATE_SIM_START
        + datetime.timedelta(days=16),  # Milk price increased
        MockCrawler.DATE_SIM_START
        + datetime.timedelta(days=35),  # Water price decreased, bread cycle
        MockCrawler.DATE_SIM_START + datetime.timedelta(days=60),
        datetime.date(2024, 12, 15),  # Date before simulation start
        MockCrawler.DATE_MOCKY3_STORE_ADDED,  # Date when Mocky 3 is added
        MockCrawler.DATE_PRODUCT_EAN_FIX_IN_ALL_STORES,  # Date when STABLE_NO_EAN gets valid EAN in all stores
    ]

    for test_date in dates_to_test:
        logger.info(
            f"\n--- Testing get_all_products for {MockCrawler.CHAIN} on date: {test_date} ---"
        )
        mock_stores = mock_crawler.get_all_products(test_date)

        if mock_stores:
            logger.info(
                f"Successfully retrieved {len(mock_stores)} mock stores for {test_date}."
            )
            for store_idx, store in enumerate(mock_stores):
                logger.info(
                    f"  Store {store_idx+1}: {store.name} (ID: {store.store_id})"
                )
                if store.items:
                    for prod_idx, product in enumerate(store.items):
                        logger.info(
                            f"    Product {prod_idx+1}: {product.product} ({product.product_id})"
                            f" - Barcode: {product.barcode}"
                            f" - Price: {product.price}"
                            f" (Unit: {product.unit_price})"
                            f" - Special: {product.special_price}"
                            f" - Best 30d: {product.best_price_30}"
                        )
                else:
                    logger.info("    No products in this store.")
        else:
            logger.warning(
                f"No mock stores were returned by get_all_products for {test_date}."
            )
        logger.info("--- Test End ---")

    # Example of using the crawl method (which calls get_all_products internally)
    # The current date for this test run will be 2025-05-28
    current_test_date = datetime.date(2025, 5, 28)
    logger.info(
        f"\n--- Testing crawl method for {MockCrawler.CHAIN} on date: {current_test_date} ---"
    )
    try:
        # Note: crawl method in BaseCrawler might save to file, this test just checks if it runs
        crawled_stores = mock_crawler.crawl(current_test_date)
        if crawled_stores:
            logger.info(
                f"Crawl method returned {len(crawled_stores)} stores for {current_test_date}."
            )
            # Further checks could be added if needed
            logger.info("Crawl method test successful (execution check).")
        else:
            logger.warning(f"Crawl method returned no stores for {current_test_date}.")
    except Exception as e:
        logger.error(f"Error during crawl method test: {e}", exc_info=True)
