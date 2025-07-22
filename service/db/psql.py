from contextlib import asynccontextmanager
import asyncpg
from typing import (
    AsyncGenerator,
    AsyncIterator,
    List,
    Any,
)
import logging
import os
import io
import re
from csv import DictReader
from pathlib import Path
from decimal import Decimal
from datetime import date
from .base import Database
from .models import (
    Chain,
    ChainStats,
    ChainWithId,
    Product,
    ProductWithId,
    Store,
    ChainProduct,
    Price,
    StorePrice,
    StoreWithId,
    ChainProductWithId,
    User,
)


class PostgresDatabase(Database):
    """PostgreSQL implementation of the database interface using asyncpg."""

    def __init__(self, dsn: str, min_size: int = 20, max_size: int = 50):
        """Initialize the PostgreSQL database connection pool.

        Args:
            dsn: Database connection string
            min_size: Minimum number of connections in the pool
            max_size: Maximum number of connections in the pool
        """
        self.dsn = dsn
        self.min_size = min_size
        self.max_size = max_size
        self.pool = None
        self.logger = logging.getLogger(__name__)

    async def connect(self) -> None:
        self.pool = await asyncpg.create_pool(
            dsn=self.dsn,
            min_size=self.min_size,
            max_size=self.max_size,
        )

    @asynccontextmanager
    async def _get_conn(self) -> AsyncGenerator[asyncpg.Connection]:
        """Context manager to acquire a connection from the pool."""
        if not self.pool:
            raise RuntimeError("Database pool is not initialized")
        async with self.pool.acquire() as conn:
            yield conn

    @asynccontextmanager
    async def _atomic(self) -> AsyncIterator[asyncpg.Connection]:
        """Context manager for atomic transactions."""
        async with self._get_conn() as conn:
            async with conn.transaction():
                yield conn

    async def close(self) -> None:
        """Close all database connections."""
        if self.pool:
            await self.pool.close()

    async def create_tables(self) -> None:
        schema_path = os.path.join(os.path.dirname(__file__), "psql.sql")

        try:
            with open(schema_path, "r") as f:
                schema_sql = f.read()

            async with self._get_conn() as conn:
                await conn.execute(schema_sql)
                self.logger.info("Database tables created successfully")
        except Exception as e:
            self.logger.error(f"Error creating tables: {e}")
            raise

    async def _fetchval(self, query: str, *args: Any) -> Any:
        async with self._get_conn() as conn:
            return await conn.fetchval(query, *args)

    async def get_product_barcodes(self) -> dict[str, int]:
        async with self._get_conn() as conn:
            rows = await conn.fetch("SELECT id, ean FROM products")
            return {row["ean"]: row["id"] for row in rows}

    async def get_chain_product_map(self, chain_id: int) -> dict[str, int]:
        async with self._get_conn() as conn:
            rows = await conn.fetch(
                """
                SELECT code, id FROM chain_products WHERE chain_id = $1
                """,
                chain_id,
            )
            return {row["code"]: row["id"] for row in rows}

    async def add_chain(self, chain: Chain) -> int:
        async with self._atomic() as conn:
            chain_id = await conn.fetchval(
                "SELECT id FROM chains WHERE code = $1",
                chain.code,
            )
            if chain_id is not None:
                return chain_id
            chain_id = await conn.fetchval(
                "INSERT INTO chains (code) VALUES ($1) RETURNING id",
                chain.code,
            )
            if chain_id is None:
                raise RuntimeError(f"Failed to insert chain {chain.code}")
            return chain_id

    async def list_chains(self) -> list[ChainWithId]:
        async with self._get_conn() as conn:
            rows = await conn.fetch("SELECT id, code FROM chains")
            return [ChainWithId(**row) for row in rows]  # type: ignore

    async def list_latest_chain_stats(self) -> list[ChainStats]:
        async with self._get_conn() as conn:
            rows = await conn.fetch("""
                SELECT
                    c.code AS chain_code,
                    cs.price_date,
                    cs.price_count,
                    cs.store_count,
                    cs.created_at
                FROM chains c
                JOIN LATERAL (
                    SELECT *
                    FROM chain_stats
                    WHERE chain_id = c.id
                    ORDER BY price_date DESC
                    LIMIT 1
                ) cs ON true;
            """)
            return [ChainStats(**row) for row in rows]  # type: ignore

    async def add_store(self, store: Store) -> int:
        return await self._fetchval(
            """
            INSERT INTO stores (chain_id, code, type, address, city, zipcode)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (chain_id, code) DO UPDATE SET
                type = COALESCE($3, stores.type),
                address = COALESCE($4, stores.address),
                city = COALESCE($5, stores.city),
                zipcode = COALESCE($6, stores.zipcode)
            RETURNING id
            """,
            store.chain_id,
            store.code,
            store.type,
            store.address or None,
            store.city or None,
            store.zipcode or None,
        )

    async def add_many_stores(self, stores: list[Store]) -> dict[str, int]:
        """
        Add multiple stores in a batch operation.

        Args:
            stores: List of Store objects to add or update.

        Returns:
            Dictionary mapping store codes to their database IDs.
        """
        if not stores:
            return {}

        async with self._atomic() as conn:
            # Create temporary table for bulk insert
            await conn.execute(
                """
                CREATE TEMP TABLE temp_stores (
                    chain_id INTEGER,
                    code VARCHAR(100),
                    type VARCHAR(100),
                    address VARCHAR(255),
                    city VARCHAR(100),
                    zipcode VARCHAR(20)
                )
                """
            )

            # Insert all stores into temporary table
            await conn.copy_records_to_table(
                "temp_stores",
                records=[
                    (
                        store.chain_id,
                        store.code,
                        store.type,
                        store.address or None,
                        store.city or None,
                        store.zipcode or None,
                    )
                    for store in stores
                ],
            )

            # Perform bulk upsert and get all store IDs
            await conn.execute(
                """
                INSERT INTO stores (chain_id, code, type, address, city, zipcode)
                SELECT chain_id, code, type, address, city, zipcode
                FROM temp_stores
                ON CONFLICT (chain_id, code) DO UPDATE SET
                    type = COALESCE(EXCLUDED.type, stores.type),
                    address = COALESCE(EXCLUDED.address, stores.address),
                    city = COALESCE(EXCLUDED.city, stores.city),
                    zipcode = COALESCE(EXCLUDED.zipcode, stores.zipcode)
                """
            )

            # Fetch all store IDs for the provided stores
            rows = await conn.fetch(
                """
                SELECT s.id, s.code
                FROM stores s
                JOIN temp_stores t ON s.chain_id = t.chain_id AND s.code = t.code
                """
            )

            # Clean up temporary table
            await conn.execute("DROP TABLE temp_stores")

            # Build the result dictionary
            result = {}
            for row in rows:
                result[row["code"]] = row["id"]

            return result

    async def update_store(
        self,
        chain_id: int,
        store_code: str,
        *,
        address: str | None = None,
        city: str | None = None,
        zipcode: str | None = None,
        lat: float | None = None,
        lon: float | None = None,
        phone: str | None = None,
    ) -> bool:
        """
        Update store information by chain_id and store code.
        Returns True if the store was updated, False if not found.
        """
        async with self._get_conn() as conn:
            result = await conn.execute(
                """
                UPDATE stores
                SET
                    address = COALESCE($3, stores.address),
                    city = COALESCE($4, stores.city),
                    zipcode = COALESCE($5, stores.zipcode),
                    lat = COALESCE($6, stores.lat),
                    lon = COALESCE($7, stores.lon),
                    phone = COALESCE($8, stores.phone)
                WHERE chain_id = $1 AND code = $2
                """,
                chain_id,
                store_code,
                address or None,
                city or None,
                zipcode or None,
                lat or None,
                lon or None,
                phone or None,
            )
            _, rowcount = result.split(" ")
            return int(rowcount) == 1

    async def list_stores(self, chain_code: str) -> list[StoreWithId]:
        async with self._get_conn() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    s.id, s.chain_id, s.code, s.type, s.address, s.city, s.zipcode,
                    s.lat, s.lon, s.phone
                FROM stores s
                JOIN chains c ON s.chain_id = c.id
                WHERE c.code = $1
                """,
                chain_code,
            )

            return [StoreWithId(**row) for row in rows]  # type: ignore

    async def filter_stores(
        self,
        chain_codes: list[str] | None = None,
        city: str | None = None,
        address: str | None = None,
        lat: float | None = None,
        lon: float | None = None,
        d: float = 10.0,
    ) -> list[StoreWithId]:
        # Validate lat/lon parameters
        if (lat is None) != (lon is None):
            raise ValueError(
                "Both lat and lon must be provided together, or both must be None"
            )

        async with self._get_conn() as conn:
            # Build the query dynamically based on provided filters
            where_conditions = []
            params = []
            param_counter = 1

            # Chain codes filter
            if chain_codes:
                where_conditions.append(f"c.code = ANY(${param_counter})")
                params.append(chain_codes)
                param_counter += 1

            # City filter (case-insensitive substring match)
            if city:
                where_conditions.append(f"s.city ILIKE ${param_counter}")
                params.append(f"%{city}%")
                param_counter += 1

            # Address filter (case-insensitive substring match)
            if address:
                where_conditions.append(f"s.address ILIKE ${param_counter}")
                params.append(f"%{address}%")
                param_counter += 1

            # Geolocation filter using computed earth_point column
            if lat is not None and lon is not None:
                where_conditions.append(
                    f"s.earth_point IS NOT NULL AND "
                    f"earth_distance(s.earth_point, ll_to_earth(${param_counter}, ${param_counter + 1})) <= ${param_counter + 2}"
                )
                params.extend([lat, lon, d * 1000])  # Convert km to meters
                param_counter += 3

            # Build the complete query
            base_query = """
                SELECT
                    s.id, s.chain_id, s.code, s.type, s.address, s.city, s.zipcode,
                    s.lat, s.lon, s.phone
                FROM stores s
                JOIN chains c ON s.chain_id = c.id
            """

            if where_conditions:
                query = base_query + " WHERE " + " AND ".join(where_conditions)
            else:
                query = base_query

            query += " ORDER BY c.code, s.code"
            rows = await conn.fetch(query, *params)
            return [StoreWithId(**row) for row in rows]  # type: ignore

    async def add_ean(self, ean: str) -> int:
        """
        Add an empty product with only EAN barcode info.

        Args:
            ean: The EAN code to add.

        Returns:
            The database ID of the created product.
        """
        return await self._fetchval(
            "INSERT INTO products (ean) VALUES ($1) RETURNING id",
            ean,
        )

    async def add_many_eans(self, eans: list[str]) -> dict[str, int]:
        """
        Add multiple empty products with only EAN codes in a batch operation.

        Args:
            eans: List of EAN codes to add.

        Returns:
            Dictionary mapping EAN codes to their database IDs.
        """
        if not eans:
            return {}

        async with self._atomic() as conn:
            # Create temporary table for bulk insert
            await conn.execute(
                """
                CREATE TEMP TABLE temp_eans (
                    ean VARCHAR(50)
                )
                """
            )

            # Insert all EAN codes into temporary table
            await conn.copy_records_to_table(
                "temp_eans",
                records=[(ean,) for ean in eans],
            )

            # Insert new EAN codes (ignoring conflicts for existing ones)
            await conn.execute(
                """
                INSERT INTO products (ean)
                SELECT ean FROM temp_eans
                ON CONFLICT (ean) DO NOTHING
                """
            )

            # Fetch all product IDs for the requested EAN codes
            rows = await conn.fetch(
                """
                SELECT id, ean FROM products
                WHERE ean IN (SELECT ean FROM temp_eans)
                """
            )

            # Clean up temporary table
            await conn.execute("DROP TABLE temp_eans")

            # Build the result dictionary
            result = {}
            for row in rows:
                result[row["ean"]] = row["id"]

            return result

    async def get_products_by_ean(self, ean: list[str]) -> list[ProductWithId]:
        async with self._get_conn() as conn:
            rows = await conn.fetch(
                """
                SELECT id, ean, brand, name, quantity, unit
                FROM products WHERE ean = ANY($1)
                """,
                ean,
            )
            return [ProductWithId(**row) for row in rows]  # type: ignore

    async def get_product_store_prices(
        self,
        product_ids: list[int],
        store_ids: list[int] | None = None,
    ) -> list[StorePrice]:
        async with self._get_conn() as conn:
            query = """
                WITH chains_dates AS (
                  -- Find the latest loaded data per chain
                    SELECT DISTINCT ON (chain_id) chain_id, price_date AS last_price_date
                    FROM chain_stats
                    ORDER BY chain_id, price_date DESC
                )
                SELECT
                    chains.id AS chain_id,
                    chains.code AS chain_code,
                    products.ean,
                    prices.price_date,
                    prices.regular_price,
                    prices.special_price,
                    prices.best_price_30,
                    prices.unit_price,
                    prices.anchor_price,
                    stores.code AS store_code,
                    stores.type,
                    stores.address,
                    stores.city,
                    stores.zipcode,
                    stores.lat,
                    stores.lon,
                    stores.phone
                FROM chains_dates
                JOIN chains ON chains.id = chains_dates.chain_id
                JOIN chain_products ON chain_products.chain_id = chains.id
                JOIN products ON products.id = chain_products.product_id
                JOIN prices ON prices.chain_product_id = chain_products.id
                           AND prices.price_date = chains_dates.last_price_date
                JOIN stores ON stores.id = prices.store_id
                WHERE products.id = ANY($1)
            """

            params = [product_ids]
            param_idx = 2

            if store_ids is not None:
                query += f" AND stores.id = ANY(${param_idx})"
                params.append(store_ids)
                param_idx += 1

            rows = await conn.fetch(query, *params)

            return [
                StorePrice(
                    chain=row["chain_code"],
                    ean=row["ean"],
                    price_date=row["price_date"],
                    regular_price=row["regular_price"],
                    special_price=row["special_price"],
                    unit_price=row["unit_price"],
                    best_price_30=row["best_price_30"],
                    anchor_price=row["anchor_price"],
                    store=Store(
                        chain_id=row["chain_id"],
                        code=row["store_code"],
                        type=row["type"],
                        address=row["address"],
                        city=row["city"],
                        zipcode=row["zipcode"],
                        lat=row["lat"],
                        lon=row["lon"],
                        phone=row["phone"],
                    ),
                )
                for row in rows
            ]

    async def update_product(self, product: Product) -> bool:
        """
        Update product information by EAN code.

        Args:
            product: Product object containing the EAN and fields to update.
                    Only non-None fields will be updated in the database.

        Returns:
            True if the product was updated, False if not found.
        """
        async with self._get_conn() as conn:
            result = await conn.execute(
                """
                UPDATE products
                SET
                    brand = COALESCE($2, products.brand),
                    name = COALESCE($3, products.name),
                    quantity = COALESCE($4, products.quantity),
                    unit = COALESCE($5, products.unit)
                WHERE ean = $1
                """,
                product.ean,
                product.brand,
                product.name,
                product.quantity,
                product.unit,
            )
            _, rowcount = result.split(" ")
            return int(rowcount) == 1

    async def get_chain_products_for_product(
        self,
        product_ids: list[int],
        chain_ids: list[int] | None = None,
    ) -> list[ChainProductWithId]:
        async with self._get_conn() as conn:
            if chain_ids:
                # Use ANY for filtering by chain IDs
                query = """
                    SELECT
                        id, chain_id, product_id, code, name, brand,
                        category, unit, quantity
                    FROM chain_products
                    WHERE product_id = ANY($1) AND chain_id = ANY($2)
                """
                rows = await conn.fetch(query, product_ids, chain_ids)
            else:
                # Original query when no chain filtering
                query = """
                    SELECT
                        id, chain_id, product_id, code, name, brand,
                        category, unit, quantity
                    FROM chain_products
                    WHERE product_id = ANY($1)
                """
                rows = await conn.fetch(query, product_ids)
            return [ChainProductWithId(**row) for row in rows]  # type: ignore

    async def search_products(self, query: str) -> list[ProductWithId]:
        if not query.strip():
            return []

        # TODO: Implement full-text search using PostgreSQL's
        # text search capabilities
        words = [word.strip() for word in query.split() if word.strip()]
        if not words:
            return []

        where_conditions = []
        params = []

        for idx, word in enumerate(words, start=1):
            word = word.lower().replace("%", "")
            where_conditions.append(f"cp.name ILIKE ${idx}")
            params.append(f"%{word}%")

        where_clause = " AND ".join(where_conditions)
        query_sql = f"""
            SELECT
                p.ean,
                COUNT(cp) AS product_count
            FROM chain_products cp
            JOIN products p ON cp.product_id = p.id
            WHERE {where_clause}
            GROUP BY p.ean
            ORDER BY product_count DESC
        """

        async with self._get_conn() as conn:
            rows = await conn.fetch(query_sql, *params)
            eans = [row["ean"] for row in rows]

        return await self.get_products_by_ean(eans)

    async def get_product_prices(
        self, product_ids: list[int], date: date
    ) -> list[dict[str, Any]]:
        async with self._get_conn() as conn:
            return await conn.fetch(
                """
                WITH chains_dates AS (
                    -- Find the latest loaded data per chain
                   SELECT DISTINCT ON (chain_id) chain_id, price_date AS last_price_date
                   FROM chain_stats
                   WHERE price_date <= $2
                   ORDER BY chain_id, price_date DESC
                )
                SELECT chains.code AS chain,
                       chain_products.product_id,
                       chain_prices.min_price,
                       chain_prices.max_price,
                       chain_prices.avg_price,
                       chain_prices.price_date
                FROM chains_dates
                JOIN chains ON chains.id = chains_dates.chain_id
                JOIN chain_products ON chain_products.chain_id = chains.id
                JOIN chain_prices ON chain_prices.chain_product_id = chain_products.id
                                 AND chain_prices.price_date = chains_dates.last_price_date
                WHERE chain_products.product_id = ANY($1)
                """,
                product_ids,
                date,
            )

    async def add_many_prices(self, prices: list[Price]) -> int:
        async with self._atomic() as conn:
            await conn.execute(
                """
                CREATE TEMP TABLE temp_prices (
                    chain_product_id INTEGER,
                    store_id INTEGER,
                    price_date DATE,
                    regular_price DECIMAL(10, 2),
                    special_price DECIMAL(10, 2),
                    unit_price DECIMAL(10, 2),
                    best_price_30 DECIMAL(10, 2),
                    anchor_price DECIMAL(10, 2)
                )
                """
            )
            # Generate CSV data for optimized bulk insert
            csv_data = io.BytesIO()
            for p in prices:
                csv_line = (
                    f"{p.chain_product_id},{p.store_id},{p.price_date},"
                    f"{p.regular_price or '\\N'},{p.special_price or '\\N'},"
                    f"{p.unit_price or '\\N'},{p.best_price_30 or '\\N'},"
                    f"{p.anchor_price or '\\N'}\n"
                )
                csv_data.write(csv_line.encode("utf-8"))

            csv_data.seek(0)
            await conn.copy_to_table(
                "temp_prices", source=csv_data, format="csv", delimiter=",", null="\\N"
            )
            result = await conn.execute(
                """
                INSERT INTO prices(
                    chain_product_id,
                    store_id,
                    price_date,
                    regular_price,
                    special_price,
                    unit_price,
                    best_price_30,
                    anchor_price
                )
                SELECT * from temp_prices
                ON CONFLICT DO NOTHING
                """
            )
            await conn.execute("DROP TABLE temp_prices")

            _, _, rowcount = result.split(" ")
            rowcount = int(rowcount)
            return rowcount

    def _clean_price(self, value: str) -> str:
        """
        Clean and validate price value for CSV format using fast string validation.

        Args:
            value: Price value as string.

        Returns:
            Cleaned price as string or '\\N' for null.
        """
        if not value or not value.strip():
            return "\\N"

        cleaned = value.strip()

        # Fast string-based validation without Decimal object creation
        # Match valid price patterns: digits with optional decimal (1-2 places)
        if re.match(r"^\d+(\.\d{1,2})?$", cleaned):
            # Check for zero values without creating Decimal object
            if cleaned in ("0", "0.0", "0.00"):
                return "\\N"
            return cleaned

        return "\\N"

    async def add_many_prices_direct_csv(
        self,
        csv_path: Path,
        price_date: date,
        store_map: dict[str, int],
        chain_product_map: dict[str, int],
    ) -> int:
        """
        Add multiple prices directly from CSV file for optimal performance.

        Args:
            csv_path: Path to the CSV file containing price data.
            price_date: The date for which the prices are valid.
            store_map: Dictionary mapping store codes to their database IDs.
            chain_product_map: Dictionary mapping product codes to their database IDs.

        Returns:
            The number of prices successfully inserted into the database.
        """
        async with self._atomic() as conn:
            await conn.execute(
                """
                CREATE TEMP TABLE temp_prices (
                    chain_product_id INTEGER,
                    store_id INTEGER,
                    price_date DATE,
                    regular_price DECIMAL(10, 2),
                    special_price DECIMAL(10, 2),
                    unit_price DECIMAL(10, 2),
                    best_price_30 DECIMAL(10, 2),
                    anchor_price DECIMAL(10, 2)
                )
                """
            )

            # Stream CSV data directly with transformations
            csv_data = io.BytesIO()
            skipped_count = 0

            with open(csv_path, "r", encoding="utf-8") as f:
                reader = DictReader(f)  # type: ignore[no-matching-overload]
                for row in reader:
                    store_id = store_map.get(row["store_id"])
                    product_id = chain_product_map.get(row["product_id"])

                    if store_id is None or product_id is None:
                        skipped_count += 1
                        self.logger.warning(
                            f"Skipped price row due to missing store/product ({store_id}/{product_id}) mappings"
                        )
                        continue

                    # Convert price directly like the old import (no validation)
                    try:
                        regular_price = str(Decimal(row["price"].strip()))
                    except (ValueError, TypeError, AttributeError):
                        skipped_count += 1
                        self.logger.warning(
                            f"Skipped price row due to invalid price: {row['price']} - {row}"
                        )
                        continue

                    # Transform row data
                    csv_line = (
                        f"{product_id},{store_id},{price_date},"
                        f"{regular_price},"
                        f"{self._clean_price(row.get('special_price', ''))},"
                        f"{self._clean_price(row.get('unit_price', ''))},"
                        f"{self._clean_price(row.get('best_price_30', ''))},"
                        f"{self._clean_price(row.get('anchor_price', ''))}\n"
                    )

                    csv_data.write(csv_line.encode("utf-8"))

            if skipped_count > 0:
                self.logger.warning(
                    f"Skipped {skipped_count} price rows due to missing store/product ({store_id}/{product_id}) mappings"
                )

            csv_data.seek(0)
            await conn.copy_to_table(
                "temp_prices", source=csv_data, format="csv", delimiter=",", null="\\N"
            )

            result = await conn.execute(
                """
                INSERT INTO prices(
                    chain_product_id,
                    store_id,
                    price_date,
                    regular_price,
                    special_price,
                    unit_price,
                    best_price_30,
                    anchor_price
                )
                SELECT * from temp_prices
                ON CONFLICT DO NOTHING
                """
            )
            await conn.execute("DROP TABLE temp_prices")

            _, _, rowcount = result.split(" ")
            rowcount = int(rowcount)
            return rowcount

    async def add_many_chain_products(
        self,
        chain_products: List[ChainProduct],
    ) -> int:
        async with self._atomic() as conn:
            await conn.execute(
                """
                CREATE TEMP TABLE temp_chain_products (
                    chain_id INTEGER,
                    product_id INTEGER,
                    code VARCHAR(100),
                    name VARCHAR(255),
                    brand VARCHAR(255),
                    category VARCHAR(255),
                    unit VARCHAR(50),
                    quantity VARCHAR(50)
                )
                """
            )
            await conn.copy_records_to_table(
                "temp_chain_products",
                records=(
                    (
                        cp.chain_id,
                        cp.product_id,
                        cp.code,
                        cp.name,
                        cp.brand,
                        cp.category,
                        cp.unit,
                        cp.quantity,
                    )
                    for cp in chain_products
                ),
            )

            result = await conn.execute(
                """
                INSERT INTO chain_products(
                    chain_id,
                    product_id,
                    code,
                    name,
                    brand,
                    category,
                    unit,
                    quantity
                )
                SELECT * from temp_chain_products
                ON CONFLICT DO NOTHING
                """
            )
            await conn.execute("DROP TABLE temp_chain_products")

            _, _, rowcount = result.split(" ")
            rowcount = int(rowcount)
            return rowcount

    async def compute_chain_prices(self, date: date) -> None:
        async with self._get_conn() as conn:
            await conn.execute(
                """
                INSERT INTO chain_prices (
                    chain_product_id,
                    price_date,
                    min_price,
                    max_price,
                    avg_price
                )
                SELECT
                    chain_product_id,
                    price_date,
                    MIN(
                        LEAST(
                            COALESCE(regular_price, special_price),
                            COALESCE(special_price, regular_price)
                        )
                    ) AS min_price,
                    MAX(
                        LEAST(
                            COALESCE(regular_price, special_price),
                            COALESCE(special_price, regular_price)
                        )
                    ) AS max_price,
                    ROUND(
                        AVG(
                            LEAST(
                                COALESCE(regular_price, special_price),
                                COALESCE(special_price, regular_price)
                            )
                        ),
                        2
                    ) AS avg_price
                FROM prices
                WHERE price_date = $1
                GROUP BY chain_product_id, price_date
                ON CONFLICT (chain_product_id, price_date)
                DO UPDATE SET
                    min_price = EXCLUDED.min_price,
                    max_price = EXCLUDED.max_price,
                    avg_price = EXCLUDED.avg_price;

                """,
                date,
            )

    async def compute_chain_stats(self, date: date) -> None:
        async with self._atomic() as conn:
            # Not doing insert in the same query because that caused deadlocks
            # for reasons which I don't understand.
            stats = await conn.fetch(
                """
                SELECT
                    cp.chain_id,
                    COUNT(*) AS price_count,
                    COUNT(DISTINCT p.store_id) AS store_count
                FROM prices p
                JOIN chain_products cp ON cp.id = p.chain_product_id
                WHERE p.price_date = $1
                GROUP BY cp.chain_id
                """,
                date,
            )

            for record in stats:
                await conn.execute(
                    """
                    INSERT INTO chain_stats(chain_id, price_date, price_count, store_count)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (chain_id, price_date)
                    DO UPDATE SET
                        price_count = EXCLUDED.price_count,
                        store_count = EXCLUDED.store_count;
                    """,
                    record["chain_id"],
                    date,
                    record["price_count"],
                    record["store_count"],
                )

    async def get_user_by_api_key(self, api_key: str) -> User | None:
        async with self._get_conn() as conn:
            row = await conn.fetchrow(
                """
                SELECT id, name, api_key, is_active, created_at
                FROM users
                WHERE
                    api_key = $1 AND
                    is_active = TRUE
                """,
                api_key,
            )

            if row:
                return User(**row)  # type: ignore
            return None
