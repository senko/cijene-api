from dataclasses import dataclass
import os
import datetime
from typing import List
import logging
from pathlib import Path
from time import time


from crawler.store.konzum import KonzumCrawler
from crawler.store.lidl import LidlCrawler
from crawler.store.plodine import PlodineCrawler
from crawler.store.ribola import RibolaCrawler
from crawler.store.spar import SparCrawler
from crawler.store.studenac import StudenacCrawler
from crawler.store.tommy import TommyCrawler
from crawler.store.kaufland import KauflandCrawler
from crawler.store.eurospin import EurospinCrawler
from crawler.store.dm import DmCrawler
from crawler.store.ktc import KtcCrawler
from crawler.store.metro import MetroCrawler
from crawler.store.trgocentar import TrgocentarCrawler
from crawler.store.zabac import ZabacCrawler
from crawler.store.vrutak import VrutakCrawler
from crawler.store.ntl import NtlCrawler

from dotenv import load_dotenv # For loading .env file for db credentials
from crawler.cli.db_importer import connect_db as importer_connect_db, \
                                    create_tables as importer_create_tables, \
                                    import_data as importer_import_data

from crawler.store.output import save_chain, copy_archive_info, create_archive

logger = logging.getLogger(__name__)

CRAWLERS = {
    StudenacCrawler.CHAIN: StudenacCrawler,
    SparCrawler.CHAIN: SparCrawler,
    KonzumCrawler.CHAIN: KonzumCrawler,
    PlodineCrawler.CHAIN: PlodineCrawler,
    LidlCrawler.CHAIN: LidlCrawler,
    TommyCrawler.CHAIN: TommyCrawler,
    KauflandCrawler.CHAIN: KauflandCrawler,
    EurospinCrawler.CHAIN: EurospinCrawler,
    DmCrawler.CHAIN: DmCrawler,
    KtcCrawler.CHAIN: KtcCrawler,
    MetroCrawler.CHAIN: MetroCrawler,
    TrgocentarCrawler.CHAIN: TrgocentarCrawler,
    ZabacCrawler.CHAIN: ZabacCrawler,
    VrutakCrawler.CHAIN: VrutakCrawler,
    NtlCrawler.CHAIN: NtlCrawler,
    RibolaCrawler.CHAIN: RibolaCrawler,
}


def get_chains() -> List[str]:
    """
    Get the list of retail chains from the crawlers.

    Returns:
        List of retail chain names.
    """
    return list(CRAWLERS.keys())


@dataclass
class CrawlResult:
    elapsed_time: float = 0
    n_stores: int = 0
    n_products: int = 0
    n_prices: int = 0


def crawl_chain(chain: str, date: datetime.date, path: Path) -> CrawlResult:
    """
    Crawl a specific retail chain for product/pricing data and save it.

    Args:
        chain: The name of the retail chain to crawl.
        date: The date for which to fetch the product data.
        path: The directory path where the data will be saved.
    """

    crawler_class = CRAWLERS.get(chain)
    if not crawler_class:
        raise ValueError(f"Unknown retail chain: {chain}")

    crawler = crawler_class()
    t0 = time()
    try:
        stores = crawler.get_all_products(date)
    except Exception as err:
        logger.error(
            f"Error crawling {chain} for {date:%Y-%m-%d}: {err}", exc_info=True
        )
        return CrawlResult()

    if not stores:
        logger.error(f"No stores imported for {chain} on {date}")
        return CrawlResult()

    save_chain(path, stores)
    t1 = time()

    all_products = set()
    for store in stores:
        for product in store.items:
            all_products.add(product.product_id)

    return CrawlResult(
        elapsed_time=t1 - t0,
        n_stores=len(stores),
        n_products=len(all_products),
        n_prices=sum(len(store.items) for store in stores),
    )


def crawl(
    root: Path,
    date: datetime.date | None = None,
    chains: list[str] | None = None,
) -> Path:
    """
    Crawl multiple retail chains for product/pricing data and save it.

    Args:
        root: The base directory path where the data will be saved.
        date: The date for which to fetch the product data. If None, uses today's date.
        chains: List of retail chain names to crawl. If None, crawls all available chains.

    Returns:
        Path to the created ZIP archive file.
    """

    if chains is None:
        chains = get_chains()

    if date is None:
        date = datetime.date.today()

    path = root / date.strftime("%Y-%m-%d")
    zip_path = root / f"{date:%Y-%m-%d}.zip"
    os.makedirs(path, exist_ok=True)

    results = {}

    t0 = time()
    for chain in chains:
        logger.info(f"Starting crawl for {chain} on {date:%Y-%m-%d}")
        r = crawl_chain(chain, date, path / chain)
        results[chain] = r
    t1 = time()

    logger.info(f"Crawled {','.join(chains)} for {date:%Y-%m-%d} in {t1 - t0:.2f}s")
    for chain, r in results.items():
        logger.info(
            f"  * {chain}: {r.n_stores} stores, {r.n_products} products, {r.n_prices} prices in {r.elapsed_time:.2f}s"
        )

    # --- Database Import Step ---
    data_to_import_path = path # This is root / date.strftime("%Y-%m-%d")
    
    logger.info(f"Starting database import for data in {data_to_import_path}...")
    load_dotenv() # Load .env file for database credentials
    
    db_conn = None
    try:
        db_conn = importer_connect_db()
        if db_conn:
            logger.info("Successfully connected to database for import.")
            importer_create_tables(db_conn) # Ensure tables exist
            logger.info("Database tables ensured/created for import.")
            importer_import_data(str(data_to_import_path), db_conn) # Pass path as string
            logger.info(f"Database import completed successfully for {data_to_import_path}.")
        else:
            logger.error("Failed to connect to database for import. Skipping import.")
    except Exception as e:
        logger.error(f"Error during data import into database: {e}", exc_info=True)
        if db_conn:
            try:
                db_conn.rollback() # Ensure transaction is rolled back on error
                logger.info("Database transaction rolled back due to import error.")
            except Exception as rb_e:
                logger.error(f"Error during rollback: {rb_e}", exc_info=True)
    finally:
        if db_conn:
            try:
                db_conn.close()
                logger.info("Database connection closed after import attempt.")
            except Exception as close_e:
                logger.error(f"Error closing database connection: {close_e}", exc_info=True)
    # --- End of Database Import Step ---

    copy_archive_info(path)
    create_archive(path, zip_path)

    logger.info(f"Created archive {zip_path} with data for {date:%Y-%m-%d}")
    return zip_path
