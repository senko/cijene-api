import datetime
import logging
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from time import time
from typing import List


from crawler.store.dm import DmCrawler
from crawler.store.eurospin import EurospinCrawler
from crawler.store.kaufland import KauflandCrawler
from crawler.store.konzum import KonzumCrawler
from crawler.store.ktc import KtcCrawler
from crawler.store.lidl import LidlCrawler
from crawler.store.metro import MetroCrawler
from crawler.store.ntl import NtlCrawler
from crawler.store.plodine import PlodineCrawler
from crawler.store.ribola import RibolaCrawler
from crawler.store.spar import SparCrawler
from crawler.store.studenac import StudenacCrawler
from crawler.store.tommy import TommyCrawler
from crawler.store.trgocentar import TrgocentarCrawler
from crawler.store.vrutak import VrutakCrawler
from crawler.store.zabac import ZabacCrawler
from crawler.store.output import (
    copy_archive_info,
    create_archive,
    save_chain,
    save_to_db,
)
from crawler.store.from_csv import load_from_csv

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


def crawl_chain(
    chain: str, date: datetime.date, path: Path, process_db: bool = False
) -> CrawlResult:
    """
    Crawl a specific retail chain for product/pricing data and save it.

    Args:
        chain: The name of the retail chain to crawl.
        date: The date for which to fetch the product data.
        path: The directory path where the data will be saved.
        process_db: Also save the data to the database if True.
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

    logger.info(
        f"Path is {path}, saving {len(stores)} stores for {chain} on {date:%Y-%m-%d}"
    )

    if process_db:
        save_to_db(date, stores)

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
    process_db: bool = False,
) -> Path:
    """
    Crawl multiple retail chains for product/pricing data and save it.

    Args:
        root: The base directory path where the data will be saved.
        date: The date for which to fetch the product data. If None, uses today's date.
        chains: List of retail chain names to crawl. If None, crawls all available chains.
        process_db: Also save the data to the database if True.

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
        r = crawl_chain(chain, date, path / chain, process_db)
        results[chain] = r
    t1 = time()

    logger.info(f"Crawled {','.join(chains)} for {date:%Y-%m-%d} in {t1 - t0:.2f}s")
    for chain, r in results.items():
        logger.info(
            f"  * {chain}: {r.n_stores} stores, {r.n_products} products, {r.n_prices} prices in {r.elapsed_time:.2f}s"
        )

    copy_archive_info(path)
    create_archive(path, zip_path)

    logger.info(f"Created archive {zip_path} with data for {date:%Y-%m-%d}")
    return zip_path


def crawl_from_csv(
    root: Path,
    date: datetime.date | None = None,
    chains: list[str] | None = None,
    csv_dir: Path | None = None,
    process_db: bool = False,
) -> Path:
    """
    Load data from CSV files for specified chains and date.

    Args:
        root: Base output directory for saving data.
        date: Date for which to load data (YYYY-MM-DD). Defaults to today if None.
        chains: List of chain slugs to process. Defaults to all chains if None.
        csv_dir: Root directory containing date/chain subdirectories with CSV files.
    process_db: Whether to save data to DB.

    Returns:
        Path to the created ZIP archive file.
    """
    if chains is None:
        chains = get_chains()

    if date is None:
        date = datetime.date.today()

    date_str = date.strftime("%Y-%m-%d")
    path = root / date_str
    zip_path = root / f"{date_str}.zip"
    os.makedirs(path, exist_ok=True)

    results: dict[str, CrawlResult] = {}
    start_all = time()
    for chain in chains:
        logger.info(f"Loading CSV data for {chain} on {date_str}")
        t0 = time()
        try:
            stores = load_from_csv(chain, date, csv_dir)
        except FileNotFoundError:
            logger.info(f"CSV data for {chain} on {date_str} not found; skipping")
            results[chain] = CrawlResult()
            continue
        except Exception as err:
            logger.error(
                f"Error loading CSV for {chain} on {date_str}: {err}", exc_info=True
            )
            results[chain] = CrawlResult()
            continue

        if process_db:
            save_to_db(date, stores)

        save_chain(path / chain, stores)
        elapsed = time() - t0
        n_stores = len(stores)
        n_products = len({item.product_id for s in stores for item in s.items})
        n_prices = sum(len(s.items) for s in stores)
        results[chain] = CrawlResult(
            elapsed_time=elapsed,
            n_stores=n_stores,
            n_products=n_products,
            n_prices=n_prices,
        )

    total_elapsed = time() - start_all
    logger.info(
        f"Loaded CSV for {','.join(chains)} for {date_str} in {total_elapsed:.2f}s"
    )
    for chain, res in results.items():
        logger.info(
            f"  * {chain}: {res.n_stores} stores, {res.n_products} products, {res.n_prices} prices in {res.elapsed_time:.2f}s"
        )

    # copy archive-info.txt from source if available, else use default
    src_info = Path(csv_dir) / date_str / "archive-info.txt"
    if src_info and src_info.exists():
        shutil.copy(src_info, path / "archive-info.txt")
    else:
        copy_archive_info(path)
    create_archive(path, zip_path)
    logger.info(f"Created archive {zip_path} with CSV data for {date_str}")
    return zip_path
