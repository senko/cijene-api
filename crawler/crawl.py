import datetime
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from time import time
from typing import List

from dotenv import load_dotenv

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
from crawler.store.mocky import MockCrawler

logger = logging.getLogger(__name__)
load_dotenv()

IS_MOCK_MODE = os.getenv("MOCK_ENABLED", "").lower() == "true"

if IS_MOCK_MODE:
    logger.warning("Running in MOCK mode. Only MockCrawler will be available.")
    CRAWLERS = {
        MockCrawler.CHAIN: MockCrawler,
    }
else:
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
    chain: str, date: datetime.date, path: Path, output_format: str
) -> CrawlResult:
    # crawl(args.output_path, crawl_date, chains_to_crawl, output_format="sql")
    """
    Crawl a specific retail chain for product/pricing data and save it.

    Args:
        chain: The name of the retail chain to crawl.
        date: The date for which to fetch the product data.
        path: The directory path where the data will be saved.
        output_format: The format in which to save the data.
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
    if output_format == "sql":
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
    output_format: str = "csv",
) -> Path | None:
    """
    Crawl multiple retail chains for product/pricing data and save it.

    Args:
        root: The base directory path where the data will be saved.
        date: The date for which to fetch the product data. If None, uses today's date.
        chains: List of retail chain names to crawl. If None, crawls all available chains.
        output_format: The format in which to save the data.

    Returns:
        Path to the created ZIP archive file or None if output_format is 'sql'.
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
        r = crawl_chain(chain, date, path / chain, output_format)
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
