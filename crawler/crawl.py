from dataclasses import dataclass
import os
import datetime
import gc
from typing import List
import logging
from pathlib import Path
from time import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError


from crawler.store.konzum import KonzumCrawler
from crawler.store.lidl import LidlCrawler
from crawler.store.plodine import PlodineCrawler
from crawler.store.ribola import RibolaCrawler
from crawler.store.roto import RotoCrawler
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
from crawler.store.trgovina_krk import TrgovinaKrkCrawler
from crawler.store.brodokomerc import BrodokomercCrawler
from crawler.store.lorenco import LorencoCrawler
from crawler.store.boso import BosoCrawler


from crawler.store.output import save_chain, copy_archive_info, create_archive

logger = logging.getLogger(__name__)

# Timeout configurations (in seconds)
OVERALL_TIMEOUT_SECONDS = 1200  # 10 minutes for entire crawl process
CHAIN_TIMEOUT_SECONDS = 120  # 2 minutes per individual chain

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
    RotoCrawler.CHAIN: RotoCrawler,
    TrgovinaKrkCrawler.CHAIN: TrgovinaKrkCrawler,
    BrodokomercCrawler.CHAIN: BrodokomercCrawler,
    LorencoCrawler.CHAIN: LorencoCrawler,
    BosoCrawler.CHAIN: BosoCrawler,
}


@dataclass
class CrawlResult:
    elapsed_time: float = 0
    n_stores: int = 0
    n_products: int = 0
    n_prices: int = 0


def crawl_chain_with_cleanup(
    chain: str, date: datetime.date, path: Path
) -> CrawlResult:
    """
    Crawl a specific retail chain with cleanup.
    This is a wrapper that adds garbage collection after each chain completes.

    Args:
        chain: The name of the retail chain to crawl.
        date: The date for which to fetch the product data.
        path: The directory path where the data will be saved.

    Returns:
        CrawlResult with crawling statistics.
    """
    try:
        logger.info(f"Starting crawl for {chain} on {date:%Y-%m-%d}")
        result = crawl_chain(chain, date, path)
        logger.info(
            f"Completed crawl for {chain}: {result.n_stores} stores, {result.n_products} products, {result.n_prices} prices in {result.elapsed_time:.2f}s"
        )
        return result
    except Exception as e:
        logger.error(f"Failed to crawl {chain}: {e}", exc_info=True)
        return CrawlResult()  # Empty result for failed crawls
    finally:
        # Force garbage collection after each chain completes
        gc.collect()


def get_chains() -> List[str]:
    """
    Get the list of retail chains from the crawlers.

    Returns:
        List of retail chain names.
    """
    return list(CRAWLERS.keys())


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

    logger.info(f"Starting parallel crawl of {len(chains)} chains")

    t0 = time()

    # Process chains in parallel using ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=len(chains)) as executor:
        # Submit chain crawling tasks
        future_to_chain = {
            executor.submit(crawl_chain_with_cleanup, chain, date, path / chain): chain
            for chain in chains
        }

        # Collect results as they complete with timeout handling
        try:
            for future in as_completed(
                future_to_chain, timeout=OVERALL_TIMEOUT_SECONDS
            ):
                chain = future_to_chain[future]
                try:
                    result = future.result(timeout=CHAIN_TIMEOUT_SECONDS)
                    results[chain] = result
                except TimeoutError:
                    logger.error(
                        f"Chain '{chain}' timed out after {CHAIN_TIMEOUT_SECONDS // 60} minutes"
                    )
                    results[chain] = CrawlResult()
                except Exception as e:
                    logger.error(f"Failed to crawl {chain}: {e}", exc_info=True)
                    results[chain] = CrawlResult()
        except TimeoutError:
            logger.error(
                f"Overall crawling process timed out after {OVERALL_TIMEOUT_SECONDS // 60} minutes"
            )
            # Add empty results for any remaining chains
            for chain in chains:
                if chain not in results:
                    results[chain] = CrawlResult()

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
