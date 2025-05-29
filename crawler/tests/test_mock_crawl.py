import datetime
import importlib
import sys
import os
from unittest.mock import patch

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, project_root)

from crawler.tests.mocky import MockCrawler  # noqa: E402


def test_crawl(monkeypatch, tmp_path):
    monkeypatch.setenv("MOCK_ENABLED", "true")

    if "crawler.crawl" in sys.modules:
        crawl_module = importlib.reload(sys.modules["crawler.crawl"])
    else:
        from crawler import crawl as crawl_module

    with patch.dict(
        crawl_module.CRAWLERS, {MockCrawler.CHAIN: MockCrawler}, clear=True
    ):
        assert len(crawl_module.CRAWLERS) == 1  # only MockCrawler
        assert MockCrawler.CHAIN in crawl_module.CRAWLERS
        assert crawl_module.CRAWLERS[MockCrawler.CHAIN] == MockCrawler
        print(f"\nCrawlers loaded: {crawl_module.CRAWLERS}")

        chains = crawl_module.get_chains()
        assert chains == [MockCrawler.CHAIN]
        print(f"Available chains: {chains}, expected: {MockCrawler.CHAIN}")

        test_date = datetime.date(2025, 1, 1)
        output_dir = tmp_path / "crawl_output" / test_date.strftime("%Y-%m-%d")
        chain_output_dir = output_dir / MockCrawler.CHAIN
        chain_output_dir.mkdir(parents=True, exist_ok=True)

        if hasattr(crawl_module, "crawl_chain"):
            result = crawl_module.crawl_chain(
                chain=MockCrawler.CHAIN,
                date=test_date,
                path=chain_output_dir,
                output_format="sql",
            )
            print(f"Crawl result for {MockCrawler.CHAIN}: {result}")

            created_files = list(chain_output_dir.glob("*csv"))
            print(f"Found output files: {[f.name for f in created_files]}")
            assert (
                len(created_files) == 3
            ), f"No output file found in {chain_output_dir}"
        else:
            print(
                "crawl_module.crawl_chain function not found, skipping that part of the test."
            )
