import os
import datetime
from pathlib import Path
import importlib


def test_crawl_with_mock_env_variable(monkeypatch, tmp_path):
    """
    Tests that crawler.crawl uses MockCrawler when MOCK_ENABLED is 'true'.
    """
    monkeypatch.setenv("MOCK_ENABLED", "true")

    from crawler import crawl

    importlib.reload(crawl)  # to pick up the new env var

    assert len(crawl.CRAWLERS) == 1  # only MockCrawler
    assert crawl.MockCrawler.CHAIN in crawl.CRAWLERS
    assert crawl.CRAWLERS[crawl.MockCrawler.CHAIN] == crawl.MockCrawler
    print(f"\nCrawlers loaded: {crawl.CRAWLERS}")

    chains = crawl.get_chains()
    assert chains == [crawl.MockCrawler.CHAIN]
    print(f"Available chains: {chains}, expected: {crawl.MockCrawler.CHAIN}")

    test_date = datetime.date(2025, 1, 1)
    output_dir = tmp_path / "crawl_output" / test_date.strftime("%Y-%m-%d")
    chain_output_dir = output_dir / crawl.MockCrawler.CHAIN
    chain_output_dir.mkdir(parents=True, exist_ok=True)

    if hasattr(crawl, "crawl_chain"):
        result = crawl.crawl_chain(
            chain=crawl.MockCrawler.CHAIN,
            date=test_date,
            path=chain_output_dir,
            output_format="sql",
        )
        print(f"Crawl result for {crawl.MockCrawler.CHAIN}: {result}")

        created_files = list(chain_output_dir.glob("*csv"))
        print(f"Found output files: {[f.name for f in created_files]}")
        assert len(created_files) == 3, f"No output file found in {chain_output_dir}"

    else:
        print("crawl.crawl_chain function not found, skipping that part of the test.")


def test_crawl_with_normal_env_variable(monkeypatch):
    """
    Tests that crawler.crawl uses real crawlers when MOCK_ENABLED is not 'true'.
    """
    monkeypatch.delenv("MOCK_ENABLED", raising=False)  # Remove if exists

    from crawler import crawl
    importlib.reload(crawl)

    assert len(crawl.CRAWLERS) > 1  # not just the mock
    assert (
        crawl.MockCrawler.CHAIN not in crawl.CRAWLERS
    )  # please no MockCrawler in normal mode
    print(f"Crawlers loaded (normal mode): {len(crawl.CRAWLERS)}")


# To run this test:
# 1. Make sure you are in the root directory of your project (/opt/cijene-api).
# 2. Run pytest:
#    pytest tests/test_mock_crawl.py -s -v
#    (-s shows print statements, -v is for verbose output)
