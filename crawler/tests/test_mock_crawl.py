import datetime
import importlib
import os
import sys
from typing import (
    Any,
    Dict,
    Generator,
    Protocol,
    cast,
    runtime_checkable,
)
from typing import (
    Type as TypingType,
)
from unittest.mock import patch

import pytest
from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker as SQLASessionMaker

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, project_root)

from crawler.db.model import (  # noqa: E402
    Base,
    Chain,
    Product,
    StoreProduct,
)
from crawler.tests.mocky import MockCrawler  # noqa: E402


@runtime_checkable
class CrawlModuleProto(Protocol):
    CRAWLERS: Dict[str, TypingType[MockCrawler]]

    def get_chains(self) -> list[str]: ...
    def crawl_chain(
        self, chain: str, date: datetime.date, path: Any, process_db: bool
    ) -> Any: ...


@pytest.fixture(autouse=True)
def patched_db_uri(monkeypatch):
    """
    Patches the SQLALCHEMY_DATABASE_URI environment variable for tests.
    """
    test_uri_from_env = os.getenv("SQLALCHEMY_DATABASE_URI_TEST")
    final_test_uri = None

    if test_uri_from_env:
        final_test_uri = test_uri_from_env
        print(f"INFO: Using SQLALCHEMY_DATABASE_URI_TEST: {final_test_uri}")
    else:
        final_test_uri = "sqlite:///tmp/crawler_test.db"

    monkeypatch.setenv("SQLALCHEMY_DATABASE_URI", final_test_uri)
    print(
        f"INFO: SQLALCHEMY_DATABASE_URI temporarily set to {final_test_uri} for testing."
    )


@pytest.fixture
def db_engine(patched_db_uri) -> Engine:
    current_db_uri = os.getenv("SQLALCHEMY_DATABASE_URI")
    assert current_db_uri is not None, (
        "SQLALCHEMY_DATABASE_URI not set by patched_db_uri"
    )
    engine = create_engine(current_db_uri)
    Base.metadata.create_all(engine)  # create tables if we don't have it yet
    return engine


@pytest.fixture(autouse=True)
def db_session(
    SqlAlchemyTestSession: SQLASessionMaker[Session],
) -> Generator[Session, None, None]:
    session: Session = SqlAlchemyTestSession()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture
def SqlAlchemyTestSession(db_engine: Engine) -> SQLASessionMaker[Session]:
    return SQLASessionMaker(bind=db_engine, class_=Session)


# from crawler.tests.mocky import MockCrawler  # noqa: E402


@pytest.fixture(autouse=True)
def crawl() -> Generator[CrawlModuleProto, None, None]:
    """
    Provides the 'crawler.crawl' module with its CRAWLERS dictionary
    patched to only include MockCrawler.
    Reloads the module for each test to ensure a clean state before patching.
    """
    crawl_module_obj: Any
    if "crawler.crawl" in sys.modules:
        crawl_module_obj = importlib.reload(sys.modules["crawler.crawl"])
    else:
        from crawler import crawl as raw_crawl_module

        crawl_module_obj = raw_crawl_module

    typed_crawl_module = cast(CrawlModuleProto, crawl_module_obj)

    with patch.dict(
        typed_crawl_module.CRAWLERS,
        {MockCrawler.CHAIN: MockCrawler},
        clear=True,
    ):
        assert len(typed_crawl_module.CRAWLERS) == 1, (
            "CRAWLERS dict should only have MockCrawler"
        )
        assert MockCrawler.CHAIN in typed_crawl_module.CRAWLERS, (
            "MockCrawler.CHAIN key missing in CRAWLERS"
        )
        assert typed_crawl_module.CRAWLERS[MockCrawler.CHAIN] == MockCrawler, (
            "CRAWLERS not patched with MockCrawler instance"
        )
        print(f"\nINFO (fixture): CRAWLERS patched: {typed_crawl_module.CRAWLERS}")
        yield typed_crawl_module


def delete_all_data(db_session: Session):
    engine = db_session.get_bind()
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)


def prepare_crawl_test_environment(
    base_tmp_path, test_date: datetime.date, chain_name: str = MockCrawler.CHAIN
):
    output_dir = base_tmp_path / "crawl_output" / test_date.strftime("%Y-%m-%d")
    chain_specific_output_dir = output_dir / chain_name
    chain_specific_output_dir.mkdir(parents=True, exist_ok=True)
    return test_date, chain_specific_output_dir


def test_crawl(crawl: CrawlModuleProto, tmp_path, db_session: Session):
    delete_all_data(db_session)
    chains = crawl.get_chains()
    assert chains == [MockCrawler.CHAIN], f"Expected chains to be {[MockCrawler.CHAIN]}"
    print(f"Available chains: {chains}, expected: {MockCrawler.CHAIN}")

    test_date, chain_output_dir = prepare_crawl_test_environment(
        tmp_path, MockCrawler.DATE_SIM_START
    )

    if hasattr(crawl, "crawl_chain"):
        result = crawl.crawl_chain(
            chain=MockCrawler.CHAIN,
            date=test_date,
            path=chain_output_dir,
            process_db=True,
        )
        print(f"Crawl result for {MockCrawler.CHAIN}: {result}")

        created_files = list(chain_output_dir.glob("*csv"))
        print(f"Found output files: {[f.name for f in created_files]}")
        assert len(created_files) == 3, (
            f"Expected 3 output files in {chain_output_dir}, found {len(created_files)}"
        )
    else:
        print("crawl.crawl_chain function not found, skipping that part of the test.")

    # db CHAINS
    chains = db_session.query(Chain).all()
    print(f"Chains in DB after crawl: {[chain.name for chain in chains]}")
    assert len(chains) == 1, "No chains found in the database after crawl."
    # db STORES
    print(f"Stores in DB after crawl: {[store.ext_name for store in chains[0].stores]}")
    assert len(chains[0].stores) == 2, "No stores found in the database after crawl."
    # db PRODUCTS
    products = db_session.query(Product).all()
    print(
        f"Product EANs in DB after crawl: {[product.barcode for product in products]}"
    )
    assert len(products) == 4, (
        "Wrong number of products found in the database after crawl."
    )
    # db STORE PRODUCTS
    assert len(chains[0].stores[0].products) == 3, (
        "Wrong number of products found in the database after crawl."
    )
    assert len(chains[0].stores[1].products) == 3, (
        "Wrong number of products found in the database after crawl."
    )


def test_db_new_store(crawl: CrawlModuleProto, tmp_path, db_session: Session):
    """
    New store - new record in db
    - we get store that is not in the db
    - test if we have it in the db
    """
    delete_all_data(db_session)

    test_date, chain_output_dir = prepare_crawl_test_environment(
        tmp_path, MockCrawler.DATE_SIM_START
    )

    crawl.crawl_chain(
        chain=MockCrawler.CHAIN,
        date=test_date,
        path=chain_output_dir,
        process_db=True,
    )
    chains = db_session.query(Chain).all()
    print(
        f"Stores in DB after initial crawl: {[store.ext_name for store in chains[0].stores]}"
    )
    assert len(chains[0].stores) == 2, (
        "Initial stores not found in the database after crawl."
    )

    test_date, chain_output_dir = prepare_crawl_test_environment(
        tmp_path, MockCrawler.DATE_MOCKY3_STORE_ADDED
    )

    crawl.crawl_chain(
        chain=MockCrawler.CHAIN,
        date=test_date,
        path=chain_output_dir,
        process_db=True,
    )
    db_session.refresh(chains[0])
    print(
        f"Stores in DB after new store crawl: {[store.ext_name for store in chains[0].stores]}"
    )
    assert len(chains[0].stores) == 3, (
        "New store not added to the database after crawl."
    )


def test_db_store_info_changed(monkeypatch, tmp_path):
    """
    Store info changed - updated record in db
    """
    assert True


def test_db_new_chain(monkeypatch, tmp_path):
    """
    New chain - new record in db
    """
    assert True


def test_db_chain_info_changed(monkeypatch, tmp_path):
    """
    Chain info changed - updated record in db
    """
    assert True


def test_db_new_product(monkeypatch, tmp_path):
    """
    New product in global table - new record in db
    """
    assert True


def test_db_product_info_changed(monkeypatch, tmp_path):
    """
    Product info changed - updated record in db
    As we have the same products in different chains, we need to decide how to handle this.
    """
    assert True


def test_db_new_store_product(monkeypatch, tmp_path):
    """
    New store product - new record in db
    New store product price - new record in db
    """
    assert True


def test_db_store_product_info_changed(monkeypatch, tmp_path):
    """
    Store product info changed - updated record in db
    """
    assert True


def test_db_product_price_changed(monkeypatch, tmp_path):
    """
    Product price changed - updated record in db
    """
    assert True


def test_db_product_price_not_changed(monkeypatch, tmp_path):
    """
    Product price not changed - no new record in db
    We will use price from the previous record
    """
    assert True


def test_db_no_valid_ean_replacement(monkeypatch, crawl, tmp_path, db_session):
    """
    No valid EAN - test replacement logic.
    Checks if a product with an invalid EAN is saved with a generated placeholder EAN.
    """
    delete_all_data(db_session)

    test_date, chain_output_dir = prepare_crawl_test_environment(
        tmp_path, MockCrawler.DATE_SIM_START
    )

    crawl.crawl_chain(
        chain=MockCrawler.CHAIN,
        date=test_date,
        path=chain_output_dir,
        process_db=True,
    )

    expected_placeholder_ean = f"{MockCrawler.CHAIN}:STABLE_NO_EAN"

    product_in_db = (
        db_session.query(Product)
        .filter_by(barcode=expected_placeholder_ean)
        .one_or_none()
    )

    assert product_in_db is not None, (
        f"Product with placeholder EAN '{expected_placeholder_ean}' not found in DB."
    )

    store_products_with_placeholder_ean = (
        db_session.query(StoreProduct).filter_by(barcode=expected_placeholder_ean).all()
    )
    assert len(store_products_with_placeholder_ean) > 0, (
        f"No StoreProduct entries found with placeholder EAN '{expected_placeholder_ean}'."
    )
    print(f"StoreProducts with placeholder EAN: {store_products_with_placeholder_ean}")


def test_db_ean_replacement(monkeypatch, tmp_path):
    """
    If we used EAN replacement (chain/store was sending invalid one),
    but now we got valid EAN - we should update the record with the new EAN
    or use the existing one that has the same EAN (from another chain/store).
    We need to check first for replacement if it exists (chain:product_id),
    """

    # in products: we have product with chain:product_id, now we have valid EAN
    # EAN not in products - we should update the record with the new real EAN
    assert True

    # in products: we have product with chain:product_id, now we have valid EAN
    # EAN in products - we should use the existing one that has the same EAN
    assert True
