import pytest
from pathlib import Path
import datetime
from unittest.mock import patch, MagicMock, call # Added call for more specific assertions if needed

from crawler.crawl import crawl, CrawlResult # Assuming CrawlResult is defined here or importable

# Test the integration of crawl function with the DB importer logic
@patch('crawler.crawl.load_dotenv')
@patch('crawler.crawl.importer_import_data')
@patch('crawler.crawl.importer_create_tables')
@patch('crawler.crawl.importer_connect_db')
@patch('crawler.crawl.create_archive') 
@patch('crawler.crawl.copy_archive_info') 
@patch('crawler.crawl.crawl_chain') 
def test_crawl_triggers_importer(
    mock_crawl_chain, mock_copy_info, mock_create_archive,
    mock_importer_connect_db, mock_importer_create_tables, 
    mock_importer_import_data, mock_load_dotenv, tmp_path: Path
):
    """
    Tests that the main crawl() function correctly calls the database importer functions
    after the crawling phase.
    """
    # 1. Setup Mocks
    # Mock for the database connection object
    mock_db_conn = MagicMock()
    mock_importer_connect_db.return_value = mock_db_conn
    
    # Mock for crawl_chain to return a dummy CrawlResult
    mock_crawl_chain.return_value = CrawlResult(elapsed_time=1.0, n_stores=1, n_products=10, n_prices=100)

    # 2. Define Test Parameters
    test_date = datetime.date(2023, 10, 26)
    test_chains = ["dummy_chain_1", "dummy_chain_2"]
    root_output_path = tmp_path
    
    # Expected path that will be passed to the importer
    date_str_folder = test_date.strftime("%Y-%m-%d")
    expected_data_import_path = root_output_path / date_str_folder

    # 3. Create Dummy Directory Structure that importer_import_data expects
    # importer_import_data uses glob.glob(os.path.join(abs_base_data_path, "*"))
    # and then os.path.isdir(d). So, it expects subdirectories for chains.
    # It also expects stores.csv, products.csv, prices.csv within those chain dirs.
    for chain_name in test_chains:
        chain_path = expected_data_import_path / chain_name
        chain_path.mkdir(parents=True, exist_ok=True)
        # Create dummy files that get_file_hash and subsequent processing might expect
        # Even if content processing is mocked/skipped, file existence might be checked.
        (chain_path / "stores.csv").touch()
        (chain_path / "products.csv").touch()
        (chain_path / "prices.csv").touch()

    # 4. Call the main crawl() function
    crawl(root=root_output_path, date=test_date, chains=test_chains)

    # 5. Assertions
    # Assert that load_dotenv was called (it's in crawl.py before importer calls)
    mock_load_dotenv.assert_called_once()
    
    # Assert that the database connection was attempted
    mock_importer_connect_db.assert_called_once()
    
    # Assert that create_tables was called with the mock connection
    mock_importer_create_tables.assert_called_once_with(mock_db_conn)
    
    # Assert that import_data was called with the correct path and connection
    # The path passed to importer_import_data should be a string version of expected_data_import_path
    mock_importer_import_data.assert_called_once_with(str(expected_data_import_path), mock_db_conn)
    
    # Assert that the database connection was closed
    mock_db_conn.close.assert_called_once()
    
    # Assert that crawl_chain was called for each chain
    assert mock_crawl_chain.call_count == len(test_chains)
    expected_crawl_chain_calls = []
    for chain_name in test_chains:
        # The path passed to crawl_chain is root_output_path / date_str_folder / chain_name
        expected_chain_output_path = expected_data_import_path / chain_name
        expected_crawl_chain_calls.append(call(chain_name, test_date, expected_chain_output_path))
    # mock_crawl_chain.assert_has_calls(expected_crawl_chain_calls, any_order=True) # any_order=True if order doesn't matter

    # Assert that archiving functions were called (as they are after importer)
    mock_copy_info.assert_called_once_with(expected_data_import_path)
    expected_zip_path = root_output_path / f"{date_str_folder}.zip"
    mock_create_archive.assert_called_once_with(expected_data_import_path, expected_zip_path)
```
