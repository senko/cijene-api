import pytest
from unittest.mock import MagicMock, patch, call, mock_open
from pathlib import Path
import os
import hashlib # For test hash calculation if needed directly
import tempfile # For temporary directory
import shutil # For cleaning up temp directory
import csv # For writing test CSVs

# Assuming csv_processor.importer can be imported.
# This might require PYTHONPATH adjustments or installing csv_processor.
# For this context, we assume it's importable.
from csv_processor import importer

# --- Pytest Fixtures ---

@pytest.fixture
def mock_db_connection(mocker):
    """Mocks psycopg2.connect and returns a mock connection and cursor."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mocker.patch('psycopg2.connect', return_value=mock_conn)
    return mock_conn, mock_cursor

@pytest.fixture
def temp_csv_env_setup(mocker, tmp_path):
    """
    Creates a temporary directory structure for CSV files,
    mocks CSV_DIR and DATABASE_URL environment variables.
    Yields the base temporary path.
    """
    # tmp_path is a pytest fixture providing a Path object to a temporary directory
    
    # Mock environment variables
    mocker.patch.dict(os.environ, {
        "DATABASE_URL": "mock_db_url_for_testing",
        "CSV_DIR": str(tmp_path)
    })
    
    # Example: Create a chain directory and some CSV files
    chain1_dir = tmp_path / "chain_A"
    chain1_dir.mkdir()

    # stores.csv
    stores_content = [importer.STORES_CSV_HEADERS] + [
        ["store1", "Main Store", "supermarket", "123 Main St", "Anytown", "12345"]
    ]
    with open(chain1_dir / "stores.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(stores_content)

    # products.csv
    products_content = [importer.PRODUCTS_CSV_HEADERS] + [
        ["prod1", "barcode123", "Test Product 1", "TestBrand", "TestCat", "kg", "1"]
    ]
    with open(chain1_dir / "products.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(products_content)

    # prices.csv
    prices_content = [importer.PRICES_CSV_HEADERS] + [
        ["store1", "prod1", "9.99", "9.99", "", "", ""]
    ]
    with open(chain1_dir / "prices.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(prices_content)
        
    yield tmp_path # The test function can use this path to reference created files

    # Teardown (tmp_path is automatically cleaned by pytest)


# --- Test Cases ---

def test_main_happy_path(mocker, mock_db_connection, temp_csv_env_setup):
    """
    Test the main function for a successful processing of a new batch.
    """
    _, mock_cursor = mock_db_connection

    # Mock check_if_batch_processed to indicate it's a new batch
    mocker.patch('csv_processor.importer.check_if_batch_processed', return_value=False)
    
    # Mock process_batch itself, as its details are tested separately
    # We just want to see if main calls it correctly
    mock_process_batch_func = mocker.patch('csv_processor.importer.process_batch', return_value=True)

    # Expected batch identifier and file paths based on temp_csv_env_setup
    chain_name = "chain_A"
    base_csv_path = temp_csv_env_setup / chain_name
    expected_stores_csv = base_csv_path / "stores.csv"
    expected_products_csv = base_csv_path / "products.csv"
    expected_prices_csv = base_csv_path / "prices.csv"
    
    # Calculate hash as importer.main would
    # Need to mock open for calculate_files_hash if it's not using the temp files directly
    # For simplicity, let's assume calculate_files_hash works and mock its result or let it run
    # If calculate_files_hash is called by main, ensure it can read the temp files.
    # The temp_csv_env_setup fixture writes real files, so calculate_files_hash should work.
    
    importer.main()

    # Assertions
    mock_process_batch_func.assert_called_once()
    args, _ = mock_process_batch_func.call_args
    # args[0] is conn, args[1] is cursor, args[2] is chain_name
    # args[3] is stores_csv_path, args[4] is products_csv_path, args[5] is prices_csv_path
    assert args[2] == chain_name
    assert args[3] == expected_stores_csv
    assert args[4] == expected_products_csv
    assert args[5] == expected_prices_csv
    
    # Check if processed_batches table was updated
    # Example: Assert that an INSERT or ON CONFLICT statement was executed
    # This requires knowing the hash. Let's get it by calling the real function.
    actual_hash = importer.calculate_files_hash([expected_stores_csv, expected_products_csv, expected_prices_csv])
    
    # Look for the call to update processed_batches
    # The last call to cursor.execute (typically) after a successful process_batch
    # This depends on the exact sequence of operations in main()
    # A more robust way might be to mock the specific DB call for updating processed_batches
    
    found_update_call = False
    for db_call in mock_cursor.execute.call_args_list:
        sql = db_call[0][0] # First argument of the first positional argument
        params = db_call[0][1] if len(db_call[0]) > 1 else ()
        if "INSERT INTO processed_batches" in sql and params[0] == chain_name and params[1] == actual_hash:
            found_update_call = True
            break
    assert found_update_call, "processed_batches table was not updated correctly"
    mock_db_connection[0].commit.assert_called() # Check if commit was called on the connection


def test_check_if_batch_processed_new_batch(mocker):
    """Test check_if_batch_processed for a new batch (no existing record)."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None # Simulate no record found

    result = importer.check_if_batch_processed(mock_cursor, "new_batch", "new_hash")
    
    mock_cursor.execute.assert_called_once_with(
        "SELECT files_hash FROM processed_batches WHERE batch_identifier = %s",
        ("new_batch",)
    )
    assert result is False # Should return False, indicating batch needs processing


def test_check_if_batch_processed_existing_unchanged(mocker):
    """Test check_if_batch_processed for an existing, unchanged batch."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("same_hash",) # Simulate record found with same hash

    result = importer.check_if_batch_processed(mock_cursor, "existing_batch", "same_hash")
    
    assert result is True # Should return True, indicating batch should be skipped


def test_check_if_batch_processed_existing_changed(mocker):
    """Test check_if_batch_processed for an existing, but changed batch."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = ("old_hash",) # Simulate record found with different hash

    result = importer.check_if_batch_processed(mock_cursor, "changed_batch", "new_hash")
    
    assert result is False # Should return False, indicating batch needs reprocessing

# A more focused test for process_batch
def test_process_batch_core_logic(mocker):
    """Test the core database interactions of process_batch."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    
    # Mock return value for chain upsert
    mock_cursor.fetchone.return_value = (1,) # chain_id = 1

    # Mock read_csv_file to return controlled data
    # Stores data (includes 'name' as per importer's expectation)
    mock_stores_data = [
        {"store_id": "s1", "name": "Store Name 1", "type": "typeA", "address": "Addr1", "city": "City1", "zipcode": "zip1"}
    ]
    # Products data
    mock_products_data = [
        {"product_id": "p1", "barcode": "bc1", "name": "Prod Name 1", "brand": "BrandX", "category": "CatY", "unit": "pc", "quantity": "1", "date_added": "2023-01-01", "packaging": "box"}
    ]
    # Prices data
    mock_prices_data = [
        {"store_id": "s1", "product_id": "p1", "price": "10.99", "unit_price": "10.99"} # other optional fields omitted
    ]

    mocker.patch('csv_processor.importer.read_csv_file', side_effect=[
        iter(mock_stores_data),  # First call for stores.csv
        iter(mock_products_data), # Second call for products.csv
        iter(mock_prices_data)    # Third call for prices.csv
    ])

    # Dummy paths for the CSV files, as read_csv_file is mocked
    dummy_stores_path = Path("dummy/stores.csv")
    dummy_products_path = Path("dummy/products.csv")
    dummy_prices_path = Path("dummy/prices.csv")

    success = importer.process_batch(mock_conn, mock_cursor, "test_chain", 
                                     dummy_stores_path, dummy_products_path, dummy_prices_path)

    assert success is True
    mock_cursor.execute.assert_any_call('BEGIN') # Check transaction start

    # Chain upsert
    mock_cursor.execute.assert_any_call(
        "INSERT INTO chains (name) VALUES (%s) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING chain_id;",
        ("test_chain",)
    )
    
    # Store upsert - verify with expected data structure
    # importer.py uses row.get("column_name")
    store_row = mock_stores_data[0]
    expected_store_params = (
        store_row.get("store_id"), 1, store_row.get("name"), store_row.get("type"), 
        store_row.get("address"), store_row.get("city"), store_row.get("zipcode")
    )
    # Extract the actual call for stores insert/update
    stores_sql_call = next(c for c in mock_cursor.execute.call_args_list if "INSERT INTO stores" in c[0][0])
    assert "INSERT INTO stores" in stores_sql_call[0][0]
    assert stores_sql_call[0][1] == expected_store_params

    # Product upsert - verify
    product_row = mock_products_data[0]
    expected_product_params = (
        product_row.get("product_id"), product_row.get("barcode"), product_row.get("name"), product_row.get("brand"),
        product_row.get("category"), product_row.get("unit"), product_row.get("quantity"),
        product_row.get("packaging"), importer.to_date_or_none(product_row.get("date_added"))
    )
    products_sql_call = next(c for c in mock_cursor.execute.call_args_list if "INSERT INTO products" in c[0][0])
    assert "INSERT INTO products" in products_sql_call[0][0]
    assert products_sql_call[0][1] == expected_product_params

    # Price insert - verify
    price_row = mock_prices_data[0]
    expected_price_params = (
        price_row.get("store_id"), price_row.get("product_id"), importer.to_decimal_or_none(price_row.get("price")),
        importer.to_decimal_or_none(price_row.get("unit_price")),
        importer.to_decimal_or_none(price_row.get("best_price_30")), # Will be None if key missing
        importer.to_decimal_or_none(price_row.get("special_price")),# Will be None
        importer.to_decimal_or_none(price_row.get("anchor_price")), # Will be None
        price_row.get("anchor_price_date"), # Will be None
        importer.to_decimal_or_none(price_row.get("initial_price")) # Will be None
    )
    prices_sql_call = next(c for c in mock_cursor.execute.call_args_list if "INSERT INTO prices" in c[0][0])
    assert "INSERT INTO prices" in prices_sql_call[0][0]
    assert prices_sql_call[0][1] == expected_price_params
    
    mock_conn.commit.assert_called_once()


def test_read_csv_file_valid_data(tmp_path):
    """Test read_csv_file with a valid CSV file."""
    csv_file = tmp_path / "test.csv"
    headers = ["id", "name", "value"]
    rows_data = [
        {"id": "1", "name": "item1", "value": "100"},
        {"id": "2", "name": "item2", "value": "200"},
    ]
    
    # Write test CSV
    with open(csv_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows_data)

    # Expected output (list of dicts)
    expected_output = list(rows_data) # DictWriter writes strings, DictReader reads them as strings
    
    # Read using the function
    # importer.py's read_csv_file expects expected_headers to be a list of strings
    # It then uses the file's actual header for DictReader
    result = list(importer.read_csv_file(csv_file, headers)) 
    
    assert result == expected_output


def test_read_csv_file_header_mismatch(tmp_path, caplog):
    """Test read_csv_file when headers in file don't match all expected headers."""
    csv_file = tmp_path / "bad_header.csv"
    file_headers = ["id", "name"] # Missing "value"
    expected_headers_param = ["id", "name", "value"] # What the function expects
    
    with open(csv_file, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(file_headers)
        writer.writerow(["1", "item1"])

    # Call the function and collect results (should be empty due to mismatch)
    result = list(importer.read_csv_file(csv_file, expected_headers_param))
    
    assert not result # No data should be yielded
    assert "Header mismatch" in caplog.text # Check for log warning
    assert f"Expected: {expected_headers_param}" in caplog.text
    assert f"Got: {file_headers}" in caplog.text


def test_read_csv_file_empty(tmp_path, caplog):
    """Test read_csv_file with an empty CSV file."""
    csv_file = tmp_path / "empty.csv"
    csv_file.touch() # Create empty file

    expected_headers = ["id", "name"]
    result = list(importer.read_csv_file(csv_file, expected_headers))

    assert not result
    assert f"CSV file is empty: {csv_file}" in caplog.text


def test_read_csv_file_not_found(tmp_path, caplog):
    """Test read_csv_file with a non-existent CSV file."""
    csv_file = tmp_path / "non_existent.csv"
    expected_headers = ["id", "name"]
    result = list(importer.read_csv_file(csv_file, expected_headers))

    assert not result
    assert f"CSV file not found: {csv_file}" in caplog.text

# Further tests could include:
# - test_main_skip_processed_batch
# - test_main_reprocess_changed_batch
# - test_main_missing_csv_files
# - test_process_batch_rollback_on_error
# - test_calculate_files_hash
# - test_to_decimal_or_none, test_to_date_or_none for various inputs
# - More edge cases for read_csv_file
# - Tests for logging at different points
# - Test for handling of DATABASE_URL or CSV_DIR not being set
# - Test for different CSV content (e.g. empty values for optional fields)
# - Test for case where chain upsert fails to return chain_id in process_batch

# Example for testing optional fields in products (within process_batch or a dedicated test)
def test_process_batch_handles_optional_product_fields(mocker):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (1,) # chain_id

    # Product data with some optional fields missing or empty
    mock_products_data = [
        {"product_id": "p1", "barcode": "bc1", "name": "Prod Name 1", 
         "brand": "BrandX", "category": "CatY", "unit": "pc", "quantity": "1", 
         "date_added": "", "packaging": None} # Empty date_added, packaging is None
    ]
    mocker.patch('csv_processor.importer.read_csv_file', side_effect=[
        iter([]), # stores
        iter(mock_products_data), # products
        iter([])  # prices
    ])

    importer.process_batch(mock_conn, mock_cursor, "test_chain", Path("s.csv"), Path("p.csv"), Path("pr.csv"))
    
    product_row = mock_products_data[0]
    expected_product_params = (
        product_row.get("product_id"), product_row.get("barcode"), product_row.get("name"), product_row.get("brand"),
        product_row.get("category"), product_row.get("unit"), product_row.get("quantity"),
        None,  # packaging should be None
        None   # date_added should be None (converted from empty string)
    )
    
    found_product_call = False
    for call_args in mock_cursor.execute.call_args_list:
        sql = call_args[0][0]
        params = call_args[0][1]
        if "INSERT INTO products" in sql:
            assert params == expected_product_params
            found_product_call = True
            break
    assert found_product_call, "Product insert call not found or params mismatch for optional fields"

```
