import pytest
from unittest.mock import patch, mock_open, MagicMock, call
from decimal import Decimal, InvalidOperation
import hashlib

# Import functions from the script to be tested
from crawler.cli import db_importer

# --- Tests for get_file_hash ---
def test_get_file_hash_success():
    """Test get_file_hash with mocked file content."""
    mock_file_content = b"line1\nline2\n"
    expected_hash = hashlib.sha256(mock_file_content).hexdigest()

    # Mock open() to return a mock file object that simulates read()
    m_open = mock_open(read_data=mock_file_content)
    with patch('builtins.open', m_open):
        actual_hash = db_importer.get_file_hash("dummy_path.txt")

    assert actual_hash == expected_hash
    m_open.assert_called_once_with("dummy_path.txt", "rb")

def test_get_file_hash_file_not_found():
    """Test get_file_hash when file does not exist."""
    m_open = mock_open()
    m_open.side_effect = FileNotFoundError
    with patch('builtins.open', m_open):
        assert db_importer.get_file_hash("non_existent.txt") is None

def test_get_file_hash_other_exception():
    """Test get_file_hash with a generic exception during file reading."""
    m_open = mock_open()
    m_open.side_effect = IOError("Disk error")
    with patch('builtins.open', m_open):
        assert db_importer.get_file_hash("any_file.txt") is None

# --- Tests for connect_db ---
@patch('crawler.cli.db_importer.psycopg2')
@patch('crawler.cli.db_importer.os.getenv')
def test_connect_db_uses_env_vars(mock_getenv, mock_psycopg2):
    """Test connect_db calls psycopg2.connect with credentials from environment variables."""
    # Setup mock environment variables
    # For db_importer, these are POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
    env_vars = {
        "DB_HOST": "testhost",
        "DB_PORT": "5433",
        "POSTGRES_DB": "testdb",
        "POSTGRES_USER": "testuser",
        "POSTGRES_PASSWORD": "testpass"
    }
    mock_getenv.side_effect = lambda key, default=None: env_vars.get(key, default)
    
    mock_conn = MagicMock()
    mock_psycopg2.connect.return_value = mock_conn

    conn = db_importer.connect_db()

    mock_psycopg2.connect.assert_called_once_with(
        host="testhost",
        port="5433",
        dbname="testdb",
        user="testuser",
        password="testpass"
    )
    assert conn == mock_conn

@patch('crawler.cli.db_importer.psycopg2')
@patch('crawler.cli.db_importer.os.getenv')
# @patch('crawler.cli.db_importer.sys.exit') # sys.exit is not called in the new connect_db for library use
def test_connect_db_failure(mock_getenv, mock_psycopg2): # Removed mock_sys_exit
    """Test connect_db handles OperationalError and returns None."""
    env_vars = { 
        "DB_HOST": "testhost",
        "DB_PORT": "5432",
        "POSTGRES_DB": "testdb",
        "POSTGRES_USER": "testuser",
        "POSTGRES_PASSWORD": "testpass"
    }
    mock_getenv.side_effect = lambda key, default=None: env_vars.get(key, default)
    mock_psycopg2.connect.side_effect = db_importer.psycopg2.OperationalError("Connection refused")

    conn = db_importer.connect_db()
    
    assert conn is None 
    # mock_sys_exit.assert_called_once_with(1) # No longer expecting sys.exit


# --- Tests for create_tables ---
def test_create_tables():
    """Test create_tables executes all expected SQL CREATE statements."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

    db_importer.create_tables(mock_conn)

    # Updated counts for the new schema in db_importer.py
    # stores, products, prices, processed_files (4 tables)
    # idx_stores_chain_name, idx_products_chain_name, idx_products_barcode, 
    # idx_prices_store_db_id, idx_prices_product_db_id, idx_prices_crawled_at,
    # idx_processed_files_file_hash (7 indexes)
    expected_statements_count = 4 + 7 
    assert mock_cursor.execute.call_count == expected_statements_count

    executed_sqls = [call_args[0][0] for call_args in mock_cursor.execute.call_args_list]
    
    # Verify new schema (no stores_metadata)
    assert not any("CREATE TABLE IF NOT EXISTS stores_metadata" in sql for sql in executed_sqls)
    assert any("CREATE TABLE IF NOT EXISTS stores" in sql for sql in executed_sqls)
    assert any("CREATE TABLE IF NOT EXISTS products" in sql for sql in executed_sqls)
    assert any("CREATE TABLE IF NOT EXISTS prices" in sql for sql in executed_sqls)
    assert any("CREATE TABLE IF NOT EXISTS processed_files" in sql for sql in executed_sqls)
    
    # Verify new indexes
    assert any("CREATE INDEX IF NOT EXISTS idx_stores_chain_name ON stores(chain_name);" in sql for sql in executed_sqls)
    assert any("CREATE INDEX IF NOT EXISTS idx_products_chain_name ON products(chain_name);" in sql for sql in executed_sqls)
    assert any("CREATE INDEX IF NOT EXISTS idx_products_barcode ON products(barcode);" in sql for sql in executed_sqls)
    assert any("CREATE INDEX IF NOT EXISTS idx_prices_store_db_id ON prices(store_db_id);" in sql for sql in executed_sqls)
    assert any("CREATE INDEX IF NOT EXISTS idx_prices_product_db_id ON prices(product_db_id);" in sql for sql in executed_sqls)
    assert any("CREATE INDEX IF NOT EXISTS idx_prices_crawled_at ON prices(crawled_at);" in sql for sql in executed_sqls)
    assert any("CREATE INDEX IF NOT EXISTS idx_processed_files_file_hash ON processed_files(file_hash);" in sql for sql in executed_sqls)

    mock_conn.commit.assert_called_once()

def test_create_tables_db_error_rollbacks_and_raises():
    """Test create_tables rollbacks and re-raises on database error."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_cursor.execute.side_effect = db_importer.psycopg2.Error("DB error")

    with pytest.raises(db_importer.psycopg2.Error):
        db_importer.create_tables(mock_conn)
    
    mock_conn.rollback.assert_called_once()


# --- Tests for is_file_processed ---
def test_is_file_processed_true_when_found():
    """Test is_file_processed returns True when file hash is found."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = (1,) # Simulate finding a record

    result = db_importer.is_file_processed(mock_cursor, "path/to/file.csv", "hash123")
    
    mock_cursor.execute.assert_called_once_with(
        "SELECT 1 FROM processed_files WHERE file_path = %s AND file_hash = %s",
        ("path/to/file.csv", "hash123")
    )
    assert result is True

def test_is_file_processed_false_when_not_found():
    """Test is_file_processed returns False when file hash is not found."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None # Simulate not finding a record

    result = db_importer.is_file_processed(mock_cursor, "path/to/file.csv", "hash123")
    assert result is False

def test_is_file_processed_db_error():
    """Test is_file_processed returns False on database error."""
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = db_importer.psycopg2.Error("DB query failed")

    result = db_importer.is_file_processed(mock_cursor, "path/to/file.csv", "hash123")
    assert result is False # Should default to False on error to be safe

# --- Tests for mark_file_as_processed ---
@patch('crawler.cli.db_importer.datetime') # Mock datetime to control 'now'
def test_mark_file_as_processed(mock_datetime):
    """Test mark_file_as_processed executes correct SQL."""
    mock_cursor = MagicMock()
    
    # Mock 'now'
    fixed_now = db_importer.datetime.datetime(2023, 1, 1, 12, 0, 0, tzinfo=db_importer.datetime.timezone.utc)
    mock_datetime.datetime.now.return_value = fixed_now

    db_importer.mark_file_as_processed(mock_cursor, "path/to/file.csv", "hash123")

    expected_sql = """
            INSERT INTO processed_files (file_path, file_hash, imported_at, updated_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (file_path) DO UPDATE SET
                file_hash = EXCLUDED.file_hash,
                updated_at = CURRENT_TIMESTAMP;
            """
    # Normalize whitespace for comparison
    normalized_expected_sql = " ".join(expected_sql.split())
    normalized_actual_sql = " ".join(mock_cursor.execute.call_args[0][0].split())
    
    assert normalized_actual_sql == normalized_expected_sql
    mock_cursor.execute.assert_called_once_with(
        pytest.approx(normalized_expected_sql), # Use approx for string comparison if needed, or direct
        ("path/to/file.csv", "hash123", fixed_now, fixed_now)
    )

def test_mark_file_as_processed_db_error():
    """Test mark_file_as_processed logs error but does not raise on DB error."""
    mock_cursor = MagicMock()
    mock_cursor.execute.side_effect = db_importer.psycopg2.Error("DB insert failed")
    
    # We expect it to log an error but not re-raise, so the test should pass without an exception.
    try:
        db_importer.mark_file_as_processed(mock_cursor, "path/to/file.csv", "hash123")
    except db_importer.psycopg2.Error:
        pytest.fail("mark_file_as_processed should not re-raise psycopg2.Error")
    # Further check: mock logging and verify error was logged (optional, depends on test depth)


# --- Tests for to_decimal_or_none (CSV row to SQL value conversion) ---
@pytest.mark.parametrize("input_value, field_name, expected_output", [
    ("10.99", "price", Decimal("10.99")),
    (" 0.5 ", "quantity", Decimal("0.5")),
    ("", "price", None),
    (None, "price", None),
    ("  ", "unit_price", None),
    ("invalid", "price", None), # Invalid operation
    ("1,234.56", "price", None), # Assuming standard decimal format, comma is invalid
])
def test_to_decimal_or_none(input_value, field_name, expected_output, caplog):
    """Test to_decimal_or_none handles various inputs correctly."""
    # caplog is a pytest fixture to capture log output
    db_importer.logging.getLogger().setLevel(db_importer.logging.WARNING) # Ensure warnings are captured

    result = db_importer.to_decimal_or_none(input_value, field_name)
    assert result == expected_output

    if input_value == "invalid" or input_value == "1,234.56":
        assert f"Invalid decimal value '{input_value}' for field '{field_name}'. Using NULL." in caplog.text
    else:
        # Check that no warning was logged for valid inputs
        if caplog.text: # if there is any log text
            assert f"Invalid decimal value" not in caplog.text

# Example of testing the main processing logic (conceptual, needs more mocks for a full run)
# This would be significantly more complex due to the nested loops and multiple file interactions.
# For this subtask, we're focusing on the individual functions.

# To make the script importable without running main():
# Ensure main() is called within:
# if __name__ == "__main__":
#    main()
# This is already confirmed to be the case.
# Test data for main loop would be complex, involving mocking glob, os.path.exists, open for CSVs, etc.
# For now, the tests above cover the specified unit test requirements.

# If you wanted to test parts of main(), you'd need extensive mocking:
# @patch('crawler.cli.db_importer.connect_db')
# @patch('crawler.cli.db_importer.create_tables')
# @patch('crawler.cli.db_importer.glob.glob')
# @patch('crawler.cli.db_importer.os.path.isdir')
# @patch('crawler.cli.db_importer.os.path.basename')
# @patch('crawler.cli.db_importer.os.path.exists')
# @patch('builtins.open', new_callable=mock_open)
# # ... and potentially more mocks for csv.DictReader, etc.
# def test_main_loop_concept(mock_open_builtin, mock_exists, ...):
#     # Setup mocks for connect_db to return a mock connection and cursor
#     # Setup glob.glob to return mock chain directories
#     # Setup os.path.exists to simulate file presence/absence
#     # Mock open for stores.csv, products.csv, prices.csv with sample data
#     # ... then call db_importer.main() and assert specific calls or behaviors
#     pass

# --- Tests for remove_diacritics ---
@pytest.mark.parametrize("input_text, expected_output", [
    ("Čakovec, Đurđevac, Šibenik, Županja, Ćuprija", "Cakovec, Durdevac, Sibenik, Zupanja, Cuprija"),
    ("ČĆŠĐŽčćšđž", "CCSDZccsdz"),
    ("Zagreb, Split, Rijeka", "Zagreb, Split, Rijeka"), # No diacritics
    ("Velika Gorica", "Velika Gorica"),
    ("München", "München"), # Non-Croatian diacritics unchanged
    ("", ""), # Empty string
    ("Test 123!", "Test 123!"), # With numbers and symbols
    ("ščđćž ŠČĐĆŽ", "scdcz SCDCZ"), # All mixed
    (None, None), # Non-string input
    (123, 123),   # Non-string input
])
def test_remove_diacritics(input_text, expected_output):
    """Test remove_diacritics function with various inputs."""
    assert db_importer.remove_diacritics(input_text) == expected_output
```
