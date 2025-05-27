import pytest
import os
import shutil
from decimal import Decimal

# Import the helper to run the importer's main logic and the hash utility
from .conftest import run_importer_main, get_file_hash_util

# Sample data path relative to this test file, to locate original CSVs
# (though conftest's importer_test_data_dir handles copying)
# TEST_SAMPLE_DATA_DIR = os.path.join(os.path.dirname(__file__), "sample_data", "test_chain_1")


def count_rows(cursor, table_name):
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    return cursor.fetchone()[0]

def get_all_rows_as_dicts(cursor, table_name):
    cursor.execute(f"SELECT * FROM {table_name}")
    columns = [desc[0] for desc in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

def test_initial_import(monkeypatch, postgresql_container, test_db_connection, importer_test_data_dir):
    """
    Tests the initial import of data from sample CSV files.
    Verifies table contents, row counts, and processed_files entries.
    """
    db_params = postgresql_container
    data_dir = importer_test_data_dir
    
    # Run the importer script
    run_importer_main(monkeypatch, db_params, data_dir)

    # --- Verification ---
    with test_db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        # 1. Verify stores (chain_name is 'test_chain_1' by convention from sample data path)
        # Sample data now has 3 stores
        assert count_rows(cur, "stores") == 3 
        
        # Check STORE001 with "Čakovec City"
        cur.execute("SELECT * FROM stores WHERE chain_name = 'test_chain_1' AND store_id = 'STORE001'")
        store1 = cur.fetchone()
        assert store1 is not None
        assert store1['name'] is None 
        assert store1['address'] == '123 Main St'
        assert store1['store_type'] == 'supermarket' 
        assert store1['city'] == 'Cakovec City' # Verify de-diacriticized

        # Check STORE002 (Testburg - no diacritics)
        cur.execute("SELECT * FROM stores WHERE chain_name = 'test_chain_1' AND store_id = 'STORE002'")
        store2 = cur.fetchone()
        assert store2 is not None
        assert store2['city'] == 'Testburg'

        # Check STORE003 with "Varaždinville"
        cur.execute("SELECT * FROM stores WHERE chain_name = 'test_chain_1' AND store_id = 'STORE003'")
        store3 = cur.fetchone()
        assert store3 is not None
        assert store3['store_type'] == 'market'
        assert store3['city'] == 'Varazdinville' # Verify de-diacriticized

        # 2. Verify products (assuming products.csv is not changed and only has 2 products for test_chain_1)
        assert count_rows(cur, "products") == 2
        cur.execute("SELECT * FROM products WHERE chain_name = 'test_chain_1' AND product_id = 'PROD001'")
        prod1 = cur.fetchone()
        assert prod1 is not None
        assert prod1['name'] == 'Test Product 1'
        assert prod1['brand'] == 'BrandA'
        
        # 3. Verify prices
        # STORE001,PROD001,10.99
        # STORE001,PROD002,5.50
        # STORE002,PROD001,10.89
        assert count_rows(cur, "prices") == 3 
        # Get store_db_id and product_db_id for a specific price check
        cur.execute("SELECT id FROM stores WHERE store_id = 'STORE001' AND chain_name = 'test_chain_1'")
        store001_db_id = cur.fetchone()['id']
        cur.execute("SELECT id FROM products WHERE product_id = 'PROD001' AND chain_name = 'test_chain_1'")
        prod001_db_id = cur.fetchone()['id']

        cur.execute("SELECT price FROM prices WHERE store_db_id = %s AND product_db_id = %s", (store001_db_id, prod001_db_id))
        price_entry = cur.fetchone()
        assert price_entry is not None
        assert price_entry['price'] == Decimal("10.99")

        # 5. Verify processed_files
        assert count_rows(cur, "processed_files") == 3
        
        expected_files = ["stores.csv", "products.csv", "prices.csv"]
        processed_files_rows = get_all_rows_as_dicts(cur, "processed_files")
        
        for fname in expected_files:
            file_path_in_db = os.path.join(data_dir, "test_chain_1", fname)
            original_file_path = os.path.join(importer_test_data_dir, "test_chain_1", fname) # Path to actual file to hash
            expected_hash = get_file_hash_util(original_file_path)
            
            file_entry = next((r for r in processed_files_rows if r['file_path'] == file_path_in_db), None)
            assert file_entry is not None, f"{fname} not found in processed_files"
            assert file_entry['file_hash'] == expected_hash
            assert file_entry['imported_at'] is not None
            assert file_entry['updated_at'] is not None
            assert file_entry['imported_at'] == file_entry['updated_at'] # For initial import

def test_idempotency_no_changes(monkeypatch, postgresql_container, test_db_connection, importer_test_data_dir):
    """
    Tests that running the importer again with no changes to CSV files
    does not alter existing data and correctly identifies files as processed.
    """
    db_params = postgresql_container
    data_dir = importer_test_data_dir

    # --- First run (initial import) ---
    run_importer_main(monkeypatch, db_params, data_dir)
    
    initial_counts = {}
    initial_processed_files = {}
    with test_db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        # initial_counts["stores_metadata"] = count_rows(cur, "stores_metadata") # No stores_metadata
        initial_counts["stores"] = count_rows(cur, "stores")
        initial_counts["products"] = count_rows(cur, "products")
        initial_counts["prices"] = count_rows(cur, "prices")
        initial_counts["processed_files"] = count_rows(cur, "processed_files")
        for row in get_all_rows_as_dicts(cur, "processed_files"):
            initial_processed_files[row['file_path']] = (row['file_hash'], row['updated_at'])

    # --- Second run (no changes to files) ---
    # Ensure enough time passes for updated_at to potentially change if files were re-processed
    # However, our logic should skip them based on hash, so updated_at for these should NOT change.
    # If it does, it means it's re-marking them as processed, which is acceptable by `mark_file_as_processed`
    # The key is that the hash matches and it's not inserting duplicate data.
    import time
    time.sleep(1) # Ensure a time difference if updated_at were to change on re-processing
    
    run_importer_main(monkeypatch, db_params, data_dir)

    # --- Verification ---
    with test_db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        # Data table counts should be identical
        # assert count_rows(cur, "stores_metadata") == initial_counts["stores_metadata"] # No stores_metadata
        assert count_rows(cur, "stores") == initial_counts["stores"]
        assert count_rows(cur, "products") == initial_counts["products"]
        assert count_rows(cur, "prices") == initial_counts["prices"]
        
        # Processed_files count should be identical
        assert count_rows(cur, "processed_files") == initial_counts["processed_files"]
        
        # Verify hashes and updated_at times (should ideally be unchanged if skipped correctly)
        current_processed_files = get_all_rows_as_dicts(cur, "processed_files")
        for row in current_processed_files:
            assert row['file_path'] in initial_processed_files
            expected_hash, expected_updated_at = initial_processed_files[row['file_path']]
            assert row['file_hash'] == expected_hash
            # updated_at for skipped files should remain the same as it was not re-marked
            assert row['updated_at'] == expected_updated_at, \
                f"updated_at for {row['file_path']} changed, but file should have been skipped."


def test_file_update_and_new_chain(monkeypatch, postgresql_container, test_db_connection, importer_test_data_dir):
    """
    Tests importer behavior when a file is updated and a new chain is added.
    1. Initial import.
    2. Modify prices.csv in test_chain_1.
    3. Add a new chain 'test_chain_2' with its own data.
    4. Run importer again.
    5. Verify changes and new data.
    """
    db_params = postgresql_container
    data_dir = importer_test_data_dir # This is the root for chains, e.g., /tmp/importer_xyz/

    # --- First run (initial import of test_chain_1) ---
    run_importer_main(monkeypatch, db_params, data_dir)
    
    original_prices_path_chain1 = os.path.join(data_dir, "test_chain_1", "prices.csv")
    original_hash_prices_chain1 = get_file_hash_util(original_prices_path_chain1)

    with test_db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        # Get initial state of processed_files for test_chain_1/prices.csv
        cur.execute("SELECT * FROM processed_files WHERE file_path = %s", (original_prices_path_chain1,))
        pf_prices_chain1_before_update = cur.fetchone()
        assert pf_prices_chain1_before_update is not None
        assert pf_prices_chain1_before_update['file_hash'] == original_hash_prices_chain1

    # --- Modifications ---
    # 1. Modify prices.csv in test_chain_1
    # STORE001,PROD001,10.99 -> change to 12.99
    modified_prices_content_chain1 = []
    with open(original_prices_path_chain1, 'r') as f:
        lines = f.readlines()
        modified_prices_content_chain1.append(lines[0]) # header
        row1_parts = lines[1].strip().split(',')
        row1_parts[2] = "12.99" # Change price
        modified_prices_content_chain1.append(",".join(row1_parts) + "\n")
        modified_prices_content_chain1.extend(lines[2:])
        
    with open(original_prices_path_chain1, 'w') as f:
        f.writelines(modified_prices_content_chain1)
    
    updated_hash_prices_chain1 = get_file_hash_util(original_prices_path_chain1)
    assert updated_hash_prices_chain1 != original_hash_prices_chain1

    # 2. Add a new chain 'test_chain_2'
    new_chain_dir = os.path.join(data_dir, "test_chain_2")
    os.makedirs(new_chain_dir, exist_ok=True)
    
    # Create sample files for test_chain_2 (minimal)
    with open(os.path.join(new_chain_dir, "stores.csv"), "w") as f:
        f.write("store_id,type,address,city,zipcode\n")
        f.write("NEWSTORE001,kiosk,789 Pine,NewCity,54321\n")
    
    with open(os.path.join(new_chain_dir, "products.csv"), "w") as f:
        f.write("product_id,barcode,name,brand,category,unit,quantity\n")
        f.write("NEWPROD001,777888999,New Product,BrandC,CategoryZ,pcs,1\n")

    with open(os.path.join(new_chain_dir, "prices.csv"), "w") as f:
        f.write("store_id,product_id,price,unit_price\n")
        f.write("NEWSTORE001,NEWPROD001,99.99,99.99\n")

    # --- Second run (with updated file and new chain) ---
    time.sleep(1) # Ensure time difference for updated_at
    run_importer_main(monkeypatch, db_params, data_dir)

    # --- Verification ---
    with test_db_connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        # Verify updated price for test_chain_1
        cur.execute("SELECT id FROM stores WHERE store_id = 'STORE001' AND chain_name = 'test_chain_1'")
        store001_db_id_c1 = cur.fetchone()['id']
        cur.execute("SELECT id FROM products WHERE product_id = 'PROD001' AND chain_name = 'test_chain_1'")
        prod001_db_id_c1 = cur.fetchone()['id']

        cur.execute("SELECT price FROM prices WHERE store_db_id = %s AND product_db_id = %s", 
                    (store001_db_id_c1, prod001_db_id_c1))
        updated_price_entry = cur.fetchone()
        assert updated_price_entry is not None
        assert updated_price_entry['price'] == Decimal("12.99") # Verifying the change

        # Verify processed_files entry for the updated prices.csv of test_chain_1
        cur.execute("SELECT * FROM processed_files WHERE file_path = %s", (original_prices_path_chain1,))
        pf_prices_chain1_after_update = cur.fetchone()
        assert pf_prices_chain1_after_update is not None
        assert pf_prices_chain1_after_update['file_hash'] == updated_hash_prices_chain1
        assert pf_prices_chain1_after_update['updated_at'] > pf_prices_chain1_before_update['updated_at']

        # Verify data for new chain 'test_chain_2'
        # No stores_metadata table, chain_name is directly in stores and products
        
        assert count_rows(cur, "stores WHERE chain_name = 'test_chain_2'") == 1
        assert count_rows(cur, "products WHERE chain_name = 'test_chain_2'") == 1
        # To count prices for chain 2, join with stores table
        cur.execute("""
            SELECT COUNT(p.id) 
            FROM prices p
            JOIN stores s ON p.store_db_id = s.id
            WHERE s.chain_name = 'test_chain_2'
        """)
        assert cur.fetchone()[0] == 1
        
        cur.execute("SELECT * FROM stores WHERE chain_name = 'test_chain_2' AND store_id = 'NEWSTORE001'")
        new_store = cur.fetchone()
        assert new_store is not None
        assert new_store['city'] == "NewCity"

        # Verify processed_files entries for test_chain_2
        expected_new_chain_files = ["stores.csv", "products.csv", "prices.csv"]
        for fname in expected_new_chain_files:
            file_path_in_db = os.path.join(data_dir, "test_chain_2", fname)
            original_file_path = os.path.join(new_chain_dir, fname)
            expected_hash = get_file_hash_util(original_file_path)
            
            cur.execute("SELECT * FROM processed_files WHERE file_path = %s", (file_path_in_db,))
            file_entry = cur.fetchone()
            assert file_entry is not None, f"{fname} for test_chain_2 not found in processed_files"
            assert file_entry['file_hash'] == expected_hash
            assert file_entry['imported_at'] == file_entry['updated_at'] # New files

        # Total processed files should be 3 (chain1) + 3 (chain2) = 6
        assert count_rows(cur, "processed_files") == 6
```
