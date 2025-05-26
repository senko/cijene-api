import os
import csv
import logging
import hashlib
import psycopg2
from psycopg2 import extras # For DictCursor
from pathlib import Path
from datetime import date

# --- Configuration ---
DATABASE_URL = os.getenv('DATABASE_URL')
CSV_DIR = os.getenv('CSV_DIR')

# Expected CSV Headers by the importer
# IMPORTANT: Assuming 'stores.csv' includes 'name' as per subtask instructions.
STORES_CSV_HEADERS = ["store_id", "name", "type", "address", "city", "zipcode"]
PRODUCTS_CSV_HEADERS = ["product_id", "barcode", "name", "brand", "category", "unit", "quantity"]
# Optional: packaging, date_added (will be handled with row.get())

PRICES_CSV_HEADERS = ["store_id", "product_id", "price", "unit_price", "best_price_30", "anchor_price", "special_price"]
# Optional: anchor_price_date, initial_price (will be handled with row.get())


# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_db_connection():
    """Establishes and returns a database connection."""
    if not DATABASE_URL:
        logger.error("DATABASE_URL environment variable not set.")
        raise ValueError("DATABASE_URL not set.")
    try:
        conn = psycopg2.connect(DATABASE_URL)
        logger.info("Successfully connected to the database.")
        return conn
    except psycopg2.OperationalError as e:
        logger.error(f"Database connection failed: {e}")
        raise

def read_csv_file(file_path: Path, expected_headers: list[str]):
    """
    Reads a CSV file, verifies headers, and yields rows as dictionaries.
    Skips the header row after verification.
    """
    if not file_path.exists():
        logger.warning(f"CSV file not found: {file_path}")
        return
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header is None:
                logger.warning(f"CSV file is empty: {file_path}")
                return
            
            # Check if all expected headers are present
            if not all(h in header for h in expected_headers):
                logger.warning(f"Header mismatch in {file_path}. Expected: {expected_headers}, Got: {header}. Skipping file.")
                return
            
            # Use DictReader for the rest of the file, using the actual header from the file
            # Re-open or seek(0) and then use DictReader with the verified header
            f.seek(0) 
            dict_reader = csv.DictReader(f)
            for row in dict_reader:
                yield row
    except Exception as e:
        logger.error(f"Error reading CSV file {file_path}: {e}")
        raise

def calculate_files_hash(file_paths: list[Path]) -> str:
    """Calculates SHA256 hash of the concatenated content of given files."""
    hasher = hashlib.sha256()
    for file_path in file_paths:
        if not file_path.exists():
            logger.warning(f"File not found for hashing: {file_path}. Batch hash will be affected.")
            continue # Or raise error if all files must exist
        try:
            with open(file_path, 'rb') as f:
                while chunk := f.read(8192): # Read in chunks
                    hasher.update(chunk)
        except IOError as e:
            logger.error(f"Could not read file {file_path} for hashing: {e}")
            # Decide if this is a critical error or if hashing can proceed with available files
            # For now, we continue, and the hash will be different if a file is unreadable
    return hasher.hexdigest()

def check_if_batch_processed(cursor, batch_identifier: str, current_files_hash: str) -> bool:
    """
    Checks if the batch has already been processed and if its content has changed.
    Returns True if batch should be skipped, False otherwise.
    """
    try:
        cursor.execute(
            "SELECT files_hash FROM processed_batches WHERE batch_identifier = %s",
            (batch_identifier,)
        )
        result = cursor.fetchone()
        if result:
            stored_hash = result[0] # Assuming fetchone() with DictCursor or accessing by index
            if stored_hash == current_files_hash:
                logger.info(f"Batch '{batch_identifier}' already processed and unchanged (Hash: {current_files_hash[:8]}...). Skipping.")
                return True
            else:
                logger.info(f"Batch '{batch_identifier}' previously processed but files have changed (Old Hash: {stored_hash[:8]}..., New Hash: {current_files_hash[:8]}...). Reprocessing.")
                return False
        else:
            logger.info(f"Batch '{batch_identifier}' not processed before. Processing now.")
            return False
    except psycopg2.Error as e:
        logger.error(f"Database error checking batch '{batch_identifier}': {e}")
        raise # Propagate to potentially roll back main transaction if this is part of one

def to_decimal_or_none(value_str):
    if value_str is None or value_str.strip() == "":
        return None
    try:
        return psycopg2.extensions.Decimal(value_str)
    except Exception: # Broad exception for various conversion errors
        logger.warning(f"Could not convert '{value_str}' to Decimal. Using None.")
        return None

def to_date_or_none(date_str):
    if date_str is None or date_str.strip() == "":
        return None
    try:
        # Attempt to parse common date formats if necessary, or rely on a standard one.
        # For now, assuming YYYY-MM-DD or that database can handle it.
        return date.fromisoformat(date_str) 
    except ValueError:
        logger.warning(f"Could not parse date string '{date_str}' to date. Using None.")
        return None

def process_batch(conn, cursor, chain_name: str, stores_csv_path: Path, products_csv_path: Path, prices_csv_path: Path):
    """
    Processes a single batch of CSV files (stores, products, prices) for a given chain.
    Operations are wrapped in a database transaction.
    """
    try:
        logger.info(f"Starting processing for chain: {chain_name}")
        cursor.execute('BEGIN')

        # 1. Upsert Chain
        cursor.execute(
            "INSERT INTO chains (name) VALUES (%s) ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING chain_id;",
            (chain_name,)
        )
        chain_id_tuple = cursor.fetchone()
        if not chain_id_tuple:
            logger.error(f"Could not obtain chain_id for chain '{chain_name}'. Skipping batch.")
            conn.rollback() # Rollback before returning
            return False
        chain_id = chain_id_tuple[0]
        logger.info(f"Upserted chain '{chain_name}', chain_id: {chain_id}")

        # 2. Process stores.csv
        stores_count = 0
        for row in read_csv_file(stores_csv_path, STORES_CSV_HEADERS):
            try:
                cursor.execute(
                    """
                    INSERT INTO stores (store_id, chain_id, name, store_type, address, city, zipcode)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (store_id) DO UPDATE SET
                        chain_id = EXCLUDED.chain_id,
                        name = EXCLUDED.name,
                        store_type = EXCLUDED.store_type,
                        address = EXCLUDED.address,
                        city = EXCLUDED.city,
                        zipcode = EXCLUDED.zipcode;
                    """,
                    (
                        row.get("store_id"), chain_id, row.get("name"), row.get("type"),
                        row.get("address"), row.get("city"), row.get("zipcode")
                    )
                )
                stores_count += 1
            except psycopg2.Error as e:
                logger.error(f"Error processing store row {row.get('store_id')} for chain '{chain_name}': {e}. Row: {row}")
                # Decide if to skip row or fail batch. For now, log and continue.
            except Exception as e: # Catch other errors like data conversion
                logger.error(f"Unexpected error processing store row {row.get('store_id')} for chain '{chain_name}': {e}. Row: {row}")


        logger.info(f"Processed {stores_count} rows from {stores_csv_path.name}")

        # 3. Process products.csv
        products_count = 0
        for row in read_csv_file(products_csv_path, PRODUCTS_CSV_HEADERS):
            try:
                date_added_val = to_date_or_none(row.get("date_added")) # Handle if 'date_added' key doesn't exist
                
                cursor.execute(
                    """
                    INSERT INTO products (product_id, barcode, name, brand, category, unit, quantity, packaging, date_added)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (product_id) DO UPDATE SET
                        barcode = EXCLUDED.barcode,
                        name = EXCLUDED.name,
                        brand = EXCLUDED.brand,
                        category = EXCLUDED.category,
                        unit = EXCLUDED.unit,
                        quantity = EXCLUDED.quantity,
                        packaging = EXCLUDED.packaging,
                        date_added = EXCLUDED.date_added;
                    """,
                    (
                        row.get("product_id"), row.get("barcode"), row.get("name"), row.get("brand"),
                        row.get("category"), row.get("unit"), row.get("quantity"),
                        row.get("packaging"), date_added_val
                    )
                )
                products_count += 1
            except psycopg2.Error as e:
                logger.error(f"Error processing product row {row.get('product_id')} for chain '{chain_name}': {e}. Row: {row}")
            except Exception as e:
                logger.error(f"Unexpected error processing product row {row.get('product_id')} for chain '{chain_name}': {e}. Row: {row}")

        logger.info(f"Processed {products_count} rows from {products_csv_path.name}")

        # 4. Process prices.csv
        prices_count = 0
        for row in read_csv_file(prices_csv_path, PRICES_CSV_HEADERS):
            try:
                price_val = to_decimal_or_none(row.get("price"))
                if price_val is None: # Price is a NOT NULL field in DB
                    logger.warning(f"Skipping price row due to missing price for product {row.get('product_id')} in store {row.get('store_id')}. Row: {row}")
                    continue

                cursor.execute(
                    """
                    INSERT INTO prices (store_id, product_id, price, unit_price, best_price_30,
                                        special_price, anchor_price, anchor_price_date, initial_price)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s); 
                    """,
                    (
                        row.get("store_id"), row.get("product_id"), price_val,
                        to_decimal_or_none(row.get("unit_price")),
                        to_decimal_or_none(row.get("best_price_30")),
                        to_decimal_or_none(row.get("special_price")),
                        to_decimal_or_none(row.get("anchor_price")),
                        row.get("anchor_price_date"), # Assumed TEXT in DB, no conversion needed unless specific format
                        to_decimal_or_none(row.get("initial_price"))
                    )
                )
                prices_count += 1
            except psycopg2.Error as e:
                logger.error(f"Error processing price row for product {row.get('product_id')} in store {row.get('store_id')}: {e}. Row: {row}")
            except Exception as e:
                logger.error(f"Unexpected error processing price row for product {row.get('product_id')} in store {row.get('store_id')}: {e}. Row: {row}")

        logger.info(f"Processed {prices_count} rows from {prices_csv_path.name}")

        conn.commit()
        logger.info(f"Successfully processed and committed batch for chain: {chain_name}")
        return True

    except psycopg2.Error as e:
        logger.error(f"Database error during batch processing for chain '{chain_name}': {e}")
        if conn:
            conn.rollback()
        return False
    except Exception as e:
        logger.error(f"Unexpected error during batch processing for chain '{chain_name}': {e}")
        if conn:
            conn.rollback()
        return False

def main():
    logger.info("Starting CSV importer script.")

    if not CSV_DIR:
        logger.error("CSV_DIR environment variable not set.")
        return

    base_dir = Path(CSV_DIR)
    if not base_dir.is_dir():
        logger.error(f"CSV_DIR '{CSV_DIR}' is not a valid directory.")
        return

    conn = None # Initialize conn to None
    try:
        conn = get_db_connection()
        cursor = conn.cursor() # Using default cursor, not DictCursor for simplicity here, adjust if needed

        for chain_subdir in base_dir.iterdir():
            if chain_subdir.is_dir():
                chain_name = chain_subdir.name
                logger.info(f"Processing potential batch in directory: {chain_subdir}")

                stores_csv = chain_subdir / "stores.csv"
                products_csv = chain_subdir / "products.csv"
                prices_csv = chain_subdir / "prices.csv"

                required_files = [stores_csv, products_csv, prices_csv]
                if not all(f.exists() for f in required_files):
                    logger.warning(f"Skipping batch '{chain_name}'. Not all required CSV files found in {chain_subdir}. Missing: {[f.name for f in required_files if not f.exists()]}")
                    continue
                
                current_files_hash = calculate_files_hash(required_files)
                batch_identifier = chain_name # Using chain name as batch identifier

                if check_if_batch_processed(cursor, batch_identifier, current_files_hash):
                    continue # Skip this batch

                # Process the batch
                success = process_batch(conn, cursor, chain_name, stores_csv, products_csv, prices_csv)

                if success:
                    try:
                        cursor.execute(
                            """
                            INSERT INTO processed_batches (batch_identifier, files_hash, processed_at)
                            VALUES (%s, %s, CURRENT_TIMESTAMP)
                            ON CONFLICT (batch_identifier) DO UPDATE SET
                                files_hash = EXCLUDED.files_hash,
                                processed_at = CURRENT_TIMESTAMP;
                            """,
                            (batch_identifier, current_files_hash)
                        )
                        conn.commit()
                        logger.info(f"Successfully recorded processing for batch '{batch_identifier}' with hash {current_files_hash[:8]}...")
                    except psycopg2.Error as e:
                        logger.error(f"Failed to update processed_batches table for '{batch_identifier}': {e}")
                        conn.rollback() # Rollback this specific operation
                        # Consider if the main batch processing should also be rolled back if this fails.
                        # For now, the batch data is committed, but its processing status update failed.
                else:
                    logger.warning(f"Batch processing failed for '{chain_name}'. Not updating processed_batches table.")
                    # process_batch should have rolled back its own transaction.

        logger.info("CSV importer script finished.")

    except ValueError as e: # For DATABASE_URL or CSV_DIR not set
        logger.error(f"Configuration error: {e}")
    except psycopg2.OperationalError as e: # For initial connection failure
         logger.error(f"Cannot connect to database: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred in main: {e}", exc_info=True)
    finally:
        if conn:
            if conn.cursor: # Check if cursor was created
                try:
                    conn.cursor().close() # Ensure cursor is closed
                except Exception as e_cur:
                    logger.error(f"Error closing cursor: {e_cur}")
            try:
                conn.close()
                logger.info("Database connection closed.")
            except Exception as e_conn:
                logger.error(f"Error closing connection: {e_conn}")


if __name__ == "__main__":
    main()
