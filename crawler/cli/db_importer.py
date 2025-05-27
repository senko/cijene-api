import argparse
import os
import glob
import csv
import psycopg2 # type: ignore
from psycopg2 import extras as psycopg2_extras # type: ignore
import hashlib
import logging
import datetime
import sys
from decimal import Decimal, InvalidOperation
from dotenv import load_dotenv # type: ignore

# --- Configuration ---
# Logging will be configured in main after parsing args

# --- Database Connection ---
def connect_db():
    """Establishes a connection to the PostgreSQL database using environment variables."""
    try:
        db_host = os.getenv("DB_HOST", "localhost")
        db_port = os.getenv("DB_PORT", "5432")
        db_name = os.getenv("POSTGRES_DB")
        db_user = os.getenv("POSTGRES_USER")
        db_pass = os.getenv("POSTGRES_PASSWORD")

        if not all([db_name, db_user, db_pass]):
            logging.error("Missing one or more required database environment variables: POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD")
            return None
            
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_pass
        )
        logging.info(f"Successfully connected to database {db_name} on {db_host}:{db_port}.")
        return conn
    except psycopg2.OperationalError as e:
        logging.error(f"Database connection failed: {e}")
        return None

# --- Schema Initialization ---
def create_tables(conn):
    """Creates tables and indexes if they don't exist."""
    queries = [
        """
        CREATE TABLE IF NOT EXISTS stores (
            id SERIAL PRIMARY KEY,
            store_id VARCHAR(255) NOT NULL,
            chain_name VARCHAR(255) NOT NULL,
            name VARCHAR(255),
            store_type VARCHAR(255),
            address TEXT,
            city VARCHAR(255),
            zipcode VARCHAR(50),
            UNIQUE(store_id, chain_name)
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_stores_chain_name ON stores(chain_name);",
        """
        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            product_id VARCHAR(255) NOT NULL,
            chain_name VARCHAR(255) NOT NULL,
            barcode VARCHAR(255),
            name TEXT,
            brand VARCHAR(255),
            category VARCHAR(255),
            unit VARCHAR(50),
            quantity VARCHAR(100),
            UNIQUE(product_id, chain_name)
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_products_chain_name ON products(chain_name);",
        "CREATE INDEX IF NOT EXISTS idx_products_barcode ON products(barcode);",
        """
        CREATE TABLE IF NOT EXISTS prices (
            id BIGSERIAL PRIMARY KEY,
            store_db_id INTEGER REFERENCES stores(id) ON DELETE CASCADE NOT NULL,
            product_db_id INTEGER REFERENCES products(id) ON DELETE CASCADE NOT NULL,
            price DECIMAL(10, 2) NOT NULL,
            unit_price DECIMAL(10, 2),
            best_price_30 DECIMAL(10, 2),
            anchor_price DECIMAL(10, 2),
            special_price DECIMAL(10, 2),
            crawled_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
            UNIQUE(store_db_id, product_db_id, crawled_at)
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_prices_store_db_id ON prices(store_db_id);",
        "CREATE INDEX IF NOT EXISTS idx_prices_product_db_id ON prices(product_db_id);",
        "CREATE INDEX IF NOT EXISTS idx_prices_crawled_at ON prices(crawled_at);",
        """
        CREATE TABLE IF NOT EXISTS processed_files (
            id SERIAL PRIMARY KEY,
            file_path TEXT NOT NULL UNIQUE,
            file_hash VARCHAR(64) NOT NULL,
            imported_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP NOT NULL
        );
        """,
        "CREATE INDEX IF NOT EXISTS idx_processed_files_file_hash ON processed_files(file_hash);"
    ]
    try:
        with conn.cursor() as cur:
            for query in queries:
                cur.execute(query)
            conn.commit()
        logging.info("Database schema initialized/verified.")
    except psycopg2.Error as e:
        logging.error(f"Error creating tables or indexes: {e}")
        conn.rollback()
        raise

# --- File Processing Utilities ---
def get_file_hash(file_path):
    """Computes the SHA256 hash of a file."""
    sha256_hash = hashlib.sha256()
    try:
        with open(file_path, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except FileNotFoundError:
        logging.warning(f"File not found for hashing: {file_path}")
        return None
    except Exception as e:
        logging.error(f"Error hashing file {file_path}: {e}")
        return None

def is_file_processed(cursor, relative_file_path, file_hash):
    """Checks if the file (by relative path and hash) has been processed."""
    try:
        cursor.execute(
            "SELECT 1 FROM processed_files WHERE file_path = %s AND file_hash = %s",
            (relative_file_path, file_hash)
        )
        return cursor.fetchone() is not None
    except psycopg2.Error as e:
        logging.error(f"Error checking if file {relative_file_path} is processed: {e}")
        return False # Assume not processed on error to be safe

def mark_file_as_processed(cursor, relative_file_path, file_hash):
    """Marks a file as processed or updates its hash and timestamp if it exists."""
    now = datetime.datetime.now(datetime.timezone.utc)
    try:
        cursor.execute(
            """
            INSERT INTO processed_files (file_path, file_hash, imported_at, updated_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (file_path) DO UPDATE SET
                file_hash = EXCLUDED.file_hash,
                updated_at = CURRENT_TIMESTAMP;
            """,
            (relative_file_path, file_hash, now, now)
        )
        logging.info(f"Marked file {relative_file_path} as processed (hash: {file_hash[:8]}).")
    except psycopg2.Error as e:
        logging.error(f"Error marking file {relative_file_path} as processed: {e}")
        # Do not re-raise here to allow main loop to continue if one file marking fails,
        # but the transaction for this chain will be rolled back later if this is part of it.

def remove_diacritics(text: str) -> str:
    if not isinstance(text, str):
        return text
    replacements = {
        'š': 's', 'đ': 'd', 'č': 'c', 'ć': 'c', 'ž': 'z',
        'Š': 'S', 'Đ': 'D', 'Č': 'C', 'Ć': 'C', 'Ž': 'Z'
    }
    for diacritic, ascii_char in replacements.items():
        text = text.replace(diacritic, ascii_char)
    return text

def to_decimal_or_none(value_str, field_name_for_log=""):
    """Converts a string to Decimal, returns None if empty or invalid."""
    if value_str is None or value_str.strip() == "":
        return None
    try:
        return Decimal(value_str)
    except InvalidOperation:
        logging.warning(f"Invalid decimal value '{value_str}' for field '{field_name_for_log}'. Using NULL.")
        return None

# --- Main Importer Logic ---
def import_data(base_data_path, conn):
    """Imports data from CSV files found in subdirectories of base_data_path."""
    logging.info(f"Starting import from base data path: {base_data_path}")
    
    abs_base_data_path = os.path.abspath(base_data_path)
    logging.info(f"Absolute base data path resolved to: {abs_base_data_path}")

    chain_dirs = [d for d in glob.glob(os.path.join(abs_base_data_path, "*")) if os.path.isdir(d)]
    if not chain_dirs:
        logging.warning(f"No chain subdirectories found in {abs_base_data_path}. Nothing to import.")
        return

    for chain_dir_path in chain_dirs:
        chain_name = os.path.basename(chain_dir_path)
        logging.info(f"Processing chain: {chain_name} from path {chain_dir_path}")

        # Per-chain data maps
        store_csv_id_to_db_id_map = {}
        product_csv_id_to_db_id_map = {}
        
        # Files to process in order
        files_to_process_ordered = ["stores.csv", "products.csv", "prices.csv"]

        try: # Start transaction for this chain
            with conn.cursor(cursor_factory=psycopg2_extras.DictCursor) as cur:
                # Load existing mappings for this chain if files were skipped but prices needs them
                # This is useful if stores.csv or products.csv were unchanged but prices.csv is new/updated.
                cur.execute("SELECT store_id, id FROM stores WHERE chain_name = %s", (chain_name,))
                for record in cur:
                    store_csv_id_to_db_id_map[record['store_id']] = record['id']
                
                cur.execute("SELECT product_id, id FROM products WHERE chain_name = %s", (chain_name,))
                for record in cur:
                    product_csv_id_to_db_id_map[record['product_id']] = record['id']


                for csv_filename in files_to_process_ordered:
                    full_file_path = os.path.join(chain_dir_path, csv_filename)
                    # Ensure relative_file_path uses OS-independent separator '/'
                    relative_file_path = os.path.join(chain_name, csv_filename).replace(os.sep, '/')


                    if not os.path.exists(full_file_path):
                        logging.info(f"File {full_file_path} not found. Skipping.")
                        continue

                    file_hash = get_file_hash(full_file_path)
                    if not file_hash:
                        logging.error(f"Could not compute hash for {full_file_path}. Skipping this file.")
                        continue

                    if is_file_processed(cur, relative_file_path, file_hash):
                        logging.info(f"File {relative_file_path} (hash: {file_hash[:8]}) is unchanged and already processed. Skipping.")
                        continue
                    
                    logging.info(f"Processing new/updated file: {relative_file_path} (hash: {file_hash[:8]})")

                    if csv_filename == "stores.csv":
                        with open(full_file_path, 'r', encoding='utf-8-sig') as f: # utf-8-sig for potential BOM
                            reader = csv.DictReader(f)
                            for row_num, row in enumerate(reader):
                                try:
                                    cur.execute(
                                        """
                                        INSERT INTO stores (store_id, chain_name, name, store_type, address, city, zipcode)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                                        ON CONFLICT (store_id, chain_name) DO UPDATE SET
                                            name = EXCLUDED.name, store_type = EXCLUDED.store_type,
                                            address = EXCLUDED.address, city = EXCLUDED.city, zipcode = EXCLUDED.zipcode
                                        RETURNING id;
                                        """,
                                        (row.get('store_id'), chain_name, row.get('name'), row.get('type'), # CSV header is 'type'
                                         row.get('address'), remove_diacritics(row.get('city')), row.get('zipcode'))
                                    )
                                    store_db_id = cur.fetchone()[0]
                                    store_csv_id_to_db_id_map[row.get('store_id')] = store_db_id
                                except psycopg2.Error as e:
                                    logging.error(f"DB error processing row {row_num+1} in {relative_file_path}: {e}. Row: {row}")
                                    raise # Propagate to rollback chain transaction
                                except Exception as e:
                                    logging.error(f"Unexpected error on row {row_num+1} in {relative_file_path}: {e}. Row: {row}")
                                    raise

                    elif csv_filename == "products.csv":
                        with open(full_file_path, 'r', encoding='utf-8-sig') as f:
                            reader = csv.DictReader(f)
                            for row_num, row in enumerate(reader):
                                try:
                                    cur.execute(
                                        """
                                        INSERT INTO products (product_id, chain_name, barcode, name, brand, category, unit, quantity)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                        ON CONFLICT (product_id, chain_name) DO UPDATE SET
                                            barcode = EXCLUDED.barcode, name = EXCLUDED.name, brand = EXCLUDED.brand,
                                            category = EXCLUDED.category, unit = EXCLUDED.unit, quantity = EXCLUDED.quantity
                                        RETURNING id;
                                        """,
                                        (row.get('product_id'), chain_name, row.get('barcode'), row.get('name'),
                                         row.get('brand'), row.get('category'), row.get('unit'), row.get('quantity'))
                                    )
                                    product_db_id = cur.fetchone()[0]
                                    product_csv_id_to_db_id_map[row.get('product_id')] = product_db_id
                                except psycopg2.Error as e:
                                    logging.error(f"DB error processing row {row_num+1} in {relative_file_path}: {e}. Row: {row}")
                                    raise
                                except Exception as e:
                                    logging.error(f"Unexpected error on row {row_num+1} in {relative_file_path}: {e}. Row: {row}")
                                    raise
                                    
                    elif csv_filename == "prices.csv":
                        with open(full_file_path, 'r', encoding='utf-8-sig') as f:
                            reader = csv.DictReader(f)
                            for row_num, row in enumerate(reader):
                                try:
                                    store_id_csv = row.get('store_id')
                                    product_id_csv = row.get('product_id')

                                    store_db_id = store_csv_id_to_db_id_map.get(store_id_csv)
                                    product_db_id = product_csv_id_to_db_id_map.get(product_id_csv)

                                    if store_db_id is None:
                                        logging.warning(f"Prices row {row_num+1} in {relative_file_path}: Store ID '{store_id_csv}' not found in mapping for chain '{chain_name}'. Skipping row: {row}")
                                        continue
                                    if product_db_id is None:
                                        logging.warning(f"Prices row {row_num+1} in {relative_file_path}: Product ID '{product_id_csv}' not found in mapping for chain '{chain_name}'. Skipping row: {row}")
                                        continue

                                    price_val = to_decimal_or_none(row.get('price'), 'price')
                                    if price_val is None:
                                        logging.error(f"Prices row {row_num+1} in {relative_file_path}: 'price' field is missing or invalid. Skipping row: {row}")
                                        continue
                                    
                                    crawled_at_str = row.get('crawled_at') # Assuming ISO format with TZ
                                    crawled_at_dt = None
                                    if crawled_at_str:
                                        try:
                                            crawled_at_dt = datetime.datetime.fromisoformat(crawled_at_str)
                                        except ValueError:
                                            logging.warning(f"Invalid crawled_at format '{crawled_at_str}' in {relative_file_path}, row {row_num+1}. Using current timestamp.")
                                            crawled_at_dt = datetime.datetime.now(datetime.timezone.utc)
                                    else: # Default to current time if not provided
                                        crawled_at_dt = datetime.datetime.now(datetime.timezone.utc)

                                    cur.execute(
                                        """
                                        INSERT INTO prices (store_db_id, product_db_id, price, unit_price, best_price_30, anchor_price, special_price, crawled_at)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                        ON CONFLICT (store_db_id, product_db_id, crawled_at) DO NOTHING; 
                                        """, # Or DO UPDATE if re-crawling same product at same store at exact same time should update
                                        (store_db_id, product_db_id, price_val,
                                         to_decimal_or_none(row.get('unit_price'), 'unit_price'),
                                         to_decimal_or_none(row.get('best_price_30'), 'best_price_30'),
                                         to_decimal_or_none(row.get('anchor_price'), 'anchor_price'),
                                         to_decimal_or_none(row.get('special_price'), 'special_price'),
                                         crawled_at_dt
                                        )
                                    )
                                except psycopg2.Error as e:
                                    logging.error(f"DB error processing price row {row_num+1} in {relative_file_path}: {e}. Row: {row}")
                                    raise
                                except Exception as e:
                                    logging.error(f"Unexpected error on price row {row_num+1} in {relative_file_path}: {e}. Row: {row}")
                                    raise
                    
                    # If processing the file was successful up to this point
                    mark_file_as_processed(cur, relative_file_path, file_hash)

            conn.commit() # Commit transaction for the entire chain
            logging.info(f"Successfully processed and committed data for chain: {chain_name}")

        except (psycopg2.Error, Exception) as e: # Catch errors from file processing or db ops for this chain
            logging.error(f"Error processing chain {chain_name}. Rolling back changes for this chain. Error: {e}", exc_info=False) # Set exc_info=True for full traceback
            if conn: # Check if conn is valid before rollback
                conn.rollback()
        
    logging.info("Import process completed.")


# --- Script Execution Block ---
if __name__ == "__main__":
    load_dotenv() 

    parser = argparse.ArgumentParser(description="Import store data from CSVs into PostgreSQL.")
    parser.add_argument("--data-path", required=True, help="Root directory of the CSV data (e.g., ./docker_data/crawler/)")
    args = parser.parse_args()

    log_level_str = os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
    
    db_conn = None
    try:
        db_conn = connect_db()
        if db_conn:
            create_tables(db_conn) # Idempotent schema creation
            import_data(args.data_path, db_conn)
        else:
            logging.error("Failed to connect to the database. Exiting.")
            sys.exit(1) # Exit if DB connection failed in main
            
    except Exception as e:
        # This top-level exception handles errors not caught within import_data's chain processing loop
        # (e.g., error during connect_db if it didn't sys.exit, error in create_tables, or args.data_path issue)
        logging.critical(f"A critical error occurred during the import process: {e}", exc_info=True)
        if db_conn: # If connection exists and an error happened outside chain loop but after connect
             db_conn.rollback() 
    finally:
        if db_conn:
            db_conn.close()
            logging.info("Database connection closed.")

```
