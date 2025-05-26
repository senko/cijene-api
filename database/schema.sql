-- database/schema.sql

-- Chains Table
CREATE TABLE chains (
    chain_id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

-- Stores Table
CREATE TABLE stores (
    store_id TEXT PRIMARY KEY,
    chain_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    store_type TEXT,
    address TEXT,
    city TEXT,
    zipcode TEXT,
    FOREIGN KEY (chain_id) REFERENCES chains(chain_id)
);

-- Products Table
CREATE TABLE products (
    product_id TEXT PRIMARY KEY,
    barcode TEXT UNIQUE,
    name TEXT NOT NULL,
    brand TEXT,
    category TEXT,
    unit TEXT,
    quantity TEXT,
    packaging TEXT,
    date_added DATE
);

-- Prices Table
CREATE TABLE prices (
    price_id SERIAL PRIMARY KEY,
    store_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    price DECIMAL(10, 2) NOT NULL,
    unit_price DECIMAL(10, 2),
    best_price_30 DECIMAL(10, 2),
    special_price DECIMAL(10, 2),
    anchor_price DECIMAL(10, 2),
    anchor_price_date TEXT,
    initial_price DECIMAL(10, 2),
    FOREIGN KEY (store_id) REFERENCES stores(store_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

-- Indexes
DROP INDEX IF EXISTS idx_stores_chain;
CREATE INDEX idx_stores_store_id ON stores(store_id);
CREATE INDEX idx_stores_chain_id ON stores(chain_id);

CREATE INDEX idx_chains_name ON chains(name);

CREATE INDEX idx_products_product_id ON products(product_id);
CREATE INDEX idx_products_barcode ON products(barcode);
CREATE INDEX idx_products_name ON products(name);
CREATE INDEX idx_products_brand ON products(brand);
CREATE INDEX idx_products_category ON products(category);

CREATE INDEX idx_prices_store_id ON prices(store_id);
CREATE INDEX idx_prices_product_id ON prices(product_id);
CREATE INDEX idx_prices_timestamp ON prices(timestamp);

-- Processed CSV Batches Table
CREATE TABLE processed_batches (
    batch_identifier TEXT PRIMARY KEY, -- e.g., directory path like 'YYYY-MM-DD/chain_name' or a unique name for the batch
    files_hash TEXT NOT NULL,          -- A combined hash of the CSV files in the batch
    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_processed_batches_identifier ON processed_batches(batch_identifier);
