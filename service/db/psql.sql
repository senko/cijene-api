-- Users table to store API users
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    api_key VARCHAR(64) UNIQUE NOT NULL,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_users_api_key ON users (api_key);

-- Chains table to store retailer chains
CREATE TABLE IF NOT EXISTS chains (
    id SERIAL PRIMARY KEY,
    code VARCHAR(50) UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Chain stats table to store statistics of loaded data per chain
CREATE TABLE IF NOT EXISTS chain_stats (
    id SERIAL PRIMARY KEY,
    chain_id INTEGER NOT NULL REFERENCES chains (id),
    price_date DATE NOT NULL,
    price_count INTEGER NOT NULL,
    store_count INTEGER NOT NULL,
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (chain_id, price_date)
);

-- Stores table to store retailer locations
CREATE TABLE IF NOT EXISTS stores (
    id SERIAL PRIMARY KEY,
    chain_id INTEGER NOT NULL REFERENCES chains (id),
    code VARCHAR(100) NOT NULL,
    type VARCHAR(100),
    address VARCHAR(255),
    city VARCHAR(100),
    zipcode VARCHAR(20),
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    phone VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (chain_id, code)
);

-- Add new columns to existing stores table if they don't exist
ALTER TABLE stores
ADD COLUMN IF NOT EXISTS lat DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS lon DOUBLE PRECISION,
ADD COLUMN IF NOT EXISTS phone VARCHAR(50);

-- Requires "cube" and "earthdistance" extensions for geospatial queries
CREATE EXTENSION IF NOT EXISTS cube;
CREATE EXTENSION IF NOT EXISTS earthdistance;
ALTER TABLE stores
ADD COLUMN IF NOT EXISTS earth_point earth GENERATED ALWAYS AS (ll_to_earth (lat, lon)) STORED;
CREATE INDEX IF NOT EXISTS idx_stores_earth_point ON stores USING GIST (earth_point);

-- Products table to store global product information
CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    ean VARCHAR(50) UNIQUE NOT NULL,
    brand VARCHAR(255),
    name VARCHAR(255),
    quantity DECIMAL(10, 3),
    unit VARCHAR(10) CHECK (unit IN ('L', 'kg', 'm', 'kom')),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_products_ean ON products (ean);

-- Chain products table to store retailer-specific product information
CREATE TABLE IF NOT EXISTS chain_products (
    id SERIAL PRIMARY KEY,
    chain_id INTEGER NOT NULL REFERENCES chains (id),
    product_id INTEGER REFERENCES products (id),
    code VARCHAR(100) NOT NULL,
    brand VARCHAR(255),
    name VARCHAR(255) NOT NULL,
    category VARCHAR(255),
    unit VARCHAR(50),
    quantity VARCHAR(50),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (chain_id, code)
);

CREATE INDEX IF NOT EXISTS idx_chain_products_product_id ON chain_products (product_id);

-- Prices table to store product prices
CREATE TABLE IF NOT EXISTS prices (
    id BIGSERIAL PRIMARY KEY,
    chain_product_id INTEGER NOT NULL REFERENCES chain_products (id),
    store_id INTEGER NOT NULL REFERENCES stores (id),
    price_date DATE NOT NULL,
    regular_price DECIMAL(10, 2) NOT NULL,
    special_price DECIMAL(10, 2),
    unit_price DECIMAL(10, 2),
    -- current_price DECIMAL(10, 2) NOT NULL,
    best_price_30 DECIMAL(10, 2),
    anchor_price DECIMAL(10, 2),
    UNIQUE (chain_product_id, store_id, price_date)
);

-- Mark regular_price as required if the table already exists
ALTER TABLE prices ALTER COLUMN regular_price SET NOT NULL;

-- Prices table to store min/max/avg prices per chain
CREATE TABLE IF NOT EXISTS chain_prices (
    id SERIAL PRIMARY KEY,
    chain_product_id INTEGER NOT NULL REFERENCES chain_products (id),
    price_date DATE NOT NULL,
    min_price DECIMAL(10, 2) NOT NULL,
    max_price DECIMAL(10, 2) NOT NULL,
    avg_price DECIMAL(10, 2) NOT NULL,
    UNIQUE (chain_product_id, price_date)
);

  -- Drop the existing unique constraint and primary key
  ALTER TABLE prices DROP CONSTRAINT IF EXISTS prices_chain_product_id_store_id_price_date_key;
  ALTER TABLE prices DROP CONSTRAINT IF EXISTS prices_pkey;
  ALTER TABLE prices DROP COLUMN IF EXISTS id;

  -- Add the composite primary key
  ALTER TABLE prices ADD PRIMARY KEY (chain_product_id, store_id, price_date);