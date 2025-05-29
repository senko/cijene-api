from sqlalchemy import (
    DECIMAL,
    TIMESTAMP,
    Column,
    Date,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.sql import func

Base = declarative_base()


class Chain(Base):
    __tablename__ = "chains"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False, unique=True)
    slug = Column(String(255), nullable=False, unique=True)

    stores = relationship("Store", back_populates="chain")

    __table_args__ = (
        Index("idx_chains_name", "name"),
        Index("idx_chains_slug", "slug"),
    )

    def __repr__(self):
        return f"<Chain(name='{self.name}', slug='{self.slug}')>"


class Store(Base):
    __tablename__ = "stores"

    id = Column(Integer, primary_key=True)
    chain_id = Column(Integer, ForeignKey("chains.id"), nullable=False)
    ext_store_id = Column(String(255), nullable=False)
    ext_name = Column(String(255))
    ext_store_type = Column(String(255))
    ext_street_address = Column(Text)
    ext_city = Column(String(255))
    ext_zipcode = Column(String(50))

    chain = relationship("Chain", back_populates="stores")
    products = relationship("StoreProduct", back_populates="store")

    __table_args__ = (
        Index("idx_stores_chain_id", "chain_id"),
        Index("idx_stores_chain_ext_id", "chain_id", "ext_store_id"),
        Index("idx_stores_ext_store_id", "ext_store_id"),
    )

    def __repr__(self):
        return f"<Store(name='{self.ext_name}', city='{self.ext_city}')>"


class Product(Base):
    __tablename__ = "products"

    barcode = Column(
        String(40), primary_key=True, unique=True, nullable=False
    )  # if not available, {chain}-{store_product_id} will be used
    ext_name = Column(String)  # - name of the product, use from one of the stores
    ext_brand = Column(String, nullable=True)
    ext_category = Column(String, nullable=True)
    ext_unit = Column(String, nullable=True)
    ext_quantity = Column(String, nullable=True)

    store_products = relationship("StoreProduct", back_populates="product")

    def __repr__(self):
        return f"<Product(barcode='{self.barcode}', name='{self.ext_name}')>"


class StoreProduct(Base):
    __tablename__ = "store_products"

    id = Column(Integer, primary_key=True)  # auto-incrementing ID
    store_id = Column(Integer, ForeignKey("stores.id"))
    barcode = Column(String(40), ForeignKey("products.barcode"))
    ext_product_id = Column(String, nullable=False)

    store = relationship("Store", back_populates="products")
    product = relationship("Product", back_populates="store_products")
    prices = relationship("ProductPrice", back_populates="store_product")

    __table_args__ = (
        Index(
            "idx_store_products_store_id_ext_product_id", "store_id", "ext_product_id"
        ),
        Index("idx_store_products_barcode", "barcode"),
        Index("idx_store_products_ext_product_id", "ext_product_id"),  # optional
        Index("idx_store_products_store_id", "store_id"),
    )

    def __repr__(self):
        return f"<StoreProduct(store_id={self.store_id}, ext_product_id='{self.ext_product_id}')>"


class ProductPrice(Base):
    __tablename__ = "product_prices"

    id = Column(Integer, primary_key=True)
    store_product_id = Column(Integer, ForeignKey("store_products.id"))
    valid_date = Column(Date, nullable=False)
    price = Column(DECIMAL(10, 2), nullable=False)
    unit_price = Column(DECIMAL(10, 2))
    best_price_30 = Column(DECIMAL(10, 2))
    anchor_price = Column(DECIMAL(10, 2))
    special_price = Column(DECIMAL(10, 2))
    crawled_at = Column(
        TIMESTAMP(timezone=True),
        server_default=func.current_timestamp(),
        nullable=False,
    )

    store_product = relationship("StoreProduct", back_populates="prices")

    __table_args__ = (
        Index("idx_product_prices_store_product_id", "store_product_id"),
        Index("idx_product_prices_date", "valid_date"),
        Index(
            "idx_product_prices_store_product_id_date",
            "store_product_id",
            "valid_date",
            unique=True,
        ),
        Index(
            "idx_product_prices_date_store_product_id", "valid_date", "store_product_id"
        ),
    )

    def __repr__(self):
        return f"<ProductPrice(store_product_id={self.store_product_id}, price={self.price})>"
