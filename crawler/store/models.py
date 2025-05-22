from decimal import Decimal
from typing import List, Optional
from datetime import date

from pydantic import BaseModel, Field
from sqlalchemy import create_engine, Column, String, Numeric, ForeignKey
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Integer


class Product(BaseModel):
    """
    Unified product model for all stores.
    """

    product: str  # Product name
    product_id: str  # Store specific product identifier
    brand: str  # Brand name
    quantity: str  # Amount (e.g., "500g", "1L")
    unit: str  # Unit of measure (e.g., "kg", "kom")
    price: Decimal  # Current retail price
    unit_price: Decimal  # Price per unit of measure
    barcode: str  # EAN/barcode
    category: str  # Product category

    # Optional fields that appear in some stores
    best_price_30: Optional[Decimal] = None  # Lowest price in last 30 days
    special_price: Optional[Decimal] = None  # Promotional/discounted price
    anchor_price: Optional[Decimal] = None  # Reference price (often May 2, 2025)
    anchor_price_date: Optional[str] = None  # Date of reference price
    packaging: Optional[str] = None  # Packaging information
    initial_price: Optional[Decimal] = (
        None  # Initial price for newly added products (if available)
    )
    date_added: Optional[date] = None  # When the product was added (if available)

    def __str__(self):
        return f"{self.brand.title()} {self.product.title()} (EAN: {self.barcode})"


class Store(BaseModel):
    """
    Unified store model for all retailers.
    """

    chain: str  # Store chain name, lowercase ("konzum", "lidl", "spar", etc.)
    store_id: str  # Chain-specific store (location) identifier
    name: str  # Store name (e.g., "Lidl Zagreb")
    store_type: str  # Type (e.g., "supermarket", "hipermarket")
    city: str  # City location
    street_address: str  # Street address
    zipcode: str = ""  # Postal code (empty default if not available)
    items: List[Product] = Field(default_factory=list)  # Products in this store

    def __str__(self):
        return f"{self.name} ({self.street_address})"


Base = declarative_base()

class StoreDB(Base):
    __tablename__ = 'stores'

    id = Column(Integer, primary_key=True)
    chain = Column(String)
    store_id = Column(String)
    name = Column(String)
    store_type = Column(String)
    city = Column(String)
    street_address = Column(String)
    zipcode = Column(String)
    products = relationship("ProductDB", back_populates="store")

    def __repr__(self):
        return f"<Store(name='{self.name}', city='{self.city}')>"

class ProductDB(Base):
    __tablename__ = 'products'

    id = Column(Integer, primary_key=True)
    store_id = Column(Integer, ForeignKey('stores.id'))
    barcode = Column(String, nullable=True)
    product_id = Column(String)
    name = Column(String)
    brand = Column(String, nullable=True)
    category = Column(String, nullable=True)
    unit = Column(String, nullable=True)
    quantity = Column(String, nullable=True)
    price = Column(Numeric, nullable=True)
    unit_price = Column(Numeric, nullable=True)
    best_price_30 = Column(Numeric, nullable=True)
    anchor_price = Column(Numeric, nullable=True)

    store = relationship("StoreDB", back_populates="products")

    def __repr__(self):
        return f"<Product(name='{self.name}', price={self.price})>"

def create_db(db_path: str):
    engine = create_engine(f'sqlite:///{db_path}')
    Base.metadata.create_all(engine)
    return engine
