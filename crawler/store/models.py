from decimal import Decimal
from typing import List, Optional
from datetime import date

from pydantic import BaseModel, Field


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
    unit_price: Optional[Decimal] = None  # Price per unit of measure, as published
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
