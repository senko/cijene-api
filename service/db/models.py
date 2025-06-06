from typing import Optional
from datetime import date, datetime
from decimal import Decimal

from dataclasses import dataclass, fields


@dataclass(frozen=True, slots=True, kw_only=True)
class User:
    id: int
    name: str
    api_key: str
    is_active: bool
    created_at: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class Chain:
    code: str


@dataclass(frozen=True, slots=True, kw_only=True)
class ChainWithId(Chain):
    id: int


@dataclass(frozen=True, slots=True, kw_only=True)
class ChainSummary:
    chain_code: str
    price_date: date
    price_count: int
    store_count: int
    created_at: datetime


@dataclass(frozen=True, slots=True, kw_only=True)
class Store:
    chain_id: int
    code: str
    type: Optional[str] = None
    address: Optional[str] = None
    city: Optional[str] = None
    zipcode: Optional[str] = None


@dataclass(frozen=True, slots=True, kw_only=True)
class StoreWithId(Store):
    id: int


@dataclass(frozen=True, slots=True, kw_only=True)
class Product:
    ean: str
    brand: Optional[str] = None
    name: Optional[str] = None
    quantity: Optional[Decimal] = None
    unit: Optional[str] = None

    def to_dict(self):
        return {f.name: getattr(self, f.name) for f in fields(self)}


@dataclass(frozen=True, slots=True, kw_only=True)
class ProductWithId(Product):
    id: int


@dataclass(frozen=True, slots=True, kw_only=True)
class ChainProduct:
    chain_id: int
    product_id: int
    code: str
    name: str
    brand: Optional[str] = None
    category: Optional[str] = None
    unit: Optional[str] = None
    quantity: Optional[str] = None

    def to_dict(self):
        return {f.name: getattr(self, f.name) for f in fields(self)}


@dataclass(frozen=True, slots=True, kw_only=True)
class ChainProductWithId(ChainProduct):
    id: int


@dataclass(frozen=True, slots=True)
class Price:
    chain_product_id: int
    store_id: int
    price_date: date
    regular_price: Optional[Decimal] = None
    special_price: Optional[Decimal] = None
    unit_price: Optional[Decimal] = None
    best_price_30: Optional[Decimal] = None
    anchor_price: Optional[Decimal] = None
