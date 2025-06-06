from decimal import Decimal
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
import datetime

from service.config import settings
from service.db.models import ChainSummary, ProductWithId
from service.routers.auth import RequireAuth

router = APIRouter(tags=["Products, Chains and Stores"], dependencies=[RequireAuth])
db = settings.get_db()


class ListChainsResponse(BaseModel):
    """List chains response schema."""

    chains: list[str] = Field(..., description="List of retail chain codes.")


@router.get("/chains/", summary="List retail chains")
async def list_chains() -> ListChainsResponse:
    """List all available chains."""
    chains = await db.list_chains()
    return ListChainsResponse(chains=[chain.code for chain in chains])


class StoreResponse(BaseModel):
    """Store response schema."""

    chain_code: str = Field(..., description="Code of the retail chain.")
    code: str = Field(..., description="Unique code of the store.")
    type: str | None = Field(
        ...,
        description="Type of the store (e.g., supermarket, hypermarket).",
    )
    address: str | None = Field(..., description="Physical address of the store.")
    city: str | None = Field(..., description="City where the store is located.")
    zipcode: str | None = Field(..., description="Postal code of the store location.")


class ListStoresResponse(BaseModel):
    """List stores response schema."""

    stores: list[StoreResponse] = Field(
        ..., description="List stores for the specified chain."
    )


@router.get(
    "/{chain_code}/stores/",
    summary="List retail chain stores",
)
async def list_stores(chain_code: str) -> ListStoresResponse:
    """
    List all stores (locations) for a particular chain.

    Future plan: Allow filtering by store type and location.
    """
    stores = await db.list_stores(chain_code)

    if not stores:
        raise HTTPException(status_code=404, detail=f"No chain {chain_code}")

    return ListStoresResponse(
        stores=[
            StoreResponse(
                chain_code=chain_code,
                code=store.code,
                type=store.type,
                address=store.address,
                city=store.city,
                zipcode=store.zipcode,
            )
            for store in stores
        ]
    )


class ChainProductResponse(BaseModel):
    """Chain product with price information response schema."""

    chain: str = Field(..., description="Chain code.")
    code: str = Field(..., description="Product code within the chain.")
    name: str = Field(..., description="Product name within the chain.")
    brand: str | None = Field(..., description="Product brand within the chain.")
    category: str | None = Field(..., description="Product category within the chain.")
    unit: str | None = Field(..., description="Product unit within the chain.")
    quantity: str | None = Field(..., description="Product quantity within the chain.")
    min_price: Decimal = Field(..., description="Minimum price across chain stores.")
    max_price: Decimal = Field(..., description="Maximum price across chain stores.")
    avg_price: Decimal = Field(..., description="Average price across chain stores.")


class ProductResponse(BaseModel):
    """Basic product information response schema."""

    ean: str = Field(..., description="EAN barcode of the product.")
    brand: str | None = Field(..., description="Brand of the product.")
    name: str | None = Field(..., description="Name of the product.")
    quantity: str | None = Field(..., description="Quantity of the product.")
    unit: str | None = Field(..., description="Unit of the product.")
    chains: list[ChainProductResponse] = Field(
        ..., description="List of chain-specific product information."
    )


class ProductSearchResponse(BaseModel):
    products: list[ProductResponse] = Field(
        ..., description="List of products matching the search query."
    )


async def prepare_product_response(
    products: list[ProductWithId],
    date: datetime.date | None,
    filtered_chains: list[str] | None,
) -> list[ProductResponse]:
    chains = await db.list_chains()
    if filtered_chains:
        chains = [c for c in chains if c.code in filtered_chains]
    chain_id_to_code = {chain.id: chain.code for chain in chains}

    if not date:
        date = datetime.date.today()

    product_ids = [product.id for product in products]

    chain_products = await db.get_chain_products_for_product(
        product_ids,
        [chain.id for chain in chains],
    )

    product_response_map = {
        product.id: ProductResponse(
            ean=product.ean,
            brand=product.brand or "",
            name=product.name or "",
            quantity=str(product.quantity) if product.quantity else None,
            unit=product.unit,
            chains=[],
        )
        for product in products
    }

    cpr_map = {}
    for cp in chain_products:
        product_id = cp.product_id
        chain = chain_id_to_code[cp.chain_id]

        cpr_data = cp.to_dict()
        cpr_data["chain"] = chain
        cpr_map[(product_id, chain)] = cpr_data

    prices = await db.get_product_prices(product_ids, date)
    for p in prices:
        product_id = p["product_id"]
        chain = p["chain"]
        cpr_data = cpr_map.get((product_id, chain))
        if not cpr_data:
            continue

        cpr_data["min_price"] = p["min_price"]
        cpr_data["max_price"] = p["max_price"]
        cpr_data["avg_price"] = p["avg_price"]
        product_response_map[product_id].chains.append(ChainProductResponse(**cpr_data))

    # Fixup global product brand and name using chain data
    # Logic here is that the longest string is the most likely to be most useful
    for product in product_response_map.values():
        if not product.brand:
            chain_brands = [cpr.brand for cpr in product.chains if cpr.brand]
            chain_brands.sort(key=lambda x: len(x))
            if chain_brands:
                product.brand = chain_brands[0].capitalize()

        if not product.name:
            chain_names = [cpr.name for cpr in product.chains if cpr.name]
            chain_names.sort(key=lambda x: len(x), reverse=True)
            if chain_names:
                product.name = chain_names[0].capitalize()

    return [p for p in product_response_map.values() if p.chains]


@router.get("/products/{ean}/", summary="Get product data/prices by barcode")
async def get_product(
    ean: str,
    date: datetime.date = Query(
        None,
        description="Date in YYYY-MM-DD format, defaults to today",
    ),
    chains: str = Query(
        None,
        description="Comma-separated list of chain codes to include",
    ),
) -> ProductResponse:
    """
    Get product information including chain products and prices by their
    barcode. For products that don't have official EAN codes and use
    chain-specific codes, use the "chain:<product_code>" format.

    The price information is for the last known date earlier than or
    equal to the specified date. If no date is provided, current date is used.
    """

    products = await db.get_products_by_ean([ean])
    if not products:
        raise HTTPException(
            status_code=404,
            detail=f"Product with EAN {ean} not found",
        )

    product_responses = await prepare_product_response(
        products=products,
        date=date,
        filtered_chains=(
            [c.lower().strip() for c in chains.split(",")] if chains else None
        ),
    )

    if not product_responses:
        with_chains = " with specified chains" if chains else ""
        raise HTTPException(
            status_code=404,
            detail=f"No product information found for EAN {ean}{with_chains}",
        )

    return product_responses[0]


@router.get("/products/", summary="Search for products by name")
async def search_products(
    q: str = Query(..., description="Search query for product names"),
    date: datetime.date = Query(
        None,
        description="Date in YYYY-MM-DD format, defaults to today",
    ),
    chains: str = Query(
        None,
        description="Comma-separated list of chain codes to include",
    ),
) -> ProductSearchResponse:
    """
    Search for products by name.

    Returns a list of products that match the search query.
    """
    if not q.strip():
        return ProductSearchResponse(products=[])

    products = await db.search_products(q)

    product_responses = await prepare_product_response(
        products=products,
        date=date,
        filtered_chains=(
            [c.lower().strip() for c in chains.split(",")] if chains else None
        ),
    )

    return ProductSearchResponse(products=product_responses)


class ChainSummariesResponse(BaseModel):
    chain_summaries: list[ChainSummary] = Field(
        ..., description="List chain summaries."
    )


@router.get(
    "/chain-summaries/",
    summary="Return summaries of currently loaded data per chain."
)
async def chain_summaries() -> ChainSummariesResponse:
    """Return summaries of currently loaded data per chain."""

    chain_summaries = await db.list_latest_chain_summaries()
    return ChainSummariesResponse(chain_summaries=chain_summaries)
