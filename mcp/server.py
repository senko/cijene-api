"""
MCP server for querying api.cijene.dev.
"""

import datetime
import httpx
import logging
from mcp.server.fastmcp import FastMCP
from pydantic_settings import BaseSettings
from typing import Any


class Settings(BaseSettings):
    CIJENE_API_BASE: str = "https://api.cijene.dev"
    CIJENE_API_KEY: str = ""
    LISTEN_PORT: int = 0


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
settings = Settings()
mcp = FastMCP("Cijene")

# Global HTTP client for connection pooling
_client: httpx.AsyncClient | None = None


async def get_client() -> httpx.AsyncClient:
    global _client
    if _client is None:
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        if settings.CIJENE_API_KEY:
            headers["Authorization"] = f"Bearer {settings.CIJENE_API_KEY}"
        _client = httpx.AsyncClient(headers=headers, timeout=30.0)
    return _client


async def call_api(path: str, params: dict[str, Any] | None = None) -> Any:
    """Calls the API and returns the response as a JSON object (dict/list)."""
    client = await get_client()
    try:
        response = await client.get(path, params=params)
        response.raise_for_status()
        return response.json()
    except httpx.RequestError as e:
        logger.error(f"Request error fetching {path}: {e}")
        return {"error": f"Request error: {str(e)}"}
    except httpx.HTTPStatusError as e:
        logger.error(f"HTTP error {e.response.status_code} fetching {path}: {e}")
        return {"error": f"API error {e.response.status_code}: {e.response.text}"}
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return {"error": f"An unexpected error occurred: {str(e)}"}


@mcp.tool()
async def check_health() -> Any:
    """
    Health check endpoint.
    """
    return await call_api(f"{settings.CIJENE_API_BASE}/health")


@mcp.tool()
async def list_archives() -> Any:
    """
    List all available ZIP archive data files with their metadata.
    """
    return await call_api(f"{settings.CIJENE_API_BASE}/v0/list")


@mcp.tool()
async def list_chains() -> Any:
    """
    List all available retail chains.
    """
    return await call_api(f"{settings.CIJENE_API_BASE}/v1/chains/")


@mcp.tool()
async def list_chain_stores(chain: str) -> Any:
    """
    List all available stores for a specified chain name.
    """
    if not chain or not chain.strip():
        return {"error": "chain parameter cannot be empty."}
    return await call_api(f"{settings.CIJENE_API_BASE}/v1/{chain}/stores/")


@mcp.tool()
async def search_stores(
    chains: list[str] | None = None,
    city: str | None = None,
    address: str | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
    distance_km: float = 10,
) -> Any:
    """
    Search for stores by chain codes, city, address, and/or geolocation.

    For geolocation search, both lat and lon must be provided together.
    Note that the geolocation search will only return stores that have
    the geo information available in the database.
    """
    params: dict[str, Any] = {"chains": "all"}
    if chains:
        params["chains"] = ",".join(chains)
    if city:
        params["city"] = city
    if address:
        params["address"] = address
    if latitude:
        params["lat"] = latitude
    if longitude:
        params["lon"] = longitude
    if distance_km:
        params["d"] = distance_km

    return await call_api(f"{settings.CIJENE_API_BASE}/v1/stores/", params=params)


@mcp.tool()
async def search_prices(
    barcodes: list[str],
    chains: list[str] | None = None,
    city: str | None = None,
    address: str | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
    distance_km: float = 10,
) -> Any:
    """
    Get product prices by store with store filtering capabilities.

    Returns prices for products in stores matching the filter criteria. For
    geolocation search, both lat and lon must be provided together. The EANs
    parameter is required and must contain at least one EAN code.
    """
    params: dict[str, Any] = {"eans": ",".join(barcodes)}
    if chains:
        params["chains"] = ",".join(chains)
    if city:
        params["city"] = city
    if address:
        params["address"] = address
    if latitude:
        params["lat"] = latitude
    if longitude:
        params["lon"] = longitude
    if distance_km:
        params["d"] = distance_km

    return await call_api(f"{settings.CIJENE_API_BASE}/v1/prices/", params=params)


@mcp.tool()
async def list_chain_stats() -> Any:
    """
    Return stats of currently loaded data per chain.
    """
    return await call_api(f"{settings.CIJENE_API_BASE}/v1/chain-stats/")


@mcp.tool()
async def search_product_barcode(
    barcode: str,
    date: datetime.date | None = None,
    chains: list[str] | None = None,
) -> Any:
    """
    Get product information including chain products and prices by their
    EAN barcode. For products that don't have official EAN codes and use
    chain-specific codes, use the "chain:<product_code>" format.

    The price information is for the last known date earlier than or equal to
    the specified date. If no date is provided, current date is used.
    """
    if not barcode or not barcode.strip():
        return {"error": "barcode parameter cannot be empty."}
    params = {}
    if chains:
        params["chains"] = ",".join(chains)
    if date:
        params["date"] = date.strftime("%Y-%m-%d")

    return await call_api(
        f"{settings.CIJENE_API_BASE}/v1/products/{barcode}/", params=params
    )


@mcp.tool()
async def search_products(
    name: str,
    date: datetime.date | None = None,
    chains: list[str] | None = None,
    fuzzy: bool = True,
    limit: int = 20,
) -> Any:
    """
    Search for products by name.
    """
    params = {"q": name, "fuzzy": fuzzy, "limit": limit}
    if chains:
        params["chains"] = ",".join(chains)
    if date:
        params["date"] = date.strftime("%Y-%m-%d")

    return await call_api(f"{settings.CIJENE_API_BASE}/v1/products/", params=params)


if __name__ == "__main__":
    if settings.LISTEN_PORT:
        mcp.settings.port = settings.LISTEN_PORT
        mcp.run(transport="streamable-http")
    else:
        mcp.run(transport="stdio")
