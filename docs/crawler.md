# Crawler Architecture Documentation

## Overview

The crawler system is designed to extract price data from various Croatian retail chains' websites. Each chain has its own crawler implementation that follows a standardized architecture pattern, ensuring consistency and maintainability across all implementations.

## Architecture Overview

### Core Components

```
crawler/
├── store/
│   ├── __init__.py
│   ├── base.py              # BaseCrawler abstract class
│   ├── models.py            # Product and Store data models
│   ├── output.py            # CSV output and ZIP archive creation
│   ├── utils.py             # Common utility functions
│   └── [chain_name].py      # Individual store implementations
```

### Data Flow

1. **Initialization**: Crawler instantiated with HTTP client and configuration
2. **Index Fetching**: Download and parse store index pages/APIs
3. **Data Retrieval**: Download individual store price lists (CSV/XML/JSON)
4. **Data Parsing**: Parse raw data into standardized `Product` and `Store` objects
5. **Output Generation**: Transform data into CSV files and ZIP archives

## Base Crawler Class

### BaseCrawler (`base.py`)

All store crawlers inherit from `BaseCrawler`, which provides:

#### Required Class Attributes

```python
class MyCrawler(BaseCrawler):
    CHAIN = "store_name"           # Lowercase chain identifier
    BASE_URL = "https://..."       # Base website URL
    INDEX_URL = "https://..."      # Price list index URL (optional)

    # Price field mappings: field_name -> (column_name, is_required)
    PRICE_MAP = {
        "price": ("MPC", True),
        "unit_price": ("Cijena po jedinici", True),
        "special_price": ("Akcijska cijena", False),
        "best_price_30": ("Najniža cijena 30 dana", False),
        "anchor_price": ("Sidrena cijena", False),
    }

    # Other field mappings
    FIELD_MAP = {
        "product": ("Naziv proizvoda", True),
        "product_id": ("Šifra proizvoda", True),
        "brand": ("Marka", False),
        "quantity": ("Količina", False),
        "unit": ("Jedinica mjere", False),
        "barcode": ("Barkod", False),
        "category": ("Kategorija", False),
    }
```

#### Optional Configuration

```python
class MyCrawler(BaseCrawler):
    TIMEOUT = 30.0                 # HTTP timeout in seconds
    USER_AGENT = "Custom Agent"    # Custom user agent
    VERIFY_TLS_CERT = True         # TLS certificate verification
    MAX_RETRIES = 3                # HTTP retry attempts
    ZIP_DATE_PATTERN = re.compile(r"...") # Regex for date extraction from ZIP URLs
```

#### Core Methods

- `fetch_text(url, encodings=None, prefix=None)`: Download text content
- `fetch_binary(url, file_pointer)`: Download binary content
- `read_csv(text, delimiter=",")`: Parse CSV text into a `DictReader`
- `parse_price(price_str, required=False)`: Parse price strings to Decimal
- `parse_csv(content, delimiter=",")`: Parse CSV content to Product objects
- `parse_xml_product(element)`: Parse XML element to Product object
- `get_zip_contents(url, suffix)`: Download ZIP and yield `(filename, bytes)` for each matching file
- `get_all_products(date)`: **Abstract method** - main entry point for crawling

## Data Models

### Product Model (`models.py`)

```python
class Product(BaseModel):
    # Required fields
    product: str          # Product name
    product_id: str       # Store-specific product ID
    brand: str           # Brand name
    quantity: str        # Amount (e.g., "500g", "1L")
    unit: str           # Unit of measure
    price: Decimal      # Current retail price
    unit_price: Decimal # Price per unit
    barcode: str        # EAN/barcode
    category: str       # Product category

    # Optional fields
    best_price_30: Optional[Decimal] = None      # Lowest price in 30 days
    special_price: Optional[Decimal] = None      # Promotional price
    anchor_price: Optional[Decimal] = None       # Reference price (2.5.2025)
    anchor_price_date: Optional[str] = None      # Date of anchor price
    packaging: Optional[str] = None              # Packaging info
    initial_price: Optional[Decimal] = None      # Initial price for new products
    date_added: Optional[date] = None            # When product was added
```

### Store Model (`models.py`)

```python
class Store(BaseModel):
    chain: str              # Chain identifier (lowercase)
    store_id: str          # Chain-specific store ID
    name: str              # Display name
    store_type: str        # "supermarket", "hipermarket", etc.
    city: str              # City name
    street_address: str    # Street address
    zipcode: str = ""      # Postal code
    items: List[Product] = Field(default_factory=list)
```

## Implementation Patterns

### 1. Index-Based Crawlers

Most common pattern for stores that provide index pages with links to individual store price lists:

```python
def get_all_products(self, date: datetime.date) -> list[Store]:
    # 1. Get index page with store links
    csv_links = self.get_index(date)

    stores = []
    for url in csv_links:
        try:
            # 2. Parse store info from URL/filename
            store = self.parse_store_info(url)

            # 3. Download and parse price data
            products = self.get_store_prices(url)

            # 4. Combine store and products
            store.items = products
            stores.append(store)

        except Exception as e:
            logger.error(f"Error processing {url}: {e}")
            continue

    return stores
```

**Examples**: Konzum, KTC, Metro, NTL, Žabac, Brodokomerc, Trgovina Krk, Roto, Boso

### 2. ZIP Archive Crawlers

For stores that provide ZIP files containing multiple CSV/XML files, the base class provides a `get_zip_contents()` generator to simplify extraction. The typical implementation pattern is:

```python
def get_all_products(self, date: datetime.date) -> list[Store]:
    # 1. Find ZIP URL for the date
    zip_url = self.get_index(date)

    stores = []
    # 2. Extract and process each file in the ZIP
    for filename, content in self.get_zip_contents(zip_url, ".csv"):
        try:
            store = self.parse_store_from_filename(filename)
            products = self.parse_csv(content.decode("encoding"))
            store.items = products
            stores.append(store)
        except Exception as e:
            logger.error(f"Error processing {filename}: {e}")
            continue

    return stores
```

The `get_zip_contents()` method handles downloading the ZIP to a temporary file and yielding the name and content of each matching file inside.

**Note**: `StudenacCrawler` (`studenac.py`) overrides this method to use the `unzip` command-line tool, which can be more robust for certain archives. Studenac's ZIP contains XML files rather than CSVs.

See `PlodineCrawler.get_all_products()` for typical usage.

**Examples**: Plodine, Eurospin, Studenac

### 3. API-Based Crawlers

For stores with JSON APIs:

```python
def get_all_products(self, date: datetime.date) -> list[Store]:
    # 1. Fetch store list from API
    store_map = self.fetch_stores_list(date)

    stores = []
    for store_info, csv_url in store_map.items():
        try:
            store = self.parse_store_from_api_data(store_info)
            csv_content = self.fetch_text(csv_url)
            products = self.parse_csv(csv_content)
            store.items = products
            stores.append(store)
        except Exception as e:
            logger.error(f"Error processing {store_info}: {e}")
            continue

    return stores
```

**Examples**: Tommy, Spar

### 4. Single-File Crawlers

For stores with global pricing (not per-store):

```python
def get_all_products(self, date: datetime.date) -> list[Store]:
    # 1. Find the price file for the date
    file_url = self.find_price_file(date)

    # 2. Download and parse
    products = self.parse_price_file(file_url)

    # 3. Create single "global" store
    store = Store(
        chain=self.CHAIN,
        store_id="all",
        name=f"{self.CHAIN.capitalize()}",
        store_type="store",
        city="",
        street_address="",
        items=products
    )

    return [store]
```

**Examples**: DM, Lorenco, Jadranka Trgovina

### 5. Excel-Based Crawlers

A few stores provide data in Excel format (.xlsx) instead of CSV or XML. This requires a library like `openpyxl` to parse.

**Example**: `DmCrawler` in `dm.py` implements the `parse_excel` method to handle this.

### 6. XML-Based Crawlers

Some stores provide data as XML files (not inside ZIP archives). These crawlers fetch XML files directly and use XPath to extract product elements.

```python
def get_all_products(self, date: datetime.date) -> list[Store]:
    # 1. Get XML file URLs for the date
    xml_urls = self.get_index(date)

    stores = []
    for url in xml_urls:
        try:
            store = self.parse_store_from_filename(url)
            content = self.fetch_binary_content(url)
            products = self.parse_xml(content)
            store.items = products
            stores.append(store)
        except Exception as e:
            logger.error(f"Error processing {url}: {e}")
            continue

    return stores
```

**Examples**: Trgocentar, Vrutak, Ribola

### 7. Additional considerations

If a crawler needs to issue specific HTTP requests that don't fit into the standard pattern of fetching a text or binary resource,
it should use `self.client` (an initialized httpx instance) to make those requests. This allows for custom methods, headers, parameters,
and other HTTP features.

When parsing the HTML content, use the `BeautifulSoup` library (version 4) for parsing, and prefer CSS selectors for element selection,
like in this example:

```python
from bs4 import BeautifulSoup
...
soup = BeautifulSoup(content, "html.parser")
urls = []
csv_links = soup.select("a[href$='.csv']")

for link in csv_links:
    href = link.get("href")
    # we know href exists and is non-empty because of our selector
    urls.append(f"{self.BASE_URL}{href}")
```

## Common Implementation Steps

### 1. Store Information Parsing

Extract store details from filenames, URLs, or API responses:

```python
def parse_store_info(self, source: str) -> Store:
    # Common patterns:
    # - Regex parsing of filenames
    # - URL parameter extraction
    # - API response field mapping

    # Example filename: "SUPERMARKET_ULICA_NAZIV_12345_GRAD_001_20250515.csv"
    pattern = r"([^_]+)_(.+)_(\d{5})_([^_]+)_(\d+)_.*\.csv"
    match = re.match(pattern, filename)

    if match:
        store_type, address, zipcode, city, store_id = match.groups()
        return Store(
            chain=self.CHAIN,
            store_type=store_type.lower(),
            store_id=store_id,
            name=f"{self.CHAIN.capitalize()} {city}",
            street_address=address.replace("_", " ").title(),
            zipcode=zipcode,
            city=city.title(),
            items=[]
        )
```

### 2. Price Data Parsing

Use field mappings to parse CSV/XML data:

```python
def parse_csv_row(self, row: dict) -> Product:
    data = {}

    # Parse price fields
    for field, (column, is_required) in self.PRICE_MAP.items():
        value = row.get(column)
        data[field] = self.parse_price(value, is_required)

    # Parse other fields
    for field, (column, is_required) in self.FIELD_MAP.items():
        value = row.get(column, "").strip()
        if not value and is_required:
            raise ValueError(f"Missing required field: {field}")
        data[field] = value

    # Apply chain-specific fixes
    data = self.fix_product_data(data)

    return Product(**data)
```

### 3. XML Data Processing

For XML-based stores, use the `parse_xml_product` method from `BaseCrawler`.

- Define `PRICE_MAP` and `FIELD_MAP` with XML tag names instead of CSV columns.
- The base method uses simple XPath expressions. For more complex XML structures, you may need to override it.
- See `StudenacCrawler.parse_xml()` in `studenac.py` for store info parsing from XML.
- See `RibolaCrawler.parse_xml()` in `ribola.py` for a complete store and product parsing example.
- See `TrgocentarCrawler` in `trgocentar.py` for XML with filename-based store info and a known-cities list for address parsing.
- See `VrutakCrawler` in `vrutak.py` for date-grouped XML URLs parsed from an HTML table.

An XML field mapping would look like this, using tag names:
```python
FIELD_MAP = {
    "product": ("NazivProizvoda", True),  # Maps to <NazivProizvoda> tag
    "product_id": ("SifraProizvoda", True),
}
```

### 4. Date Pattern Matching

Many crawlers use the `ZIP_DATE_PATTERN` class attribute to find files for a specific date from a list of URLs.

```python
ZIP_DATE_PATTERN = re.compile(r".*_(\d{2})_(\d{2})_(\d{4})\.zip")
```

The `BaseCrawler` class provides a helper method, `parse_index_for_zip()`, which uses this pattern to return a dictionary of dates to URLs. For an example of how this is used, see the `get_index()` method in `PlodineCrawler` (`plodine.py`).

### 5. Error Handling and Logging

Consistent error handling across all crawlers:

```python
def get_all_products(self, date: datetime.date) -> list[Store]:
    logger.info(f"Starting {self.CHAIN.capitalize()} crawl for {date}")

    try:
        # Main crawling logic
        stores = self._do_crawl(date)

        # Log success metrics
        n_prices = sum(len(store.items) for store in stores)
        logger.info(f"Completed {self.CHAIN.capitalize()}: "
                   f"{len(stores)} stores, {n_prices} products")

        return stores

    except Exception as e:
        logger.error(f"Error crawling {self.CHAIN}: {e}", exc_info=True)
        raise
```

## Code Style and Conventions

### 1. Naming Conventions

- **Class names**: `ChainNameCrawler` (PascalCase)
- **File names**: `chain_name.py` (lowercase with underscores)
- **Chain identifiers**: `"chain_name"` (lowercase)
- **Methods**: `snake_case`
- **Constants**: `UPPER_CASE`

### 2. Method Organization

Standard method order in crawler implementations:

```python
class ChainCrawler(BaseCrawler):
    # 1. Class constants
    CHAIN = "chain"
    BASE_URL = "..."
    PRICE_MAP = {...}
    FIELD_MAP = {...}

    # 2. Index/URL discovery methods
    def get_index(self, date): ...
    def parse_index(self, content): ...

    # 3. Store information parsing
    def parse_store_info(self, source): ...

    # 4. Price data fetching and parsing
    def get_store_prices(self, url): ...
    def parse_csv_row(self, row): ...  # if needed

    # 5. Chain-specific data fixing
    def fix_product_data(self, data): ...  # if needed

    # 6. Main entry point
    def get_all_products(self, date): ...
```

### 3. Error Handling

- Use `try/except` blocks for individual store processing
- Log warnings for non-critical errors (e.g., parsing single products)
- Log errors for store-level failures
- Continue processing remaining stores on individual failures
- Raise exceptions only for complete crawl failures

### 4. Documentation

Each crawler should include:

```python
class ChainCrawler(BaseCrawler):
    """
    Crawler for Chain store prices.

    Brief description of the crawling approach and any special considerations.
    """

    def complex_method(self, param):
        """
        Method description.

        Args:
            param: Parameter description

        Returns:
            Return value description

        Raises:
            ValueError: When this specific error occurs
        """
```

Note: Docstrings should be concise but informative (one-liner is not enough), and added to all methods.

## Testing and Debugging

### Running Individual Crawlers

Each crawler includes a `__main__` block for testing:

```python
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = ChainCrawler()
    stores = crawler.crawl(datetime.date.today())
    print(stores[0])
    print(stores[0].items[0])
```

After implementing the crawler, you can run it directly to test functionality and inspect output. From the project root, run:

```bash
python -m crawler.store.<crawler_module>
```

To test the integration with the rest of the crawler, check that it is registered with:

```bash
python -m crawler.cli.crawl -l
```

### Common Debugging Patterns

1. **Enable debug logging**: `logging.basicConfig(level=logging.DEBUG)`
2. **Check field mappings**: Verify CSV/XML column names match expectations
3. **Test price parsing**: Use `parse_price()` method directly
4. **Inspect raw data**: Print fetched content before parsing
5. **Validate URLs**: Check if generated URLs are accessible

## Troubleshooting

### Common Issues

1. **Encoding Problems**
   - Try multiple encodings: `self.fetch_text(url, encodings=["windows-1250", "utf-8"])`
   - See `EurospinCrawler.get_store_prices()` for an example of encoding handling.

2. **Date Format Issues**
   - Check that your `ZIP_DATE_PATTERN` matches the date format in the URLs.
   - You can log available dates to debug mismatches. See `LidlCrawler.get_index()` for an example.

3. **Store Parsing Failures**
   - Test regex patterns with actual filenames from the website.
   - For complex address parsing, see `KauflandCrawler.parse_store_info()`.

4. **Empty Results**
   - Verify that the `date` parameter is handled correctly by the crawler and the website.
   - Check if the store provides historical data or only current prices. Some crawlers are date-agnostic.

### Debugging Approach

1. **Test URL fetching**: `content = crawler.fetch_text(url)`
2. **Test parsing**: `products = crawler.parse_csv(content)`
3. **Test individual components** before combining them in `get_all_products`.
4. **Enable debug logging** at the top of your `if __name__ == "__main__"` block: `logging.basicConfig(level=logging.DEBUG)`

## Output Format

### CSV Structure

The output system generates three CSV files per chain:

1. **stores.csv**: Store information
   - `store_id`, `type`, `address`, `city`, `zipcode`

2. **products.csv**: Unique products
   - `product_id`, `barcode`, `name`, `brand`, `category`, `unit`, `quantity`

3. **prices.csv**: Price data linking stores and products
   - `store_id`, `product_id`, `price`, `unit_price`, `best_price_30`, `anchor_price`, `special_price`

### ZIP Archive

The system creates a ZIP archive containing:
- All CSV files organized by chain
- `archive-info.txt` with format documentation

## Common Challenges and Solutions

### 1. Character Encoding

Many Croatian sites use Windows-1250 encoding:

```python
content = self.fetch_text(url, encodings=["windows-1250", "utf-8"])
```

### 2. Price Parsing

Handle various price formats:

```python
# Base class handles: "1,50€", "1.50", ",50", "1 200,50"
price = self.parse_price(price_str, required=False)
```

### 3. Date Extraction

Use regex patterns for date extraction:

```python
ZIP_DATE_PATTERN = re.compile(r".*_(\d{2})_(\d{2})_(\d{4})\.zip")
```

### 4. Store Address Parsing

Handle inconsistent address formats:

```python
def parse_address(self, address_str):
    # Try known city patterns first
    for city in self.CITIES:
        if address_str.endswith(city):
            street = address_str[:-len(city)].strip()
            return street, city

    # Fallback to regex or other methods
    return address_str, ""
```

## Adding a New Crawler

### Step-by-Step Guide

1. **Create new file**: `crawler/store/new_chain.py`

2. **Implement basic structure**:
```python
import datetime
import logging
from crawler.store.base import BaseCrawler
from crawler.store.models import Product, Store

logger = logging.getLogger(__name__)

class NewChainCrawler(BaseCrawler):
    CHAIN = "new_chain"
    BASE_URL = "https://www.newchain.hr"

    PRICE_MAP = {
        "price": ("Price Column", True),
        "unit_price": ("Unit Price Column", True),
        # ... other mappings
    }

    FIELD_MAP = {
        "product": ("Product Name Column", True),
        "product_id": ("Product ID Column", True),
        # ... other mappings
    }

    def get_all_products(self, date: datetime.date) -> list[Store]:
        # Implement crawling logic
        pass
```

3. **Implement required methods** based on the chain's data structure

4. **Test the implementation**:
```python
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    crawler = NewChainCrawler()
    stores = crawler.crawl(datetime.date.today())
    print(f"Found {len(stores)} stores")
```

5. **Add to crawler registry** in `crawler/crawl.py`:

To register the new crawler, add it to the `CRAWLERS` dictionary in `crawler/crawl.py`:

```python
from crawler.store.new_chain import NewChainCrawler
...
CRAWLERS = {
    # ... existing chains
    NewChainCrawler.CHAIN: NewChainCrawler,
}
```

### Pre-Implementation Checklist

- [ ] Understand the chain's price list structure (CSV/XML/JSON)
- [ ] Identify how to get the index/list of stores
- [ ] Determine date format used in URLs/filenames
- [ ] Map CSV/XML columns to our data model
- [ ] Identify store information location (filename, URL, API)
- [ ] Check character encoding requirements
- [ ] Test with different dates to ensure robustness
- [ ] Determine if the site provides historical data or only current prices
- [ ] Check if ZIP files require special handling (see `StudenacCrawler`)
- [ ] Verify date format in URLs matches your `ZIP_DATE_PATTERN`
- [ ] Test with both available and unavailable dates
- [ ] Check if the site uses non-standard formats (Excel, XML, JSON API)

This architecture provides a solid foundation for implementing new crawlers while maintaining consistency and reliability across all retail chain implementations.
