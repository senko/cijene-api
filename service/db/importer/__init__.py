"""
Import package for retail chain price data.

This package provides functionality to import price data from CSV files
organized in directories or zip archives. The data is processed and stored
in the database according to the schema defined in service.db.models.

Main entry points:
- cli.main(): Command-line interface for importing data
- archive_handler.import_archive(): Import from zip archive
- archive_handler.import_directory(): Import from directory

The package is organized into the following modules:
- cli: Command-line interface
- archive_handler: Archive and directory processing
- chain_importer: Chain-level import logic
- processors: Data processing functions (stores, products, prices)
- csv_reader: CSV file reading utilities
"""

from .archive_handler import import_archive, import_directory
from .processors import process_stores, process_products, process_prices
from .csv_reader import read_csv

__all__ = [
    "import_archive",
    "import_directory",
    "process_stores",
    "process_products",
    "process_prices",
    "read_csv",
]
