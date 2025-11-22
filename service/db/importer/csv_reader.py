import logging
from csv import DictReader
from pathlib import Path
from typing import Dict, List

logger = logging.getLogger("importer.csv_reader")


async def read_csv(file_path: Path) -> List[Dict[str, str]]:
    """
    Read a CSV file and return a list of dictionaries.

    Args:
        file_path: Path to the CSV file.

    Returns:
        List of dictionaries where each dictionary represents a row in the CSV.
        Returns empty list if file cannot be read.
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            reader = DictReader(f)  # type: ignore
            return [row for row in reader]
    except FileNotFoundError:
        logger.error(f"CSV file not found: {file_path}")
        return []
    except PermissionError:
        logger.error(f"Permission denied reading CSV file: {file_path}")
        return []
    except UnicodeDecodeError as e:
        logger.error(f"Encoding error reading CSV file {file_path}: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error reading CSV file {file_path}: {e}")
        return []
