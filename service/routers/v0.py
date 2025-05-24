from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
from zoneinfo import ZoneInfo
from fastapi import APIRouter

from service.config import settings

router = APIRouter()


def format_datetime_with_timezone(dt: datetime) -> str:
    """
    Format a datetime object with proper timezone information.

    Args:
        dt: A naive datetime object (assumed to be in the configured timezone)

    Returns:
        ISO 8601 formatted datetime string with timezone offset
    """
    # Assume the naive datetime is in the configured timezone
    timezone = ZoneInfo(settings.timezone)
    dt_with_tz = dt.replace(tzinfo=timezone)

    # Remove microseconds and format as ISO string
    return dt_with_tz.replace(microsecond=0).isoformat()


def find_archives() -> list[tuple[str, int, datetime]]:
    """
    Find all ZIP archive files in the archive directory and return their metadata.

    Assumes the filename pattern is `YYYY-MM-DD.zip`.

    Returns:
        List of tuples containing the date (filename without extension),
        size, and modification time of each archive.
    """

    archive_path = Path(settings.archive_dir)
    if not archive_path.is_dir():
        return []

    archives = []
    for file_path in archive_path.iterdir():
        if file_path.is_file() and file_path.suffix == ".zip":
            date = file_path.stem
            size = file_path.stat().st_size
            mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
            archives.append((date, size, mtime))

    # Sort by date in reverse chronological order (newest first)
    archives.sort(key=lambda x: x[0], reverse=True)
    return archives


@router.get("/list")
async def list_archives() -> Dict[str, List[Dict[str, Any]]]:
    """List all available archive files with their metadata."""

    archives = []
    for date, size, mtime in find_archives():
        url = f"{settings.base_url}/v0/archive/{date}.zip"
        updated = format_datetime_with_timezone(mtime)
        archives.append({"date": date, "url": url, "size": size, "updated": updated})

    return {"archives": archives}
