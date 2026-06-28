"""
Canonical Croatian city-name normalization for store output.

City names arrive from each chain with inconsistent spelling: many chains strip
Croatian diacritics ("Pozega" vs "Požega"), others differ in casing or
punctuation ("Slavonski brod", "Sv.Kriz Zacretje"). This module maps each raw
city name to a single canonical form so the exported ``stores.csv`` (and
everything downstream) is consistent.

The canonical names are verified against official sources (the Croatian
"Općine i gradovi RH" register, GeoNames HR postal places, and a hand-curated
override list). An unmapped city falls back to heuristic title-casing and is
logged so it can be added to the map.
"""

from __future__ import annotations

import csv
import re
import unicodedata
from functools import lru_cache
from logging import getLogger
from pathlib import Path

logger = getLogger(__name__)

# The map lives in the repo-root enrichment/ folder (it is a data-cleanup layer,
# like the other enrichment CSVs).
_CITIES_CSV = Path(__file__).parents[2] / "enrichment" / "cities.csv"

# Croatian "D with stroke" (Đ/đ) and the common ETH mojibake (Ð/ð) do not
# decompose under NFD, so map them to plain ascii explicitly before stripping
# the remaining combining marks. Without this "Đakovo" would not fold to the
# same key as "Dakovo".
_DSTROKE = str.maketrans({"Đ": "D", "đ": "d", "Ð": "D", "ð": "d"})

# Connector words that stay lowercase inside a multi-word name.
_CONNECTORS = {"na", "i", "pod", "kraj", "nad", "uz", "u"}


def _strip_diacritics(text: str) -> str:
    text = text.translate(_DSTROKE)
    return "".join(
        c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn"
    )


def _has_diacritics(text: str) -> bool:
    return _strip_diacritics(text) != text


def _key(city: str) -> str:
    """Normalized lookup key: ascii-folded, lowercased, punctuation collapsed."""
    s = _strip_diacritics(city).lower()
    s = re.sub(r"[\s\-_.()]+", " ", s).strip()
    return s


def _titlecase(token: str) -> str:
    """Capitalize each hyphen-separated part: "tar-vabriga" -> "Tar-Vabriga"."""
    return "-".join(p[0].upper() + p[1:].lower() if p else p for p in token.split("-"))


def _format(city: str) -> str:
    """Heuristic display formatting for cities not present in the map.

    Trims and collapses whitespace and title-cases words (each hyphen-separated
    part too, so "Tar-Vabriga" keeps its capital V), keeping Croatian connector
    words ("na", "i", ...) lowercase. Does NOT invent diacritics.
    """
    s = re.sub(r"\s+", " ", city.strip())
    out = []
    for i, word in enumerate(s.split(" ")):
        if not word:
            continue
        if i > 0 and word.lower() in _CONNECTORS:
            out.append(word.lower())
        else:
            out.append(_titlecase(word))
    return " ".join(out)


@lru_cache(maxsize=1)
def _city_map() -> dict[str, str]:
    mapping: dict[str, str] = {}
    if not _CITIES_CSV.exists():
        logger.warning("cities.csv not found at %s; using heuristics only", _CITIES_CSV)
        return mapping
    with open(_CITIES_CSV, encoding="utf-8") as f:
        for row in csv.DictReader(f):  # type: ignore
            key = row.get("key", "").strip()
            if key:
                mapping[key] = row.get("canonical", "").strip()
    return mapping


def normalize_city(city: str | None) -> str:
    """Return the canonical city name for a raw chain-provided value.

    Empty/blank input yields an empty string. Known cities are mapped to their
    verified canonical spelling; unknown cities are heuristically formatted and
    logged so they can be added to the map.
    """
    if not city or not city.strip():
        return ""
    key = _key(city)
    mapping = _city_map()
    if key in mapping:
        return mapping[key]
    formatted = _format(city)
    logger.info("Unmapped city %r (key=%r) -> %r", city, key, formatted)
    return formatted
