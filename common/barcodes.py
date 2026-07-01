"""
Canonical EAN/barcode normalization, shared by the crawler and the service.

Some chains prefix their EAN barcodes with leading zeros (``0000054490970``)
while others store the bare form (``54490970``). Since the same GTIN is the same
number regardless of zero padding, these are false duplicates that break
cross-chain product matching. This module strips the leading zeros so one product
maps to one barcode.

Only *real* EANs are touched: 8+ digit all-numeric strings. Synthetic
``<chain>:<product_id>`` codes (which contain ``:``) and short/non-numeric values
are left alone. GS1 Restricted Circulation Numbers (RCN — region/in-store codes
that are NOT globally unique GTINs) are also left as-is so they are never merged
across chains; these are detected on the canonical fixed-length form, not the
zero-stripped one (see :func:`_is_restricted`).
"""

from __future__ import annotations


def _is_restricted(digits: str) -> bool:
    """Whether a zero-stripped numeric barcode is a GS1 Restricted Circulation
    Number (region/in-store code), which must not be merged across chains.

    RCN prefixes (GS1 General Specifications), checked on the canonical
    fixed-length GTIN form — NOT the stripped form, because stripping leading
    zeros can expose an internal digit and misclassify a normal GTIN:

    * RCN-8  (GTIN-8) : leading digit ``0`` or ``2`` (ranges 000-099, 200-299).
    * RCN-13 (GTIN-13): prefix ``02``, ``04`` or ``20``-``29``. A UPC-A (GTIN-12)
      is checked zero-extended to 13 digits, so a number-system-2 UPC-A surfaces
      as ``02`` (restricted) while a normal number-system-0 UPC-A surfaces as
      ``00`` (not restricted).

    So e.g. Del Monte ``024000011859`` / OGX ``022796976024`` (number-system 0)
    are NOT restricted and get merged, while a genuine in-store ``20...`` EAN-13
    or random-weight ``2...`` UPC-A is left intact.
    """
    n = len(digits)
    if n <= 8:
        return digits.zfill(8)[0] in ("0", "2")
    if n <= 13:
        prefix = digits.zfill(13)[:2]
    else:
        # GTIN-14: classify by its embedded GTIN-13 (skip the indicator digit).
        prefix = digits.zfill(14)[1:3]
    return prefix in ("02", "04") or "20" <= prefix <= "29"


def normalize_barcode(barcode: str | None) -> str | None:
    """Canonicalize a real EAN by stripping leading zeros.

    Returns the input unchanged if there is nothing to normalize.
    Leaves untouched: synthetic ``chain:product_id`` codes (contain ``:``),
    non-numeric or <8-digit values, and GS1 Restricted Circulation Numbers
    (see :func:`_is_restricted`).

    Idempotent: ``normalize_barcode(normalize_barcode(x)) == normalize_barcode(x)``.
    """
    if not barcode:
        return barcode

    bc = barcode.strip()

    # Synthetic chain-specific code, e.g. "konzum:12345" — never an EAN.
    if ":" in bc:
        return bc

    # Not a real EAN (too short to be a GTIN-8, or non-numeric) — leave as-is.
    if not (len(bc) >= 8 and bc.isdigit()):
        return bc

    stripped = bc.lstrip("0")

    # Degenerate all-zeros, or a restricted-circulation/in-store code that is not
    # a globally unique GTIN: don't strip.
    if not stripped or _is_restricted(stripped):
        return bc

    return stripped
