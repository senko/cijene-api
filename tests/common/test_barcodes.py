import pytest

from common.barcodes import normalize_barcode

# (raw, expected) covering every class of input the normalizer handles.
CASES = [
    # zero-padded real EANs -> stripped to the bare GTIN (the core fix)
    ("0000054490970", "54490970"),  # the requester's SPRITE example (GTIN-8)
    ("011210000018", "11210000018"),
    ("0011210000018", "11210000018"),
    ("0000038381423", "38381423"),
    ("03856003919491", "3856003919491"),
    # zero-padded UPC-A whose stripped form exposes an internal "2" but whose
    # number system is 0 (NOT restricted) -> still stripped/merged
    ("0024000011859", "24000011859"),  # Del Monte (UPC-A 024000...)
    ("0022796976024", "22796976024"),  # OGX (UPC-A 022796...)
    ("0020648100221", "20648100221"),
    # bare EANs unchanged
    ("54490970", "54490970"),
    ("3856003919491", "3856003919491"),
    ("24000011859", "24000011859"),
    # surrounding whitespace trimmed before normalizing
    ("  0000054490970  ", "54490970"),
    # synthetic chain:product_id codes left alone (even if zero-padded-looking)
    ("konzum:01310151", "konzum:01310151"),
    ("lorenco:0001", "lorenco:0001"),
    # sub-8-digit / non-numeric values left alone
    ("1", "1"),
    ("001", "001"),
    ("12345", "12345"),
    ("ABC123", "ABC123"),
    # GS1 Restricted Circulation Numbers (in-store / region codes): NOT stripped.
    # RCN-8: 8-digit GTIN-8 with leading digit 0 or 2.
    ("20490970", "20490970"),
    ("0000020490970", "0000020490970"),  # same RCN-8 padded -> left padded
    ("05449097", "05449097"),
    # RCN-13: EAN-13 prefix 20-29, or number-system 2/4 UPC-A (-> "02"/"04").
    ("2001234500009", "2001234500009"),
    ("212345678901", "212345678901"),  # number-system-2 UPC-A (random weight)
    ("0212345678901", "0212345678901"),  # same, padded -> left padded
    ("412345678901", "412345678901"),  # number-system-4 UPC-A (in-store)
    # degenerate all-zeros guarded (don't return empty)
    ("00000000", "00000000"),
]


@pytest.mark.parametrize("raw,expected", CASES)
def test_normalize_barcode(raw, expected):
    assert normalize_barcode(raw) == expected


@pytest.mark.parametrize("blank", ["", None])
def test_blank_passthrough(blank):
    # Falsy input is returned verbatim (None stays None, "" stays "").
    assert normalize_barcode(blank) == blank


@pytest.mark.parametrize("raw,expected", CASES)
def test_normalize_is_idempotent(raw, expected):
    assert normalize_barcode(expected) == expected
