import pytest

from crawler.store.cities import _format, normalize_city

# (raw, expected canonical) covering every inconsistency class we unify.
CASES = [
    # diacritic stripping (the requester's example and friends)
    ("Pozega", "Požega"),
    ("Požega", "Požega"),
    ("POŽEGA", "Požega"),
    ("  pozega  ", "Požega"),
    ("Sibenik", "Šibenik"),
    ("Varazdin", "Varaždin"),
    ("Cakovec", "Čakovec"),
    ("Nasice", "Našice"),
    # casing / punctuation only
    ("Slavonski brod", "Slavonski Brod"),
    ("Biograd Na Moru", "Biograd na Moru"),
    # Đ / đ do not decompose under NFD — must fold to the same key
    ("Dakovo", "Đakovo"),
    ("Ðakovo", "Đakovo"),  # ETH mojibake
    ("Djakovo", "Đakovo"),  # Dj digraph
    ("Durdevac", "Đurđevac"),
    # abbreviations / truncations expanded to the official name
    ("Biograd", "Biograd na Moru"),
    ("Sv Ivan Zelina", "Sveti Ivan Zelina"),
    ("Sv.Kriz Zacretje", "Sveti Križ Začretje"),
    ("Grubisno", "Grubišno Polje"),
    ("Kostajnica", "Hrvatska Kostajnica"),
    # Istrian towns: Croatian-only short form (official register is bilingual)
    ("Pula", "Pula"),
    ("Vodnjan", "Vodnjan"),
    ("Fazana", "Fažana"),
    ("Novigrad(Cittanova)", "Novigrad"),
    # Zagreb districts collapse to Zagreb, except Sesvete kept separate
    ("Zagreb Prečko", "Zagreb"),
    ("Zagreb - Sesvete", "Sesvete"),
]


@pytest.mark.parametrize("raw,expected", CASES)
def test_normalize_known_cities(raw, expected):
    assert normalize_city(raw) == expected


@pytest.mark.parametrize("blank", ["", "   ", None, "\t"])
def test_blank_city_yields_empty(blank):
    assert normalize_city(blank) == ""


# Heuristic fallback (unmapped cities): title-case each hyphen-separated part too,
# so "Tar-Vabriga" keeps its capital V instead of becoming "Tar-vabriga".
@pytest.mark.parametrize(
    "raw,expected",
    [
        ("tar-vabriga", "Tar-Vabriga"),
        ("Split-Ravne njive", "Split-Ravne Njive"),
        ("VARAŽDIN-BIŠKUPEC", "Varaždin-Biškupec"),
        ("biograd na moru", "Biograd na Moru"),  # connectors still lowercased
    ],
)
def test_format_titlecases_hyphenated_parts(raw, expected):
    assert _format(raw) == expected


@pytest.mark.parametrize("raw,expected", CASES)
def test_normalize_is_idempotent(raw, expected):
    assert normalize_city(expected) == expected
