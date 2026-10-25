from io import BytesIO

import openpyxl
import pytest

from crawler.store.djelo_vodice import DjeloVodiceCrawler

HEADER = [
    "Šifra prodajnog mjesta",
    "Naziv prodajnog mjesta",
    "Šifra artikla",
    "Naziv artikla",
    "Sidrena cijena",
    "Barkod",
    "Jedinica mjere",
    "Kategorija",
    "MP cijena",
    "Cijena za jedinicu mj.",
    "Obr. jedinica mjere",
    "Neto količina",
]
ROW = [
    "02010",
    "DUĆAN 10 VODICE",
    "010001",
    "ČOKOLADA MARS 51G",
    1.09,
    "5900951311321",
    "KOMAD",
    "ČOKOLADNI BAR",
    1.23,
    24.12,
    "KILOGRAM",
    0.0,
]


def _xlsx(drop: str | None = None, reorder: bool = False) -> bytes:
    header, row = list(HEADER), list(ROW)
    if drop:
        i = header.index(drop)
        del header[i], row[i]
    if reorder:
        header.reverse()
        row.reverse()

    workbook = openpyxl.Workbook()
    workbook.active.append(header)
    workbook.active.append(row)
    buf = BytesIO()
    workbook.save(buf)
    return buf.getvalue()


@pytest.mark.parametrize("reorder", [False, True])
def test_columns_are_read_by_name(reorder):
    store, [product] = DjeloVodiceCrawler().parse_excel(_xlsx(reorder=reorder))

    assert store.store_id == "02010"
    assert product.product_id == "010001"
    assert str(product.price) == "1.23"
    assert str(product.anchor_price) == "1.09"
    assert product.category == "ČOKOLADNI BAR"
    assert product.unit == "KILOGRAM"


def test_missing_optional_column_still_parses():
    _, [product] = DjeloVodiceCrawler().parse_excel(_xlsx(drop="Kategorija"))

    assert product.category == ""


def test_missing_required_column_raises():
    with pytest.raises(ValueError, match="MP cijena"):
        DjeloVodiceCrawler().parse_excel(_xlsx(drop="MP cijena"))
