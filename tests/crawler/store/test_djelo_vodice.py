import datetime
from io import BytesIO
from urllib.parse import quote, unquote

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
    "Raspoloživo",
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
    "Ne",
]


def _xlsx(
    drop: str | None = None, reorder: bool = False, store_id: str = "02010"
) -> bytes:
    header, row = list(HEADER), list(ROW)
    row[0] = store_id
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
    assert product.available is False


@pytest.mark.parametrize("column", ["Kategorija", "Raspoloživo"])
def test_missing_optional_column_still_parses(column):
    _, [product] = DjeloVodiceCrawler().parse_excel(_xlsx(drop=column))

    assert product.price is not None


def test_missing_required_column_raises():
    with pytest.raises(ValueError, match="MP cijena"):
        DjeloVodiceCrawler().parse_excel(_xlsx(drop="MP cijena"))


def _crawler(monkeypatch, *names: str) -> DjeloVodiceCrawler:
    """Crawler whose index page links to the given file names."""
    links = "".join(f'<a href="{quote(name)}">{name}</a>' for name in names)
    page = f"<html><body>{links}</body></html>"

    crawler = DjeloVodiceCrawler()
    monkeypatch.setattr(crawler, "fetch_text", lambda url, *args, **kwargs: page)
    return crawler


def test_index_has_one_file_per_store_for_the_date(monkeypatch):
    crawler = _crawler(
        monkeypatch,
        "SUPERMARKET#PUT GA?ELEZA 5A#VODICE#02010#26317#2026-11-13T010506.xlsx",
        "PRODAVAONICA#PUT GAĆELEZA 5A#22211 VODICE#02010#26318#2026-11-14T010512.xlsx",
        "PRODAVAONICA#ČIKOLE 2#22320 DRNIŠ#02044#26318#2026-11-14T010557.xlsx",
        "PRODAVAONICA#ČIKOLE 2#22320 DRNIŠ#02044#26318#2026-11-14T120000.xlsx",
        "PRODAVAONICA#PUT BILOSTANA 1##02066#26318#2026-11-14T010622.xlsx",
        "PRODAVAONICA###05075#26318#2026-11-14T010634.xlsx",
    )

    files = dict(
        (unquote(url).split("#")[3], (unquote(url), info))
        for url, info in crawler.get_index(datetime.date(2026, 11, 14))
    )

    assert sorted(files) == ["02010", "02044", "02066"]
    assert files["02044"][0].endswith("T120000.xlsx")
    assert files["02044"][1] == {
        "store_type": "prodavaonica",
        "street_address": "ČIKOLE 2",
        "city": "DRNIŠ",
        "zipcode": "22320",
    }
    assert files["02066"][1]["city"] == ""


def test_index_reads_the_old_file_name(monkeypatch):
    crawler = _crawler(
        monkeypatch,
        "SUPERMARKET#PUT GA?ELEZA 5A#VODICE#02010#26317#2026-11-13T010506.xlsx",
    )

    [(_, info)] = crawler.get_index(datetime.date(2026, 11, 13))

    assert info["city"] == "VODICE"
    assert info["zipcode"] == ""


def test_index_without_files_for_the_date_raises(monkeypatch):
    crawler = _crawler(
        monkeypatch,
        "PRODAVAONICA###05075#26318#2026-11-14T010634.xlsx",
    )

    with pytest.raises(ValueError, match="No XLSX file"):
        crawler.get_index(datetime.date(2026, 11, 14))


def test_store_comes_from_the_file_name():
    info = {
        "store_type": "prodavaonica",
        "street_address": "ČIKOLE 2",
        "city": "DRNIŠ",
        "zipcode": "22320",
    }

    store, _ = DjeloVodiceCrawler().parse_excel(_xlsx(store_id="02044"), info)

    assert store.store_id == "02044"
    assert store.store_type == "prodavaonica"
    assert store.street_address == "ČIKOLE 2"
    assert store.city == "DRNIŠ"
    assert store.zipcode == "22320"


def test_stores_map_overrides_the_file_name():
    info = {"store_type": "supermarket", "street_address": "PUT GA?ELEZA 5A"}

    store, _ = DjeloVodiceCrawler().parse_excel(_xlsx(), info)

    assert store.street_address == "Put Gaćeleza 5A"
    assert store.zipcode == "22211"
