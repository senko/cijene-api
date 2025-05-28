# Cijene API

Servis za preuzimanje javnih podataka o cijenama proizvoda u trgovačkim lancima u Republici Hrvatskoj.

Preuzimanje podataka o cijenama proizvoda u trgovačkim lancima u Republici Hrvatskoj
temeljeno je na Odluci o objavi cjenika i isticanju dodatne cijene kao mjeri izravne
kontrole cijena u trgovini na malo, NN 75/2025 od 2.5.2025.

Trenutno podržani trgovački lanci:

* Konzum
* Lidl
* Plodine
* Spar
* Tommy
* Studenac
* Kaufland
* Eurospin
* dm
* KTC
* Metro
* Trgocentar
* Žabac
* Vrutak
* Ribola
* NTL

## Softverska implementacija

Softver je izgrađen na Pythonu a sastoji se od dva dijela:

* Crawler - preuzima podatke s web stranica trgovačkih lanaca (`crawler`)
* Web servis - API koji omogućava pristup podacima o cijenama proizvoda (`service`) - **U IZRADI**

## Instalacija

Za instalaciju crawlera potrebno je imati instaliran Python 3.13 ili noviji. Preporučamo
korištenje `uv` za setup projekta:

```bash
git clone https://github.com/senko/cijene-api.git
cd cijene-api
uv sync --dev
```

## Korištenje

### Crawler

Za pokretanje crawlera potrebno je pokrenuti sljedeću komandu:

```bash
uv run -m crawler.cli.crawl /path/to/output-folder/
```

Ili pomoću Pythona direktno (u adekvatnoj virtualnoj okolini):

```bash
python -m crawler.cli.crawl /path/to/output-folder/
```

Crawler prima opcije `-l` za listanje podržanih trgovačkih lanaca, `-d` za
odabir datuma (default: trenutni dan), `-c` za odabir lanaca (default: svi) te
`-h` za ispis pomoći.

### Pokretanje u Windows okolini

**Napomena:** Za Windows korisnike - postavite vrijednost `PYTHONUTF8` environment varijable na `1` ili pokrenite python s `-X utf8` flag-om kako bi izbjegli probleme s character encodingom. Više detalja [na poveznici](https://github.com/senko/cijene-api/issues/9#issuecomment-2911110424).

### Web servis

Prije pokretanja servisa, kreirajte datoteku `.env` sa konfiguracijskim varijablama.
Primjer datoteke sa zadanim (default) vrijednostima može se naći u `.env.example`.

Nakon što ste kreirali `.env` datoteku, pokrenite servis koristeći:

```bash
uv run -m service.main
```

Servis će biti dostupan na `http://localhost:8000` (ako niste mijenjali port), a na
`http://localhost:8000/docs` je dostupna Swagger dokumentacija API-ja.

## Licenca

Ovaj projekt je licenciran pod [AGPL-3 licencom](LICENSE).

Podaci prikupljeni putem ovog projekta su javni i dostupni svima, temeljem
Odluke o objavi cjenika i isticanju dodatne cijene kao mjeri izravne
kontrole cijena u trgovini na malo, NN 75/2025 od 2.5.2025.
