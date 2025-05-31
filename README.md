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
uv sync --dev --extra crawler --extra service
. .venv/bin/activate
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
```bash
# Primjer pokretanja crawlera samo za jedan lanac (npr. `konzum`) za današnji dan (CSV)
python -m crawler.cli.crawl -c konzum /path/to/output-folder/
```

```bash
# Primjer pokretanja crawlera samo za jedan lanac (npr. `konzum`) za današnji dan (PostgreSQL)
python -m crawler.cli.crawl -c konzum -s /path/to/output-folder/
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

## Docker

Docker container crawler ima cronjob za pokretanje u 09h i 21h, definiran u `docker/crawlercron`

Za pokretanje crawlera u Dockeru, potrebno je imati instaliran Docker i Docker Compose.
Zatim, u root direktoriju projekta, pokrenite sljedeće komande:

```bash
docker compose up --build
```

Help:

```bash
docker compose exec -it crawler python -m crawler.cli.crawl -h
```

Primjer za pokretanje crawlera za KTC:

```bash
docker compose exec -it crawler python -m crawler.cli.crawl -c ktc /data
## Docker DB backup

Backup (with drop statements for existing tables, data and indexes):

```bash
docker compose exec db pg_dump -U $POSTGRES_USER -d $POSTGRES_DB --clean > data/backup.sql

```

```bash
docker compose exec db pg_dump -U $POSTGRES_USER -d $POSTGRES_DB --clean | gzip > data/backup.sql.gz
```

Restore:

or 
```bash
cat data/backup.sql | docker compose exec -T db psql -U $POSTGRES_USER -d $POSTGRES_DB
```

```bash
gunzip -c data/backup.sql.gz | docker compose exec -T db psql -U $POSTGRES_USER -d $POSTGRES_DB
```

## Crawl from csv

If you have CSV files from a previous crawl and want to import them into the database, 
you can use the `--from-csv-dir` option. This is useful for testing or when you want 
to re-import data without crawling again. It also saves new CSV files in the specified directory.

Unzipped archives available in `data/` directory can be used to import data into the 
database. Log and CSV files will be saved in the specified output directory `tmp/` in this example.

```bash
python -m crawler.cli.crawl -c zabac -d 2025-05-29 tmp --from-csv-dir data -v debug -s
```

For all chains available in the `data/` directory, on date `2025-05-15`, you can run:

```bash
python -m crawler.cli.crawl -d 2025-05-15 tmp --from-csv-dir data -v debug -s
```

## Licenca

Ovaj projekt je licenciran pod [AGPL-3 licencom](LICENSE).

Podaci prikupljeni putem ovog projekta su javni i dostupni svima, temeljem
Odluke o objavi cjenika i isticanju dodatne cijene kao mjeri izravne
kontrole cijena u trgovini na malo, NN 75/2025 od 2.5.2025.
