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

## Pohrana i Obrada Podataka

Sustav koristi PostgreSQL bazu podataka za pohranu prikupljenih informacija te namjenski servis za obradu CSV datoteka.

### PostgreSQL Baza Podataka
Podaci o proizvodima, cijenama, trgovinama i trgovačkim lancima sada se pohranjuju u PostgreSQL bazi podataka. Ovo omogućava strukturirano skladištenje i efikasnije dohvaćanje podataka. Glavne tablice uključuju `chains` (trgovački lanci), `stores` (pojedinačne trgovine), `products` (proizvodi) i `prices` (dnevne cijene). Interno, tablica `processed_batches` koristi se za praćenje obrađenih CSV datoteka kako bi se izbjeglo dupliciranje podataka. Kompletna shema baze podataka definirana je u datoteci `database/schema.sql`.

### CSV Procesor (`csv_processor` servis)
Novi servis, nazvan `csv_processor`, automatski obrađuje CSV datoteke koje generira crawler. Ovaj servis parsira podatke iz CSV datoteka (smještenih u `docker_data/latest/`) i unosi ih u PostgreSQL bazu. Kako bi se izbjeglo dvostruko procesiranje istih podataka, `csv_processor` računa SHA256 hash sadržaja CSV datoteka za svaku grupu (batch) i pamti obrađene grupe u `processed_batches` tablici. Ako se sadržaj CSV datoteka za određenu grupu nije promijenio, preskače se ponovni unos. Servis koristi environment varijable poput `DATABASE_URL` (za spajanje na bazu) i `CSV_DIR` (direktorij s CSV datotekama), koje su automatski konfigurirane unutar `docker-compose.yml`.

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

### Web servis

Prije pokretanja servisa, kreirajte datoteku `.env` sa konfiguracijskim varijablama.
Primjer datoteke sa zadanim (default) vrijednostima može se naći u `.env.example`.

Nakon što ste kreirali `.env` datoteku, pokrenite servis koristeći:

```bash
uv run -m service.main
```

Servis će biti dostupan na `http://localhost:8000` (ako niste mijenjali port), a na
`http://localhost:8000/docs` je dostupna Swagger dokumentacija API-ja. Očekuje se da će API dohvaćati podatke iz PostgreSQL baze.

## Docker Compose

Za pokretanje svih komponenti sustava (crawler, baza podataka, CSV procesor, API servis) putem Docker Compose:

```bash
# Kreiraj direktorij za trajne podatke
mkdir -p docker_data

# Preimenuj i prilagodi datoteku s konfiguracijskim varijablama (ako je potrebno)
cp .env.example .env

Važno: Nakon kopiranja `.env.example` u `.env`, potrebno je u `.env` datoteci postaviti vrijednosti za sljedeće varijable koje se koriste za konfiguraciju PostgreSQL baze podataka:
*   `POSTGRES_HOST` (npr. `db`, što je naziv servisa definiran u `docker-compose.yml`)
*   `POSTGRES_PORT` (npr. `5432`)
*   `POSTGRES_DB` (npr. `pricedb`)
*   `POSTGRES_USER` (npr. `priceuser`)
*   `POSTGRES_PASSWORD` (npr. `pricepassword`)

Ove postavke su neophodne za ispravan rad `db` servisa (za inicijalizaciju baze) i `csv_processor` servisa (za spajanje na bazu i unos podataka). Ukoliko `service` (API servis) bude konfiguriran za korištenje ove PostgreSQL baze, također će koristiti ove postavke. Vrijednosti navedene kao primjer (`db`, `5432`, `pricedb`, `priceuser`, `pricepassword`) su zadane vrijednosti koje koristi `docker-compose.yml` konfiguracija.

# Pokreni Docker Compose
docker compose up -d
```

Sustav sada uključuje sljedeće servise:
*   `crawler`: Preuzima podatke s web stranica trgovačkih lanaca i sprema ih kao CSV datoteke u `docker_data/latest/` direktorij.
*   `db`: PostgreSQL servis baze podataka. Podaci baze su trajno pohranjeni korištenjem Docker volume-a `postgres_data`.
*   `csv_processor`: Servis koji čita CSV datoteke iz `docker_data/latest` (koje generira `crawler`) i unosi ih u `db` servis. Ovaj servis ovisi o `db` servisu i pokreće se nakon njega. Izgrađen je pomoću `Dockerfile.csv_processor`.
*   `service`: API servis koji će (u budućnosti) dohvaćati podatke iz `db` servisa.

Standardni tijek podataka je: `crawler` preuzima podatke i sprema ih kao CSV datoteke -> `csv_processor` ih uvozi u bazu podataka -> `service` API dohvaća podatke iz baze.

# Rebuildanje kontejnera ako su napravljene promjene u kodu:

```bash
docker compose up -d --build
```

Crawler će se izvršavati svakodnevno u 09:00 i 21:00, a preuzeti podaci će biti pohranjeni
u direktoriju `docker_data`. API servis će biti dostupan na `http://localhost:8000`, a
Swagger dokumentacija na `http://localhost:8000/docs`.

### Preuzimanje podataka na zahtjev

Ako želite odmah pokrenuti preuzimanje podataka (bez čekanja na zakazane cron zadatke),
možete ga pokrenuti unutar već pokrenutog `crawler` kontejnera koristeći:

```bash
docker compose exec crawler python -m crawler.cli.crawl /data
docker compose exec crawler python -m crawler.cli.crawl -h
```

Ako želite preuzeti sve povijesne podatke od početka važenja odluke, upotrijebite:

```bash
docker compose exec crawler python -m crawler.cli.fetchhistory /data
```

## Licenca

Ovaj projekt je licenciran pod [AGPL-3 licencom](LICENSE).

Podaci prikupljeni putem ovog projekta su javni i dostupni svima, temeljem
Odluke o objavi cjenika i isticanju dodatne cijene kao mjeri izravne
kontrole cijena u trgovini na malo, NN 75/2025 od 2.5.2025.
