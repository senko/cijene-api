# Cijene API

Servis za preuzimanje javnih podataka o cijenama proizvoda u trgovačkim lancima u Republici Hrvatskoj.

Preuzimanje podataka o cijenama proizvoda u trgovačkim lancima u Republici Hrvatskoj
temeljeno je na Odluci o objavi cjenika i isticanju dodatne cijene kao mjeri izravne
kontrole cijena u trgovini na malo.

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

Softver je izgrađen na Pythonu i sastoji se od dva glavna dijela:

*   **Crawler** (`crawler`): Preuzima podatke s web stranica trgovačkih lanaca, sprema ih u CSV datoteke, te ih zatim uvozi u PostgreSQL bazu podataka.
*   **Web servis** (`service`): API koji omogućava pristup podacima o cijenama proizvoda iz baze podataka. (**U IZRADI**)

### Baza podataka

Sustav koristi PostgreSQL bazu podataka za pohranu prikupljenih podataka. Glavne tablice uključuju:

*   `stores`: Informacije o pojedinačnim lokacijama trgovina, uključujući `chain_name` (naziv lanca), `store_id` (interni ID trgovine lanca), `name` (naziv trgovine), `city` (grad, **pohranjen bez dijakritičkih znakova**, npr., "Cakovec"), `address` (adresa), itd.
*   `products`: Detalji o proizvodima kao što su `chain_name`, `product_id` (interni ID proizvoda lanca), `name` (naziv proizvoda), `brand` (marka), `barcode` (barkod), itd.
*   `prices`: Unosi o cijenama koji povezuju trgovine i proizvode, uključujući `price` (cijena), `unit_price` (jedinična cijena), i `crawled_at` (vremenska oznaka preuzimanja).
*   `processed_files`: Prati uvezene CSV datoteke i njihove hash vrijednosti kako bi se spriječila redundantna obrada.

Za ispravan rad, detalji za spajanje na bazu podataka (`DB_HOST`, `DB_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`) moraju biti konfigurirani u `.env` datoteci.

## Instalacija

Za lokalno pokretanje potrebno je imati instaliran Python 3.13 ili noviji. Preporučamo korištenje `uv` za postavljanje projekta:

```bash
git clone https://github.com/senko/cijene-api.git
cd cijene-api
# Kreirajte .env datoteku iz primjera i prilagodite je svojim potrebama
cp .env.example .env 
# (Posebno obratite pažnju na POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB)
uv sync --dev
```

## Korištenje

### Crawler

Crawler preuzima podatke i automatski ih uvozi u bazu podataka.

Za pokretanje crawlera (preuzimanje i uvoz):

```bash
# Provjerite je li .env datoteka ispravno konfigurirana s podacima za bazu
uv run -m crawler.cli.crawl /path/to/output-folder/for-csvs/
```

Ili pomoću Pythona direktno (u adekvatnoj virtualnoj okolini):

```bash
python -m crawler.cli.crawl /path/to/output-folder/for-csvs/
```

Crawler prima opcije:
* `-l` za listanje podržanih trgovačkih lanaca.
* `-d` za odabir datuma (default: trenutni dan).
* `-c` za odabir lanaca (default: svi).
* `-h` za ispis pomoći.

Logovi crawlera bit će zapisani u datoteku `crawler.log` unutar izlaznog direktorija koji ste naveli. Datoteka sadrži detaljne zapise svih faza obrade (razina DEBUG i iznad). CSV datoteke također će biti spremljene u navedeni izlazni direktorij, strukturirane po lancima i datumu.

### Ručni uvoz podataka u bazu (za postojeće CSV datoteke)

Ako imate postojeće CSV podatke (npr. preuzete ranije ili od drugog izvora) koji prate strukturu koju crawler generira, možete ih ručno uvesti pomoću `crawler.cli.db_importer` skripte:

```bash
# Provjerite je li .env datoteka ispravno konfigurirana
python -m crawler.cli.db_importer --data-path /path/to/csv_root_directory/
```

Argument `--data-path` treba pokazivati na korijenski direktorij koji sadrži poddirektorije pojedinih lanaca (npr., `./docker_data/crawler/` ako pristupate podacima koje je Docker spremio, ili specifični direktorij s datumom poput `./docker_data/crawler/2023-10-27/`).

**Napomena o normalizaciji imena gradova**: Imena gradova se u bazu spremaju bez hrvatskih dijakritičkih znakova (npr., 'Varaždin' postaje 'Varazdin').

### Web servis

Prije pokretanja servisa, kreirajte datoteku `.env` sa konfiguracijskim varijablama (ako već niste za crawler). Primjer datoteke sa zadanim (default) vrijednostima može se naći u `.env.example`.

Nakon što ste kreirali `.env` datoteku, pokrenite servis koristeći:

```bash
uv run -m service.main
```

Servis će biti dostupan na `http://localhost:8000` (ako niste mijenjali port), a na `http://localhost:8000/docs` je dostupna Swagger dokumentacija API-ja.

## Docker Compose

Za jednostavno pokretanje kompletnog sustava (Crawler, PostgreSQL baza, API servis) putem Docker Compose:

1.  **Kreirajte direktorije za trajne podatke**:
    ```bash
    mkdir -p docker_data/postgres
    mkdir -p docker_data/crawler
    ```

2.  **Konfiguracija**: Preimenujte i prilagodite datoteku s konfiguracijskim varijablama:
    ```bash
    cp .env.example .env
    ```
    **Važno**: U `.env` datoteci obavezno postavite `POSTGRES_USER`, `POSTGRES_PASSWORD`, i `POSTGRES_DB`. Ove vrijednosti koristi `db` servis za inicijalizaciju baze i `crawler` servis za spajanje na bazu. `DB_HOST` treba ostati `db` kako bi crawler mogao pronaći PostgreSQL kontejner.

3.  **Pokretanje servisa**:
    ```bash
    docker compose up -d
    ```

4.  **Rebuildanje kontejnera** (ako su napravljene promjene u kodu):
    ```bash
    docker compose up -d --build
    ```

**Opis Docker Compose setupa**:

*   **`db` servis**: Pokreće PostgreSQL 15. Podaci baze su trajno pohranjeni na hostu u `./docker_data/postgres/` (mapirano na `/var/lib/postgresql/data` unutar kontejnera).
*   **`crawler` servis**: Pokreće crawler. CSV podaci koje crawler preuzme spremaju se na hostu u `./docker_data/crawler/` (mapirano na `/data` unutar kontejnera). Cron job unutar kontejnera automatski pokreće `crawler.cli.crawl /data` svakodnevno u 09:00 i 21:00. Nakon svakog preuzimanja, podaci se automatski uvoze u `db` servis.
*   **`service` servis**: Pokreće API web servis, koji je dostupan na `http://localhost:8000`. Swagger dokumentacija je na `http://localhost:8000/docs`. Ovaj servis ovisi o `db` servisu.

**Podaci i perzistencija**: Svi relevantni podaci (CSV-ovi crawlera i datoteke baze) mapirani su na host sustav u `docker_data` direktorij, čime se osigurava njihova trajnost neovisno o životnom ciklusu kontejnera.

### Ručno pokretanje operacija s Docker Compose

*   **Preuzimanje podataka na zahtjev (crawl & import)**:
    Ako želite odmah pokrenuti preuzimanje podataka i njihov uvoz u bazu (bez čekanja na cron):
    ```bash
    docker compose exec crawler python -m crawler.cli.crawl /data
    ```
    Za pomoć oko `crawl` skripte:
    ```bash
    docker compose exec crawler python -m crawler.cli.crawl -h
    ```

*   **Ručni uvoz postojećih CSV podataka u bazu**:
    Ako imate CSV podatke unutar Docker volumena (npr. u `/data/YYYY-MM-DD`) i želite ih ručno uvesti:
    ```bash
    docker compose exec crawler python -m crawler.cli.db_importer --data-path /data/YYYY-MM-DD
    ```
    Ili za uvoz svih podataka unutar `/data` (svi datumi, svi lanci):
    ```bash
    docker compose exec crawler python -m crawler.cli.db_importer --data-path /data
    ```

*   **Preuzimanje svih povijesnih podataka od početka važenja odluke (samo CSV)**:
    ```bash
    docker compose exec crawler python -m crawler.cli.fetchhistory /data
    ```
    Nakon ovoga, možete pokrenuti ručni uvoz za `/data` kako bi se svi povijesni CSV-ovi uvezli u bazu.

## Licenca

Ovaj projekt je licenciran pod [AGPL-3 licencom](LICENSE).

Podaci prikupljeni putem ovog projekta su javni i dostupni svima, temeljem
Odluke o objavi cjenika i isticanju dodatne cijene kao mjeri izravne
kontrole cijena u trgovini na malo.
