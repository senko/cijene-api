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
* Web servis - API koji omogućava pristup podacima o cijenama proizvoda (`service`)

## Instalacija

Za instalaciju crawlera potrebno je imati instaliran Python 3.13 ili noviji. Preporučamo korištenje `uv` za setup projekta:

```bash
git clone https://github.com/senko/cijene-api.git
cd cijene-api
uv sync --dev
```

### Docker (preporučeno)

Projekt uključuje potpunu Docker konfiguraciju za lakše pokretanje i deployment. Docker setup omogućava:

* Containeriziran API servis s PostgreSQL bazom podataka
* Automatsko pokretanje crawler servisa
* Razvojno okruženje s hot reload funkcionalnosti
* Automatizirano cron pokretanje crawler-a

Za detaljne Docker instrukcije, konfiguraciju i sve opcije pokretanja, pogledajte [DOCKER.md](DOCKER.md).

**Brza instalacija s Docker-om:**

```bash
git clone https://github.com/senko/cijene-api.git
cd cijene-api
cp .env.docker.example .env
# Uredite .env prema potrebi
docker-compose up -d
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

Web servis koristi PostgreSQL bazu podataka za pohranu podataka o cijenama.

#### Konfiguracija okruženja

Projekt koristi različite primjere environment datoteka za različite načine pokretanja:

- **`.env.example`** - za lokalni razvoj (bez Docker-a)
- **`.env.docker.example`** - za Docker deployment

Za lokalni razvoj, kreirajte datoteku `.env` na osnovu `.env.example`:

```bash
cp .env.example .env
# Uredite .env prema potrebi za lokalni razvoj
```

Prije pokretanja servisa, kreirajte datoteku `.env` sa konfiguracijskim varijablama.
Primjer datoteke sa zadanim (default) vrijednostima za lokalni razvoj može se naći u `.env.example`.

Nakon što ste kreirali `.env` datoteku, pokrenite servis koristeći:

```bash
uv run -m service.main
```

Servis će biti dostupan na `http://localhost:8000` (ako niste mijenjali port), a na
`http://localhost:8000/docs` je dostupna Swagger dokumentacija API-ja.

#### Uvoz podataka

Servis drži podatke u PostgreSQL bazi podataka. Za uvoz podataka iz CSV
datoteka koje kreira crawler, možete koristiti sljedeću komandu:

```bash
uv run -m service.db.import /path/to/csv-folder/
```

CSV folder treba biti imenovan u `YYYY-MM-DD` formatu, gdje `YYYY-MM-DD`
predstavlja datum za koji se podaci uvoze, i sadržavati CSV datoteke u
istom formatu kakve generira crawler (*ne* CSV datoteke skinute sa stranica
nekog trgovačkog lanca!).

##### Opcije za uvoz

Moguće je preskočiti računanje statistika tijekom uvoza (što može značajno ubrzati proces):

```bash
uv run -m service.db.import -s /path/to/csv-folder/
# ili
uv run -m service.db.import --skip-stats /path/to/csv-folder/
```

Za debug informacije koristite `-d` opciju:

```bash
uv run -m service.db.import -d /path/to/csv-folder/
```

#### Računanje statistika

Za računanje statistika bez uvoza podataka (korisno za već uvezene podatke), koristite dedicirani stats servis:

```bash
# Računanje statistika za jedan datum
uv run -m service.db.stats 2024-01-15

# Više datuma odjednom
uv run -m service.db.stats 2024-01-15 2024-01-16 2024-01-17

# S debug informacijama
uv run -m service.db.stats -d 2024-01-15
```

Stats servis može se koristiti za:
- **Ponovno računanje statistika** nakon promjena u bazi podataka
- **Batch procesiranje** statistika za više datuma
- **Nezavisan rad** od import procesa za bolje performanse

## Dodatni podaci o proizvodima

Dodatni pročišćeni podaci o proizvodima (naziv, marka, količina, jedinica mjere)
za najčeših ~30 tisuća proizvoda dostupni su u `enrichment/products.csv` datoteci
a mogu se uvesti u bazu koristeći sljedeću komandu:

```bash
uv run -m service.db.enrich enrichment/products.csv
```

#### Kreiranje korisnika

Neki API endpointovi zahtijevaju autentifikaciju. Korisnike možete kreirati
direktno u bazi podataka koristeći SQL, npr:

```sql
INSERT INTO users (name, api_key, is_active) VALUES ('Senko', 'secret-key', TRUE);
```

## Licenca

Ovaj projekt je licenciran pod [AGPL-3 licencom](LICENSE).

Podaci prikupljeni putem ovog projekta su javni i dostupni svima, temeljem
Odluke o objavi cjenika i isticanju dodatne cijene kao mjeri izravne
kontrole cijena u trgovini na malo, NN 75/2025 od 2.5.2025.

Pročišćeni CSV podaci o proizvodima
([`enrichment/products.csv`](enrichment/products.csv))
dostupni su pod [CC BY-NC-SA licencom](https://creativecommons.org/licenses/by-nc-sa/4.0/).
