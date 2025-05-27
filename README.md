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

Logovi crawlera bit će zapisani u datoteku `crawler.log` unutar izlaznog direktorija.
Datoteka sadrži detaljne zapise svih faza obrade (razina DEBUG i iznad).

### Web servis

Prije pokretanja servisa, kreirajte datoteku `.env` sa konfiguracijskim varijablama.
Primjer datoteke sa zadanim (default) vrijednostima može se naći u `.env.example`.

Nakon što ste kreirali `.env` datoteku, pokrenite servis koristeći:

```bash
uv run -m service.main
```

Servis će biti dostupan na `http://localhost:8000` (ako niste mijenjali port), a na
`http://localhost:8000/docs` je dostupna Swagger dokumentacija API-ja.

## Docker Compose

**Opis Docker Compose setupa**:

*   **`db` servis**: Pokreće PostgreSQL 15. Podaci baze su trajno pohranjeni na hostu u `./docker_data/postgres/` (mapirano na `/var/lib/postgresql/data` unutar kontejnera).
*   **`crawler` servis**: Pokreće crawler. CSV podaci koje crawler preuzme spremaju se na hostu u `./docker_data/crawler/` (mapirano na `/data` unutar kontejnera). Cron job unutar kontejnera automatski pokreće `crawler.cli.crawl /data` svakodnevno u 09:00 i 21:00. Nakon svakog preuzimanja, podaci se automatski uvoze u `db` servis.
*   **`service` servis**: Pokreće API web servis, koji je dostupan na `http://localhost:8000`. Swagger dokumentacija je na `http://localhost:8000/docs`. Ovaj servis ovisi o `db` servisu.

**Podaci i perzistencija**: Svi relevantni podaci (CSV-ovi crawlera i datoteke baze) mapirani su na host sustav u `docker_data` direktorij, čime se osigurava njihova trajnost neovisno o životnom ciklusu kontejnera.


Za pokretanje servisa i crawlera putem Docker Compose:

# Kreiraj direktorij za trajne podatke

```bash
mkdir -p docker_data/postgres
mkdir -p docker_data/crawler
```


# Preimenuj i prilagodi datoteku s konfiguracijskim varijablama (ako je potrebno)
```bash
cp .env.example .env
```

# Pokreni Docker Compose
```bash
docker compose up -d
```

# Rebuildanje kontejnera ako su napravljene promjene u kodu:

```bash
docker compose down
docker compose up -d --build
```

Crawler će se izvršavati svakodnevno u 09:00 i 21:00, a preuzeti podaci će biti pohranjeni
u direktoriju `docker_data`. API servis će biti dostupan na `http://localhost:8000`, a
Swagger dokumentacija na `http://localhost:8000/docs`.


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

*   **Preuzimanje svih povijesnih podataka od početka važenja odluke (samo CSV)**:
    ```bash
    docker compose exec crawler python -m crawler.cli.fetchhistory /data
    ```
    Nakon ovoga, možete pokrenuti ručni uvoz za `/data` kako bi se svi povijesni CSV-ovi uvezli u bazu.

*   **Kreiranje sheme baze podataka**:
    ```bash
    docker compose exec crawler python -m crawler.cli.preparedb
    ```
    Kreira sve tablice, indekse i ostale strukture u bazi prema definicijama u crawler.db.model.


```bash
docker compose exec -it db psql -U $POSTGRES_USER -d $POSTGRES_DB
```
```
cijene_db=# \dt
               List of relations
 Schema |      Name      | Type  |    Owner    
--------+----------------+-------+-------------
 public | chains         | table | cijene_user
 public | product_prices | table | cijene_user
 public | products       | table | cijene_user
 public | store_products | table | cijene_user
 public | stores         | table | cijene_user
(5 rows)
```
```bash
python -m crawler.cli.crawl -c konzum -s tmp/
python -m crawler.cli.crawl -c ktc -s tmp/
python -m crawler.cli.crawl -c ktc -s -d 2025-05-26 tmp/
docker compose exec -T db psql -U $POSTGRES_USER -d $POSTGRES_DB -c \
"SELECT \
  (SELECT COUNT(*) FROM chains) AS chains, \
  (SELECT COUNT(*) FROM products) AS products, \
  (SELECT COUNT(*) FROM stores) AS stores, \
  (SELECT COUNT(*) FROM store_products) AS store_products, \
  (SELECT COUNT(*) FROM product_prices) AS product_prices, \
  (SELECT COUNT(DISTINCT valid_date) FROM product_prices) AS product_price_dates;"

```

docker:
```bash
docker compose exec -T crawler python -m crawler.cli.crawl -c ktc -s -d 2025-05-26 /data
```

```bash
docker compose exec -T db psql -U $POSTGRES_USER -d $POSTGRES_DB -c "SELECT COUNT(*) AS price_changes
FROM (                                      
  SELECT                                        
    pp.store_product_id
  FROM product_prices pp
  WHERE pp.valid_date IN ('2025-05-26', '2025-05-27')
  GROUP BY pp.store_product_id
  HAVING COUNT(DISTINCT pp.price) > 1
) AS changed;"
```

```bash
docker compose exec -T db psql -U $POSTGRES_USER -d $POSTGRES_DB -c "SELECT
  c.name AS chain_name,
  s.ext_name AS store_name,
  p.barcode,
  p.ext_name,
  p.ext_brand,
  p.ext_category,
  p.ext_unit,
  p.ext_quantity,
  sp.ext_product_id AS store_product_id,
  MAX(CASE WHEN pp.valid_date = '2025-05-26' THEN pp.price END) AS price_26,
  MAX(CASE WHEN pp.valid_date = '2025-05-27' THEN pp.price END) AS price_27
FROM product_prices pp
JOIN store_products sp ON pp.store_product_id = sp.id
JOIN products p ON sp.barcode = p.barcode
JOIN stores s ON sp.store_id = s.id
JOIN chains c ON s.chain_id = c.id
WHERE pp.valid_date IN ('2025-05-26', '2025-05-27')
GROUP BY
  c.name, s.ext_name,
  p.barcode, p.ext_name, p.ext_brand, p.ext_category, p.ext_unit, p.ext_quantity,
  sp.ext_product_id
HAVING
  COUNT(DISTINCT pp.valid_date) = 2 AND
  COUNT(DISTINCT pp.price) > 1
ORDER BY c.name, s.ext_name, p.ext_name;
"
```

```bash
docker compose exec -T db psql -U $POSTGRES_USER -d $POSTGRES_DB -c "SELECT 
  products.ext_name as product_name, 
  stores.ext_name as store_name,
  stores.ext_street_address as street_address, 
  stores.ext_city as city, 
  product_prices.price
FROM product_prices
JOIN store_products ON product_prices.store_product_id = store_products.id
JOIN products ON store_products.barcode = products.barcode
JOIN stores ON stores.id = store_products.store_id
ORDER BY product_prices.price DESC
LIMIT 100;
"
```

```bash
docker compose exec -T db psql -U $POSTGRES_USER -d $POSTGRES_DB -c "
SELECT COUNT(DISTINCT sp.ext_product_id) AS distinct_ext_product_count
FROM store_products sp
JOIN stores s ON sp.store_id = s.id
JOIN chains c ON s.chain_id = c.id
WHERE c.name = 'lidl';
"
```


## Licenca

Ovaj projekt je licenciran pod [AGPL-3 licencom](LICENSE).

Podaci prikupljeni putem ovog projekta su javni i dostupni svima, temeljem
Odluke o objavi cjenika i isticanju dodatne cijene kao mjeri izravne
kontrole cijena u trgovini na malo, NN 75/2025 od 2.5.2025.
