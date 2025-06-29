# Docker Setup

## Pregled

Ovaj projekt koristi Docker za kontejnerizaciju:
- **API**: FastAPI web servis za podatke o cijenama: http://localhost:8000 (dokumentacija na /docs)
- **Crawler**: Prikupljanje podataka iz hrvatskih trgovačkih lanaca
- **Baza podataka**: PostgreSQL 17 za spremanje podataka

### Postavljanje okruženja
```bash
cp .env.docker.example .env
# Uredite .env s vašim postavkama
```

### Produkcija
```bash
docker-compose -f docker-compose.yml up -d
```

### Razvoj
```bash
docker-compose up -d
```

### Pokretanje crawlera
```bash
docker-compose run --rm crawler
```

### Operacije s bazom podataka
```bash
# Uvoz podataka
docker-compose exec api uv run -m service.db.import /app/output/YYYY-MM-DD

# Izračun statistika za određeni datum ili više datuma odjednom
docker-compose exec api uv run -m service.db.stats YYYY-MM-DD YYYY-MM-DD YYYY-MM-DD

# Pristup bazi podataka
docker-compose exec db psql -U cijene_user -d cijene
```

### Logovi i status
```bash
docker-compose ps
docker-compose logs -f api
```

### Održavanje
```bash
# Ažuriranje slika
docker-compose pull
docker-compose up -d --build

# Sigurnosna kopija baze podataka
docker-compose exec db pg_dump -U cijene_user cijene > backup.sql

# Čišćenje
docker-compose down

# Čišćenje sa obrisanim volume od baze podataka
docker-compose down -v
```

## Ključna konfiguracija

Osnovne `.env` varijable:
- `POSTGRES_PASSWORD`: Lozinka baze podataka
- `BASE_URL`: Javni API URL
- `DEBUG`: Postaviti na false za produkciju
- `TIMEZONE`: Europe/Zagreb