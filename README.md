# QuadratWetter

A self-hosted weather dashboard for Mannheim's public climate sensor network ([MA sMArt Klimamessnetz](https://opendata.smartmannheim.de/dataset/klimadaten-mannheim)). Scrapes ~414 stations every 10 minutes, stores readings in TimescaleDB, and visualises them in Grafana.

## Dashboards

| Dashboard | Description |
|---|---|
| **QuadratWetter** | Live map coloured by current temperature, network stat cards, time-series history with average/median overlay |
| **Outliers** | Stations deviating more than Nσ from the network mean, with map and deviation history |

## Quick start

```bash
git clone <repo>
cd QuadratWetter

# Optional: change passwords in .env before first run
cp example.env .env
cat .env

docker compose up -d
docker compose logs -f scraper   # watch backfill + first scrape
```

Open Grafana at **http://localhost:3000** (default credentials: `admin` / `QuadratWetter`).

## How it works

### Scraper

On startup the scraper:

1. Downloads the Klimamessnetz Excel catalog (cached 24 h) and matches stations to sensor metadata
2. Fetches the full station list from the MVV Smart Cities dashboard API (~414 stations)
3. Upserts station metadata (display name, coordinates, sensor flags) into the `stations` table
4. Runs the **historical backfill** (see below)
5. Enters a polling loop, fetching only the metrics each station actually has (temperature, humidity, wind speed) every `scrape_interval` seconds

### Historical backfill

The [opendata.smartmannheim.de](https://opendata.smartmannheim.de/dataset/klimadaten-mannheim) portal publishes one zip file per day, each containing one CSV per sensor. On first startup the scraper downloads and imports the last `backfill_days` days of history (default: 7). Progress is tracked in a `backfill_log` table so restarts never re-import already-present dates.

#### Backfill limitations

- **Only matched stations are imported.** A CSV file is only imported if its sensor code (e.g. `0101-001-21`) can be resolved to a `location_id` via the live API station list. Sensors not listed by the API are silently skipped.
- **~99 of 414 stations** are matched by name (the sensor code appears in the API station name). A further ~2 are matched by GPS coordinate + sensor type at 4 decimal-place precision. The remaining ~313 stations have no Excel entry and are scraped with all three metrics as a best-effort fallback, but their historical CSV data cannot be backfilled.
- **Historical data starts 2024-02-20.** Any `backfill_days` value larger than the available history is automatically capped.
- **The backfill only runs on an empty database.** If the DB already contains data, the backfill log is consulted to determine which dates are missing and only those are imported.
- **Historical CSVs contain richer metrics** (temperature min/max, irradiation, wind direction, precipitation) that the live API does not expose. These are stored as additional metric types and visible in the history panels.

Sensor types in the historical data:

| Suffix | Sensor type | Metrics |
|---|---|---|
| `-21` | Climate sensor | temperature, humidity, irradiation, temperature min/max |
| `-31` | Wind sensor | wind speed, wind direction |
| `-11` | DWD / BBS station | extended meteorological set |
| `-41` | Soil sensor | moisture, conductivity, temperature |
| `-51` | Surface temperature | head / target temperature |

### Station catalog matching

Sensor metadata (human-readable name, sensor capability flags) is sourced from the [Metadatenkatalog Excel](https://opendata.smartmannheim.de/dataset/23c48b6a-42b6-44f1-8756-5e83340a6a0a) downloaded at startup and refreshed every 24 hours. Matching uses two strategies in order:

1. **Name-code match** — the sensor code in the API station name (e.g. `0101-001-21`) is looked up directly in the Excel catalog
2. **Coordinate + type fallback** — GPS coordinates truncated to 4 decimal places (~11 m grid) combined with the sensor type suffix, for stations whose names don't contain a recognisable code

Unmatched stations receive default flags (all three metrics scraped) and use the API address as the display name.

### Data sources

All data is **public and unauthenticated**. The API tokens are the same ones used by the public [smartmannheim.de](https://www.smartmannheim.de) dashboard, as documented by the [smartmannheim Home Assistant integration](https://github.com/rathlinus/smartmannheim).

## Configuration

Edit `scraper/config.yaml`:

```yaml
# Scrape interval in seconds (sensors update every 10 minutes)
scrape_interval: 600

# Days of history to import on first startup.
# Capped automatically to the oldest available data (2024-02-20).
# Set to 0 to skip the backfill entirely.
backfill_days: 7

# Which stations to scrape.
# "all"  → all ~414 stations
# [list] → only stations whose name contains one of these strings
targets: all
# targets:
#   - "0101-053"
#   - "0301-001"
```

Passwords are set in `.env`:

```
DB_PASSWORD=mannheimwetter
GRAFANA_PASSWORD=mannheimwetter
```

## Project structure

```
mannheim-wetter/
├── docker-compose.yml
├── .env                              passwords (gitignored)
├── scraper/
│   ├── scraper.py                    live polling loop
│   ├── backfill.py                   historical zip importer
│   ├── catalog.py                    Excel catalog loader + station matcher
│   ├── config.yaml                   scrape targets & interval
│   ├── Dockerfile
│   └── requirements.txt
├── timescaledb/
│   └── init/01_schema.sql            hypertable, indexes, compression
└── grafana/
    └── provisioning/
        ├── datasources/              auto-provisioned TimescaleDB connection
        └── dashboards/               two pre-built dashboards
```

## Database schema

```sql
-- One row per reading (narrow table)
measurements (time, location_id, metric, value, warning)

-- Station metadata from live API + Excel catalog
stations (location_id, name, display_name, lat, lon, has_temp, has_humidity, has_wind, xlsx_matched)

-- Tracks imported historical zip dates
backfill_log (date, imported_at, rows_inserted)
```

Compression is applied automatically to chunks older than 7 days.

## Resetting

```bash
docker compose down -v   # removes all data volumes
docker compose up -d     # starts fresh, backfill runs again
```

