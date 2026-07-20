# QuadratWetter

A self-hosted weather dashboard for Mannheim's public climate sensor network ([MA sMArt Klimamessnetz](https://opendata.smartmannheim.de/dataset/klimadaten-mannheim)). Scrapes ~414 stations every 10 minutes, stores readings in TimescaleDB, and visualises them in Grafana.

## Dashboards

| Dashboard | Description |
|---|---|
| **Mannheim Wetter** | Live map coloured by current temperature, network stat cards, time-series history with average/median overlay |
| **Outliers** | Stations deviating more than Nσ from the network mean, with map and deviation history |
| **Debug** | Raw readings table filterable by station and metric |

## Quick start

```bash
git clone <repo>
cd mannheim-wetter

# Optional: change passwords in .env before first run
cat .env

docker compose up -d
docker compose logs -f scraper   # watch backfill + first scrape
```

Open Grafana at **http://localhost:3000** (default credentials: `admin` / `mannheimwetter`).

## How it works

### Scraper

On startup the scraper:

1. Fetches the full station list from the MVV Smart Cities dashboard API (~414 stations)
2. Writes station metadata (name, coordinates) to the `stations` table
3. Runs the **historical backfill** (see below)
4. Enters a polling loop, fetching temperature, humidity and wind speed for every station every `scrape_interval` seconds

### Historical backfill

The [opendata.smartmannheim.de](https://opendata.smartmannheim.de/dataset/klimadaten-mannheim) portal publishes one zip file per day containing CSV files for every sensor. On first startup the scraper downloads and imports the last `backfill_days` days of history (default: 7). Progress is tracked in a `backfill_log` table so restarts never re-import already-present dates.

Sensor types in the historical data:

| Suffix | Sensor type | Metrics |
|---|---|---|
| `-21` | Climate sensor | temperature, humidity, irradiation |
| `-31` | Wind sensor | wind speed, wind direction |
| `-11` | DWD / BBS station | extended meteorological set |
| `-41` | Soil sensor | moisture, conductivity, temperature |
| `-51` | Surface temperature | head / target temperature |

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
│   ├── config.yaml                   scrape targets & interval
│   ├── Dockerfile
│   └── requirements.txt
├── timescaledb/
│   └── init/01_schema.sql            hypertable, indexes, compression
└── grafana/
    └── provisioning/
        ├── datasources/              auto-provisioned TimescaleDB connection
        └── dashboards/               three pre-built dashboards
```

## Database schema

```sql
-- One row per reading (narrow table)
measurements (time, location_id, station_name, metric, value, warning)

-- Station metadata from the live API
stations (station_name, location_id, lat, lon, address, display_name)

-- Tracks imported historical zip dates
backfill_log (date, imported_at, rows_inserted)
```

Compression is applied automatically to chunks older than 7 days.

## Resetting

```bash
docker compose down -v   # removes all data volumes
docker compose up -d     # starts fresh, backfill runs again
```

