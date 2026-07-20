"""Mannheim weather scraper.

Polls the MVV Smart Cities dashboard API and writes readings to TimescaleDB.
Station list is auto-discovered from the map endpoint; the three metric UUIDs
are hardcoded (extracted from github.com/rathlinus/smartmannheim).
"""
from __future__ import annotations

import asyncio
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import aiohttp
import asyncpg
import yaml

from backfill import run_backfill

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── API constants ──────────────────────────────────────────────────────────────

API_URL = "https://apps.mvvsmartcities.com/api/dashboarddata"
ACCOUNT_ID = "6233165a7faac33eade2c539"
DASHBOARD_TOKEN = "268b1470-a99b-4244-942e-d8fbdba033ab"
APP_ID = DASHBOARD_TOKEN
MAP_TILE_ID = "3a1e9ee5-9d72-4727-8832-9d46fc8c0395"

METRICS: tuple[dict[str, Any], ...] = (
    {
        "key": "temperature",
        "timeseries_id": "536a8e89-34c6-4a23-8bac-dec7ae840ee0",
        "tile_id": "b56d6160-6cf4-48fa-be5a-51581216d1a2",
        "display_name": "Klimasensor, Temperatur",
        "digits_field": "numDigits",
        "digits": 1,
    },
    {
        "key": "humidity",
        "timeseries_id": "de1bedd9-1b2c-40ea-8434-ca7895362ef3",
        "tile_id": "930d05a5-cefe-4dda-9190-db40cf82abbc",
        "display_name": "Klimasensor, Luftfeuchtigkeit",
        "digits_field": "numDigits",
        "digits": 0,
    },
    {
        "key": "wind_speed",
        "timeseries_id": "af7132bc-38e7-425f-8695-a8a94701a4b6",
        "tile_id": "13c34302-b5e3-433c-8602-aed08d7cf390",
        "display_name": "Durchschn. Windgeschwindigkeit",
        "digits_field": "displayDigits",
        "digits": 1,
    },
)

# ── API client ─────────────────────────────────────────────────────────────────

PARAMS = {"accountId": ACCOUNT_ID, "id": DASHBOARD_TOKEN}
REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=30)
MAX_CONCURRENT = 20


async def list_stations(session: aiohttp.ClientSession) -> list[dict[str, Any]]:
    body = {"appId": APP_ID, "dashboardTemplateTileId": MAP_TILE_ID}
    async with session.post(API_URL, params=PARAMS, json=body) as resp:
        resp.raise_for_status()
        return await resp.json(content_type=None)


async def get_indicator(
    session: aiohttp.ClientSession,
    sem: asyncio.Semaphore,
    location_id: str,
    metric: dict[str, Any],
) -> dict[str, Any] | None:
    now = datetime.now(timezone.utc)
    frm = (now - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    to = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")

    body = {
        "timeseries": [
            {
                "timeSeriesId": metric["timeseries_id"],
                "aggregationFunction": "",
                "gapFill": "None",
                "displayName": metric["display_name"],
                metric["digits_field"]: metric["digits"],
                "definitionType": "timeseries",
            }
        ],
        "from": frm,
        "to": to,
        "accountId": ACCOUNT_ID,
        "orient": "analytics",
        "timezone": "Europe/Berlin",
        "dashboardTemplateTileId": metric["tile_id"],
        "appId": APP_ID,
        "entityId": location_id,
    }

    for attempt in range(3):
        try:
            async with sem:
                async with session.post(
                    API_URL, params=PARAMS, json=body, timeout=REQUEST_TIMEOUT
                ) as resp:
                    if resp.status >= 400:
                        return None
                    data = await resp.json(content_type=None)
            if isinstance(data, list) and data:
                return data[0]
            return None
        except Exception as exc:
            if attempt == 2:
                log.debug("get_indicator failed after 3 attempts: %s", exc)
                return None
            await asyncio.sleep(2 ** attempt)
    return None


# ── DB helpers ─────────────────────────────────────────────────────────────────

INSERT_SQL = """
INSERT INTO measurements (time, location_id, station_name, metric, value, warning)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (time, station_name, metric) DO NOTHING
"""

UPSERT_STATION_SQL = """
INSERT INTO stations (station_name, location_id, display_name, lat, lon, address)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (station_name) DO UPDATE SET
    location_id  = EXCLUDED.location_id,
    display_name = EXCLUDED.display_name,
    lat          = EXCLUDED.lat,
    lon          = EXCLUDED.lon,
    address      = EXCLUDED.address,
    updated_at   = now()
"""

CREATE_STATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS stations (
    station_name    TEXT        PRIMARY KEY,
    location_id     UUID,
    display_name    TEXT,
    lat             DOUBLE PRECISION,
    lon             DOUBLE PRECISION,
    address         TEXT,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
)
"""


async def sync_station_metadata(
    pool: asyncpg.Pool, stations: list[dict[str, Any]]
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(CREATE_STATIONS_TABLE)
        rows = []
        for s in stations:
            name = s.get("name", "")
            loc_id = s.get("locationId")
            coords = s.get("location", {}).get("coordinates", [None, None])
            lon = coords[0] if len(coords) > 1 else None
            lat = coords[1] if len(coords) > 1 else None
            address = s.get("address", "").strip(" ,")
            try:
                loc_uuid = uuid.UUID(loc_id) if loc_id else None
            except ValueError:
                loc_uuid = None
            rows.append((
                name,
                loc_uuid,
                name,
                lat,
                lon,
                address or None,
            ))
        await conn.executemany(UPSERT_STATION_SQL, rows)
    log.info("Synced metadata for %d stations", len(rows))


async def connect_db() -> asyncpg.Pool:
    dsn = (
        f"postgresql://{os.environ['DB_USER']}:{os.environ['DB_PASSWORD']}"
        f"@{os.environ['DB_HOST']}:{os.environ.get('DB_PORT', '5432')}"
        f"/{os.environ['DB_NAME']}"
    )
    for attempt in range(20):
        try:
            pool = await asyncpg.create_pool(dsn, min_size=2, max_size=10)
            log.info("Connected to database")
            return pool
        except Exception as exc:
            log.info("Waiting for database (%d/20): %s", attempt + 1, exc)
            await asyncio.sleep(3)
    raise RuntimeError("Could not connect to database after 20 attempts")


# ── Config ─────────────────────────────────────────────────────────────────────

def load_config() -> dict[str, Any]:
    path = os.environ.get("CONFIG_PATH", "/app/config.yaml")
    with open(path) as f:
        return yaml.safe_load(f)


def filter_stations(
    stations: list[dict[str, Any]], targets: Any
) -> list[dict[str, Any]]:
    if targets == "all":
        return stations
    patterns = [str(t).lower() for t in targets]
    return [
        s for s in stations
        if any(p in s.get("name", "").lower() for p in patterns)
    ]


# ── Scrape cycle ───────────────────────────────────────────────────────────────

async def scrape_once(
    session: aiohttp.ClientSession,
    pool: asyncpg.Pool,
    stations: list[dict[str, Any]],
) -> None:
    sem = asyncio.Semaphore(MAX_CONCURRENT)

    tasks = [
        (station, metric)
        for station in stations
        for metric in METRICS
    ]

    log.info("Scraping %d station×metric combinations ...", len(tasks))

    async def fetch_and_store(station: dict, metric: dict) -> bool:
        location_id = station["locationId"]
        station_name = station.get("name", location_id)
        result = await get_indicator(session, sem, location_id, metric)
        if result is None:
            return False
        ts_str = result.get("timestamp")
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            return False
        value = result.get("indicator")
        warning = result.get("warning")

        async with pool.acquire() as conn:
            await conn.execute(
                INSERT_SQL,
                ts,
                uuid.UUID(location_id),
                station_name,
                metric["key"],
                float(value) if value is not None else None,
                str(warning) if warning is not None else None,
            )
        return True

    results = await asyncio.gather(
        *[fetch_and_store(s, m) for s, m in tasks],
        return_exceptions=True,
    )

    ok = sum(1 for r in results if r is True)
    err = sum(1 for r in results if r is not True)
    log.info("Done: %d inserted/upserted, %d skipped/failed", ok, err)


# ── Main loop ──────────────────────────────────────────────────────────────────

async def main() -> None:
    config = load_config()
    interval = int(config.get("scrape_interval", 600))
    targets = config.get("targets", "all")

    pool = await connect_db()

    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(connector=connector) as session:
        log.info("Fetching station list ...")
        stations = await list_stations(session)
        log.info("Discovered %d stations total", len(stations))

        await sync_station_metadata(pool, stations)

        await run_backfill(pool, int(config.get("backfill_days", 30)))

        stations = filter_stations(stations, targets)
        if targets == "all":
            log.info("Scraping all %d stations", len(stations))
        else:
            log.info("Filtered to %d stations matching %s", len(stations), targets)

        while True:
            start = asyncio.get_event_loop().time()
            try:
                await scrape_once(session, pool, stations)
            except Exception as exc:
                log.error("Scrape cycle failed: %s", exc)
            elapsed = asyncio.get_event_loop().time() - start
            sleep_for = max(0, interval - elapsed)
            log.info("Next scrape in %.0f seconds", sleep_for)
            await asyncio.sleep(sleep_for)


if __name__ == "__main__":
    asyncio.run(main())
