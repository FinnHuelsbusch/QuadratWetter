"""Historical backfill from opendata.smartmannheim.de.

Downloads daily zip files up to `backfill_days` days back, unpivots each CSV
into the narrow measurements table. Tracks imported dates in backfill_log so
re-runs are safe and always fill any missing days within the configured window.
"""
from __future__ import annotations

import asyncio
import csv
import io
import logging
import re
import zipfile
from datetime import date, datetime, timedelta, timezone
from typing import Any

import aiohttp
import asyncpg

log = logging.getLogger(__name__)

CATALOG_URL = "https://opendata.smartmannheim.de/dataset/klimadaten-mannheim"
SENTINEL = -999.0

COLUMN_METRICS: dict[str, str] = {
    "temperature":              "temperature",
    "minTemperature":           "temperature_min",
    "maxTemperature":           "temperature_max",
    "airHumidity":              "humidity",
    "irradiation":              "irradiation",
    "minWindSpeed":             "wind_speed",
    "averageWindDirection":     "wind_direction",
    "t2m_med":                  "temperature",
    "t2m_min":                  "temperature_min",
    "t2m_max":                  "temperature_max",
    "rf_med":                   "humidity",
    "rf_min":                   "humidity_min",
    "rf_max":                   "humidity_max",
    "wg_med":                   "wind_speed",
    "wg_min":                   "wind_speed_min",
    "wg_max":                   "wind_speed_max",
    "wr_med":                   "wind_direction",
    "gs_med":                   "irradiation",
    "nied_med":                 "precipitation",
    "sd_med":                   "sunshine_duration",
    "soilTemperature":          "soil_temperature",
    "volumetricWaterContent":   "soil_moisture",
    "dielectricPermittivity":   "soil_dielectric",
    "electricalConductivity":   "soil_conductivity",
    "headTemperature":          "surface_temperature_head",
    "targetTemperature":        "surface_temperature_target",
}

INSERT_SQL = """
INSERT INTO measurements (time, location_id, station_name, metric, value, warning)
VALUES ($1, NULL, $2, $3, $4, NULL)
ON CONFLICT (time, station_name, metric) DO NOTHING
"""

CREATE_LOG_TABLE = """
CREATE TABLE IF NOT EXISTS backfill_log (
    date            DATE        PRIMARY KEY,
    imported_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    rows_inserted   INTEGER     NOT NULL
)
"""

LOG_INSERT = """
INSERT INTO backfill_log (date, rows_inserted)
VALUES ($1, $2)
ON CONFLICT (date) DO NOTHING
"""


async def fetch_zip_urls(session: aiohttp.ClientSession) -> list[tuple[str, str]]:
    """Return sorted list of (date_str, url) for all available zips."""
    async with session.get(CATALOG_URL) as resp:
        resp.raise_for_status()
        html = await resp.text()
    urls = re.findall(
        r'href="(https://opendata\.smartmannheim\.de[^"]*\.zip)"', html
    )
    result = []
    for url in set(urls):
        m = re.search(r'download/(\d{4}-\d{2}-\d{2})\.zip', url)
        if m:
            result.append((m.group(1), url))
    return sorted(result)


def parse_zip(data: bytes) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        for name in zf.namelist():
            station_name = name.removesuffix(".csv")
            with zf.open(name) as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8"))
                for row in reader:
                    ts_str = row.get("timestamps", "").strip()
                    if not ts_str:
                        continue
                    try:
                        ts = datetime.fromisoformat(
                            ts_str.replace("Z", "+00:00")
                        ).astimezone(timezone.utc)
                    except ValueError:
                        continue
                    for col, metric in COLUMN_METRICS.items():
                        raw = row.get(col)
                        if raw is None:
                            continue
                        try:
                            val = float(raw)
                        except ValueError:
                            continue
                        if val == SENTINEL:
                            continue
                        rows.append((ts, station_name, metric, val))
    return rows


async def import_zip(
    session: aiohttp.ClientSession,
    pool: asyncpg.Pool,
    date_str: str,
    url: str,
    name_map: dict[str, str] | None = None,
) -> int:
    """Download, parse, and insert one zip. Returns number of rows inserted."""
    for attempt in range(3):
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=60)) as resp:
                if resp.status == 404:
                    return 0
                resp.raise_for_status()
                data = await resp.read()
            break
        except Exception as exc:
            if attempt == 2:
                log.warning("Failed to fetch %s: %s", url, exc)
                return 0
            await asyncio.sleep(2 ** attempt)

    try:
        rows = parse_zip(data)
    except Exception as exc:
        log.warning("Failed to parse %s: %s", url, exc)
        return 0

    if not rows:
        return 0

    # Normalise CSV sensor codes to canonical API station names
    if name_map:
        rows = [
            (ts, name_map.get(station_name, station_name), metric, val)
            for ts, station_name, metric, val in rows
        ]

    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.executemany(INSERT_SQL, rows)
            await conn.execute(LOG_INSERT, date.fromisoformat(date_str), len(rows))

    return len(rows)


async def _load_name_map(pool: asyncpg.Pool) -> dict[str, str]:
    """Build a mapping from CSV sensor code → canonical API station name.

    E.g. '0101-053-21' → '0101-053-21 | 0101-053-31'
    Uses a substring match identical to the post-hoc UPDATE we ran on the DB.
    Returns empty dict if the stations table doesn't exist yet.
    """
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT station_name FROM stations WHERE station_name IS NOT NULL
        """)
    api_names = [r["station_name"] for r in rows]
    name_map: dict[str, str] = {}
    for api_name in api_names:
        parts = [p.strip() for p in api_name.split("|")]
        for part in parts:
            # Only map simple sensor codes (no spaces), ignore archive suffixes
            code = part.split()[0] if part.split() else part
            if code and code not in name_map:
                name_map[code] = api_name
    return name_map


async def run_backfill(pool: asyncpg.Pool, backfill_days: int) -> None:
    if backfill_days <= 0:
        log.info("backfill_days=0 — skipping historical backfill")
        return

    async with pool.acquire() as conn:
        await conn.execute(CREATE_LOG_TABLE)
        already_done: set[str] = {
            r["date"].isoformat()
            for r in await conn.fetch("SELECT date FROM backfill_log")
        }

    # Load station name normalisation map (CSV code → API canonical name)
    name_map = await _load_name_map(pool)
    if name_map:
        log.info("Loaded station name map: %d CSV codes → API names", len(name_map))

    connector = aiohttp.TCPConnector(limit=4)
    async with aiohttp.ClientSession(connector=connector) as session:
        log.info("Fetching zip catalog ...")
        all_entries = await fetch_zip_urls(session)

    if not all_entries:
        log.warning("No zip files found on catalog page")
        return

    # Cap backfill_days to however many zips actually exist
    cutoff = datetime.now(timezone.utc) - timedelta(days=backfill_days)
    available = [(d, u) for d, u in all_entries if d >= cutoff.date().isoformat()]

    # Safety: if backfill_days exceeds full history, just use everything
    if not available:
        available = all_entries

    pending = [(d, u) for d, u in available if d not in already_done]

    if not pending:
        log.info(
            "All %d dates in the configured window already imported — nothing to do",
            len(available),
        )
        return

    log.info(
        "Backfill: %d/%d dates already done, importing %d remaining (oldest: %s, newest: %s)",
        len(already_done & {d for d, _ in available}),
        len(available),
        len(pending),
        pending[0][0],
        pending[-1][0],
    )

    connector = aiohttp.TCPConnector(limit=4)
    async with aiohttp.ClientSession(connector=connector) as session:
        total_rows = 0
        for i, (date_str, url) in enumerate(pending, 1):
            inserted = await import_zip(session, pool, date_str, url, name_map)
            total_rows += inserted
            if i % 10 == 0 or i == len(pending):
                log.info("Backfill progress: %d/%d zips, %d rows inserted", i, len(pending), total_rows)

    log.info("Backfill complete: %d rows inserted across %d dates", total_rows, len(pending))
