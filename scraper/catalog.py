"""Excel catalog loader.

Downloads the Mannheim Klimamessnetz metadata Excel, caches it for 24 hours,
and builds two lookup structures used by the scraper and backfill:

  csv_code_map : csv_code  -> CsvEntry (sensor flags, stationsname)
  location_map : location_id (UUID str) -> MatchedStation (display_name, flags)

Matching strategy (in order):
  1. Name-code match: split API station name on '|', strip archive suffixes,
     check each segment against known csv_codes.
  2. 4dp-truncate + sensor-type coordinate fallback for unmatched locations.
"""
from __future__ import annotations

import csv
import io
import logging
import os
import re
import time
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from typing import Any

import aiohttp

log = logging.getLogger(__name__)

XLSX_URL = (
    "https://opendata.smartmannheim.de/dataset/"
    "23c48b6a-42b6-44f1-8756-5e83340a6a0a/resource/"
    "6ecbb67f-6564-4a46-9c26-a99e6ad5a0c2/download/"
    "metadatenkatalog_ma_klimamessnetz.xlsx"
)
CACHE_TTL = 86400  # 24 hours

# Sensor suffix → metric keys present in that sensor type
SUFFIX_METRICS: dict[str, set[str]] = {
    "21": {"temperature", "humidity", "irradiation"},
    "31": {"wind_speed", "wind_direction"},
    "11": {"temperature", "humidity", "wind_speed"},
    "41": set(),
    "51": set(),
}


@dataclass
class CsvEntry:
    csv_code: str        # e.g. "0101-001-21"
    stationsname: str    # e.g. "T-016"
    lat: float
    lon: float
    sensor_type: str     # last segment, e.g. "21"
    has_temp: bool
    has_humidity: bool
    has_wind: bool


@dataclass
class MatchedStation:
    display_name: str
    has_temp: bool
    has_humidity: bool
    has_wind: bool
    xlsx_matched: bool


# Module-level cache: (data_bytes, fetch_timestamp)
_xlsx_cache: tuple[bytes, float] | None = None


async def _fetch_xlsx(session: aiohttp.ClientSession) -> bytes:
    global _xlsx_cache
    now = time.monotonic()
    if _xlsx_cache and (now - _xlsx_cache[1]) < CACHE_TTL:
        return _xlsx_cache[0]
    log.info("Downloading Klimamessnetz Excel catalog ...")
    async with session.get(XLSX_URL, timeout=aiohttp.ClientTimeout(total=60)) as resp:
        resp.raise_for_status()
        data = await resp.read()
    _xlsx_cache = (data, now)
    log.info("Excel downloaded (%d KB)", len(data) // 1024)
    return data


def _parse_xlsx(data: bytes) -> list[CsvEntry]:
    """Parse the Excel into a list of CsvEntry objects."""
    ns = {"ns": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        with z.open("xl/sharedStrings.xml") as f:
            ss_root = ET.parse(f).getroot()
        strings = [
            "".join(p.text or "" for p in si.findall(".//ns:t", ns))
            for si in ss_root.findall("ns:si", ns)
        ]
        with z.open("xl/worksheets/sheet1.xml") as f:
            ws_root = ET.parse(f).getroot()
        rows = ws_root.findall(".//ns:row", ns)

    def cell(c: Any) -> str:
        t = c.get("t")
        v = c.find("ns:v", ns)
        if v is None:
            return ""
        return strings[int(v.text)] if t == "s" else (v.text or "")

    entries: list[CsvEntry] = []
    for row in rows[2:]:  # skip header rows
        vals = [cell(c) for c in row.findall("ns:c", ns)]
        while len(vals) < 87:
            vals.append("")
        code_string = vals[85].strip()
        if not code_string:
            continue
        parts = code_string.split("-")
        if len(parts) < 3:
            continue
        csv_code = "-".join(parts[:3])
        sensor_type = parts[2]
        stationsname = vals[2].strip()
        try:
            lat = int(vals[5].strip()) / 1e6
            lon = int(vals[6].strip()) / 1e6
        except (ValueError, IndexError):
            continue

        # Sensor flags: col 13=TT, 16=RF, 20=FF (0=absent, 1=present, 2=derived)
        def flag(idx: int) -> bool:
            try:
                return vals[idx].strip() not in ("0", "9", "")
            except IndexError:
                return False

        entries.append(CsvEntry(
            csv_code=csv_code,
            stationsname=stationsname,
            lat=lat,
            lon=lon,
            sensor_type=sensor_type,
            has_temp=flag(13),
            has_humidity=flag(16),
            has_wind=flag(20),
        ))

    log.info("Parsed %d entries from Excel", len(entries))
    return entries


def _infer_sensor_types(api_name: str) -> set[str]:
    """Infer which sensor type suffixes are present at an API location."""
    types: set[str] = set()
    for part in api_name.split("|"):
        code = re.split(r"\s+", part.strip())[0]
        segs = code.split("-")
        if len(segs) >= 3 and segs[2].isdigit():
            types.add(segs[2])
        elif "LH" in code or code.startswith("T-"):
            types.add("21")
        elif "LW" in code or code.startswith("W-"):
            types.add("31")
    return types


def _display_name(station: dict[str, Any]) -> str:
    """Compute display name: address > displayName > name."""
    addr = (station.get("address") or "").strip(" ,")
    if addr:
        return addr
    dn = (station.get("displayName") or "").strip()
    if dn:
        return dn
    return (station.get("name") or "").strip()


def build_location_map(
    api_stations: list[dict[str, Any]],
    xlsx_entries: list[CsvEntry],
) -> dict[str, MatchedStation]:
    """
    Returns {location_id_str -> MatchedStation} for all API stations.
    Unmatched stations get default has_* = True, xlsx_matched = False.
    """
    # Build lookup structures from Excel
    code_map: dict[str, CsvEntry] = {e.csv_code: e for e in xlsx_entries}

    # 4dp-truncate + sensor-type index: (lat4, lon4, sensor_type) -> CsvEntry
    # Only include non-ambiguous entries (one entry per key)
    from collections import defaultdict
    coord_type_idx: dict[tuple[str, str, str], CsvEntry] = {}
    coord_type_counts: dict[tuple[str, str, str], int] = defaultdict(int)
    for e in xlsx_entries:
        k = (str(int(e.lat * 1e4)), str(int(e.lon * 1e4)), e.sensor_type)
        coord_type_counts[k] += 1
    for e in xlsx_entries:
        k = (str(int(e.lat * 1e4)), str(int(e.lon * 1e4)), e.sensor_type)
        if coord_type_counts[k] == 1:
            coord_type_idx[k] = e

    result: dict[str, MatchedStation] = {}
    matched_name = matched_coord = unmatched = 0

    for s in api_stations:
        loc_id = s["locationId"]
        dn = _display_name(s)
        coords = s.get("location", {}).get("coordinates", [None, None])
        lon_f, lat_f = coords[0], coords[1]

        # 1. Name-code match
        matched_entries: list[CsvEntry] = []
        for part in s.get("name", "").split("|"):
            code = re.split(r"\s+", part.strip())[0]
            if code in code_map:
                matched_entries.append(code_map[code])

        if matched_entries:
            has_temp = any(e.has_temp for e in matched_entries)
            has_humidity = any(e.has_humidity for e in matched_entries)
            has_wind = any(e.has_wind for e in matched_entries)
            # Use stationsname from the first climate (-21) entry, else first entry
            label = next(
                (e.stationsname for e in matched_entries if e.sensor_type == "21"),
                matched_entries[0].stationsname,
            )
            result[loc_id] = MatchedStation(
                display_name=label or dn,
                has_temp=has_temp,
                has_humidity=has_humidity,
                has_wind=has_wind,
                xlsx_matched=True,
            )
            matched_name += 1
            continue

        # 2. 4dp-truncate + sensor-type coordinate fallback
        if lat_f is not None and lon_f is not None:
            api_types = _infer_sensor_types(s.get("name", ""))
            coord_entries: list[CsvEntry] = []
            for stype in api_types:
                k = (str(int(lat_f * 1e4)), str(int(lon_f * 1e4)), stype)
                if k in coord_type_idx:
                    coord_entries.append(coord_type_idx[k])

            if coord_entries:
                has_temp = any(e.has_temp for e in coord_entries)
                has_humidity = any(e.has_humidity for e in coord_entries)
                has_wind = any(e.has_wind for e in coord_entries)
                label = next(
                    (e.stationsname for e in coord_entries if e.sensor_type == "21"),
                    coord_entries[0].stationsname,
                )
                result[loc_id] = MatchedStation(
                    display_name=label or dn,
                    has_temp=has_temp,
                    has_humidity=has_humidity,
                    has_wind=has_wind,
                    xlsx_matched=True,
                )
                matched_coord += 1
                continue

        # 3. No match — defaults
        result[loc_id] = MatchedStation(
            display_name=dn,
            has_temp=True,
            has_humidity=True,
            has_wind=True,
            xlsx_matched=False,
        )
        unmatched += 1

    log.info(
        "Station matching: %d by name, %d by coord+type, %d unmatched (defaults)",
        matched_name, matched_coord, unmatched,
    )
    return result


def build_csv_code_to_location_map(
    api_stations: list[dict[str, Any]],
    xlsx_entries: list[CsvEntry],
) -> dict[str, str]:
    """
    Returns {csv_code -> location_id_str} for backfill use.
    Only entries that can be matched are included.
    """
    code_map: dict[str, CsvEntry] = {e.csv_code: e for e in xlsx_entries}

    # 4dp+type coord index (non-ambiguous only)
    from collections import defaultdict
    coord_type_counts: dict[tuple[str, str, str], int] = defaultdict(int)
    for e in xlsx_entries:
        k = (str(int(e.lat * 1e4)), str(int(e.lon * 1e4)), e.sensor_type)
        coord_type_counts[k] += 1
    coord_type_idx: dict[tuple[str, str, str], CsvEntry] = {}
    for e in xlsx_entries:
        k = (str(int(e.lat * 1e4)), str(int(e.lon * 1e4)), e.sensor_type)
        if coord_type_counts[k] == 1:
            coord_type_idx[k] = e

    result: dict[str, str] = {}

    for s in api_stations:
        loc_id = s["locationId"]
        coords = s.get("location", {}).get("coordinates", [None, None])
        lon_f, lat_f = coords[0], coords[1]

        # Name-code match
        for part in s.get("name", "").split("|"):
            code = re.split(r"\s+", part.strip())[0]
            if code in code_map:
                result[code] = loc_id

        # 4dp+type coord fallback
        if lat_f is not None and lon_f is not None:
            api_types = _infer_sensor_types(s.get("name", ""))
            for stype in api_types:
                k = (str(int(lat_f * 1e4)), str(int(lon_f * 1e4)), stype)
                if k in coord_type_idx:
                    e = coord_type_idx[k]
                    if e.csv_code not in result:
                        result[e.csv_code] = loc_id

    return result


async def load_catalog(
    session: aiohttp.ClientSession,
    api_stations: list[dict[str, Any]],
) -> tuple[dict[str, MatchedStation], dict[str, str]]:
    """
    Download (or use cached) Excel, parse it, and return:
      (location_map, csv_code_to_location_id)
    """
    data = await _fetch_xlsx(session)
    entries = _parse_xlsx(data)
    location_map = build_location_map(api_stations, entries)
    csv_to_loc = build_csv_code_to_location_map(api_stations, entries)
    log.info("csv_code→location_id map: %d entries", len(csv_to_loc))
    return location_map, csv_to_loc
