"""
dataCollection.py  (fixed)
Iterate locations -> sensors -> sensor measurements for Kathmandu.
Defensive handling for missing fields so the script won't crash on malformed records.
"""

import requests
import time
from datetime import datetime, timedelta, timezone
import pandas as pd
import json

# ========== CONFIG ==========
API_KEY = "89248f97021e73583b66a60e9a98058ca24ea6ea3d5f7167028ed57d11a0d91b"
BASE = "https://api.openaq.org/v3"
CENTER_LAT = 27.717245
CENTER_LON = 85.323960
RADIUS_M = 12000
PAGE_LIMIT = 1000
SLEEP = 0.25
OUTPUT_CSV = "kathmandu_by_sensor.csv"
# Optionally stream to NDJSON to avoid memory pressure:
OUTPUT_NDJSON = None  # e.g. "kathmandu_by_sensor.ndjson" or None
PARAMETERS = ["pm25", "pm10"]   # or None for all parameters
CHUNK_DAYS = 30
# ============================

headers = {"X-API-Key": API_KEY} if API_KEY else {}

def iso_dt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def find_locations():
    url = f"{BASE}/locations"
    params = {
        "coordinates": f"{CENTER_LAT},{CENTER_LON}",
        "radius": RADIUS_M,
        "limit": 100,
        "page": 1
    }
    all_locs = []
    while True:
        r = requests.get(url, params=params, headers=headers, timeout=30)
        r.raise_for_status()
        j = r.json()
        results = j.get("results", [])
        if not results:
            break
        all_locs.extend(results)
        if len(results) < params["limit"]:
            break
        params["page"] += 1
        time.sleep(SLEEP)
    return all_locs

def chunk_ranges(start_dt: datetime, end_dt: datetime, days: int = CHUNK_DAYS):
    cur = start_dt
    while cur < end_dt:
        nxt = min(end_dt, cur + timedelta(days=days))
        yield iso_dt(cur), iso_dt(nxt)
        cur = nxt

def fetch_sensor_measurements(sensor_id: int, date_from: str, date_to: str):
    url = f"{BASE}/sensors/{sensor_id}/measurements"
    page = 1
    while True:
        params = {
            "date_from": date_from,
            "date_to": date_to,
            "limit": PAGE_LIMIT,
            "page": page
        }
        if PARAMETERS:
            params["parameter"] = ",".join(PARAMETERS)
        r = requests.get(url, params=params, headers=headers, timeout=60)
        # handle common HTTP issues gracefully
        if r.status_code == 404:
            print(f"Sensor {sensor_id}: 404 Not Found for URL: {r.url}")
            return
        if r.status_code != 200:
            print(f"Sensor {sensor_id}: HTTP {r.status_code} - skipping chunk. Body: {r.text[:400]}")
            return
        j = r.json()
        results = j.get("results", [])
        if not results:
            break
        for rec in results:
            yield rec
        if len(results) < PAGE_LIMIT:
            break
        page += 1
        time.sleep(SLEEP)

def safe_extract(rec):
    """
    Safely extract fields from a measurement record (handles None).
    Returns a dict with normalized fields.
    """
    if not rec:
        return None
    coords = rec.get("coordinates") or {}
    date = rec.get("date") or {}
    # some recs may have 'location' or nested 'sourceName' missing; use .get with default None
    return {
        "location": rec.get("location"),
        "parameter": rec.get("parameter"),
        "value": rec.get("value"),
        "unit": rec.get("unit"),
        "date_utc": date.get("utc"),
        "date_local": date.get("local"),
        "latitude": coords.get("latitude"),
        "longitude": coords.get("longitude"),
        "attribution": rec.get("attribution"),
        "sourceName": rec.get("sourceName"),
        "raw": rec
    }

def main():
    now = datetime.now(timezone.utc)
    two_years_ago = now - timedelta(days=365 * 2)

    print("Discovering locations...")
    locations = find_locations()
    print(f"Found {len(locations)} locations in radius {RADIUS_M}m")

    rows = []
    ndjson_fh = None
    if OUTPUT_NDJSON:
        ndjson_fh = open(OUTPUT_NDJSON, "a", encoding="utf-8")

    for loc in locations:
        loc_id = loc.get("id")
        loc_name = loc.get("name")
        sensors = loc.get("sensors", [])
        if not sensors:
            # fallback: explicit call
            try:
                r = requests.get(f"{BASE}/locations/{loc_id}/sensors", headers=headers, timeout=30)
                r.raise_for_status()
                sensors = r.json().get("results", [])
            except Exception as e:
                print(f"Could not fetch sensors for location {loc_id}: {e}")
                sensors = []
        print(f"Location {loc_id} '{loc_name}' -> {len(sensors)} sensors")

        for s in sensors:
            sensor_id = s.get("id")
            # parameter info might be nested differently; try both patterns
            sensor_param = (s.get("parameter") or {})
            if isinstance(sensor_param, dict):
                sensor_param_name = sensor_param.get("name") or sensor_param.get("display_name")
            else:
                sensor_param_name = sensor_param
            print(f"  Sensor {sensor_id} parameter={sensor_param_name}")

            for d_from, d_to in chunk_ranges(two_years_ago, now):
                print(f"    {d_from} -> {d_to}")
                try:
                    for rec in fetch_sensor_measurements(sensor_id, d_from, d_to):
                        try:
                            norm = safe_extract(rec)
                            if norm is None:
                                print(f"    WARNING: empty record for sensor {sensor_id}, chunk {d_from}->{d_to}")
                                continue
                            # attach location/sensor meta
                            out = {
                                "location_id": loc_id,
                                "location_name": loc_name,
                                "sensor_id": sensor_id,
                                "sensor_parameter": sensor_param_name,
                                **norm
                            }
                            if ndjson_fh:
                                ndjson_fh.write(json.dumps(out, default=str) + "\n")
                            else:
                                rows.append(out)
                        except Exception as e:
                            # catch per-record errors and continue
                            print(f"    ERROR processing record for sensor {sensor_id}: {e}. Record preview: {str(rec)[:300]}")
                            continue
                except Exception as e:
                    print(f"  ERROR fetching sensor {sensor_id} for {d_from}->{d_to}: {e}")
                    continue

    if ndjson_fh:
        ndjson_fh.close()

    if not rows and not OUTPUT_NDJSON:
        print("No measurements collected. Check API key, parameters, or endpoints.")
        return

    if rows:
        df = pd.DataFrame(rows)
        df.to_csv(OUTPUT_CSV, index=False)
        print(f"Saved {len(df)} rows to {OUTPUT_CSV}")
    else:
        print("Saved NDJSON file (streamed).")

if __name__ == "__main__":
    main()
