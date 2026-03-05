"""
mta_scraper.py — Fetch and process public MTA subway data.

Outputs:
    data/processed/mta_stations.json      Station locations + routes + ADA
    data/processed/mta_ridership.json     Annual ridership per station complex
    data/processed/mta_ontime.json        On-time performance per line
    data/processed/subway_lines.geojson   Subway line geometry from GTFS shapes

Sources (all Socrata / data.ny.gov):
    Stations:   https://data.ny.gov/resource/39hk-dx4f
    Ridership:  https://data.ny.gov/resource/wujg-7c2s
    OTP 2020-24:https://data.ny.gov/resource/vtvh-gimj
    OTP 2025+:  https://data.ny.gov/resource/ks33-g5ze
    GTFS:       http://web.mta.info/developers/data/nyct/subway/google_transit.zip

Usage:
    python3 mta_scraper.py
    python3 mta_scraper.py --ridership-year 2023
"""

import argparse
import csv
import io
import json
import zipfile
from pathlib import Path
from collections import defaultdict

import requests

PROCESSED_DIR = Path(__file__).parent / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

SOCRATA_BASE = "https://data.ny.gov/resource"

# Dataset IDs
DS_STATIONS  = "39hk-dx4f"
DS_RIDERSHIP = "wujg-7c2s"
DS_OTP_2020  = "vtvh-gimj"   # 2020–2024
DS_OTP_2025  = "ks33-g5ze"   # 2025+

HEADERS = {"Accept": "application/json"}


# ---------------------------------------------------------------------------
# Generic Socrata paginator
# ---------------------------------------------------------------------------

def socrata_get_all(dataset_id: str, params: dict, page_size: int = 50_000) -> list:
    """Paginate through a Socrata dataset and return all rows."""
    rows = []
    offset = 0
    params = {**params, "$limit": page_size}
    while True:
        params["$offset"] = offset
        r = requests.get(
            f"{SOCRATA_BASE}/{dataset_id}.json",
            params=params,
            headers=HEADERS,
            timeout=60,
        )
        r.raise_for_status()
        batch = r.json()
        rows.extend(batch)
        print(f"    fetched {len(rows):,} rows so far ...")
        if len(batch) < page_size:
            break
        offset += page_size
    return rows


# ---------------------------------------------------------------------------
# 1. Stations
# ---------------------------------------------------------------------------

def fetch_stations() -> list[dict]:
    print("Fetching subway stations ...")
    rows = socrata_get_all(DS_STATIONS, {})
    stations = []
    for r in rows:
        try:
            stations.append({
                "station_id":    r.get("station_id"),
                "complex_id":    r.get("complex_id"),
                "gtfs_stop_id":  r.get("gtfs_stop_id"),
                "name":          r.get("stop_name"),
                "lines":         r.get("daytime_routes", "").split(),
                "borough":       r.get("borough"),
                "division":      r.get("division"),
                "structure":     r.get("structure"),
                "ada":           r.get("ada") == "1",
                "lat":           float(r["gtfs_latitude"]),
                "lng":           float(r["gtfs_longitude"]),
            })
        except (KeyError, ValueError, TypeError):
            continue
    print(f"  {len(stations)} stations loaded.")
    return stations


# ---------------------------------------------------------------------------
# 2. Ridership — aggregate via SoQL GROUP BY to avoid downloading millions of rows
# ---------------------------------------------------------------------------

import calendar

def fetch_ridership(year: int) -> list[dict]:
    """
    Sample one week per month and scale to a full-year estimate.
    Uses the first 7 days of each month — 12 fast single-week GROUP BY queries.
    """
    print(f"Fetching ridership for {year} (7-day sample × 12 months) ...")

    totals: dict = {}

    for month in range(1, 13):
        days_in_month = calendar.monthrange(year, month)[1]
        # Sample the 15th — a reliably normal midmonth day
        day_start = f"{year}-{month:02d}-15T00:00:00.000"
        day_end   = f"{year}-{month:02d}-16T00:00:00.000"

        params = {
            "$select": (
                "station_complex_id,"
                "station_complex,"
                "latitude,"
                "longitude,"
                "sum(ridership) as day_ridership"
            ),
            "$group": "station_complex_id,station_complex,latitude,longitude",
            "$where": (
                f"transit_timestamp >= '{day_start}' "
                f"AND transit_timestamp < '{day_end}'"
            ),
            "$limit": 1000,
        }
        r = requests.get(
            f"{SOCRATA_BASE}/{DS_RIDERSHIP}.json",
            params=params,
            headers=HEADERS,
            timeout=60,
        )
        r.raise_for_status()
        rows = r.json()
        scale = days_in_month  # scale single day to full month
        print(f"  Month {month:02d}: {len(rows)} complexes, scale={scale}")

        for row in rows:
            try:
                cid     = row["station_complex_id"]
                monthly = int(float(row["day_ridership"]) * scale)
                if cid not in totals:
                    totals[cid] = {
                        "complex_id":       cid,
                        "name":             row["station_complex"],
                        "lat":              float(row["latitude"]),
                        "lng":              float(row["longitude"]),
                        "annual_ridership": 0,
                        "year":             year,
                    }
                totals[cid]["annual_ridership"] += monthly
            except (KeyError, ValueError, TypeError):
                continue

    result = sorted(totals.values(), key=lambda x: x["annual_ridership"], reverse=True)
    print(f"  {len(result)} station complexes with ridership data.")
    return result


# ---------------------------------------------------------------------------
# 3. On-Time Performance — combine both datasets, aggregate by line
# ---------------------------------------------------------------------------

def fetch_otp() -> list[dict]:
    print("Fetching on-time performance ...")

    all_rows = []
    for ds_id in [DS_OTP_2020, DS_OTP_2025]:
        rows = socrata_get_all(ds_id, {"$order": "month ASC"})
        all_rows.extend(rows)
    print(f"  {len(all_rows):,} raw OTP rows across both datasets.")

    # Aggregate: mean OTP per line, split by weekday vs weekend
    # day_type: "1" = weekday, "2" = weekend/holiday
    line_data: dict = defaultdict(lambda: {
        "weekday_trips_on_time": 0, "weekday_trips_scheduled": 0,
        "weekend_trips_on_time": 0, "weekend_trips_scheduled": 0,
        "months": set(),
    })

    for row in all_rows:
        line     = row.get("line", "").strip()
        day_type = row.get("day_type", "1")
        month    = row.get("month", "")[:7]  # YYYY-MM
        try:
            on_time   = int(float(row["num_on_time_trips"]))
            scheduled = int(float(row["num_sched_trips"]))
        except (KeyError, ValueError, TypeError):
            continue

        d = line_data[line]
        if day_type == "1":
            d["weekday_trips_on_time"]   += on_time
            d["weekday_trips_scheduled"] += scheduled
        else:
            d["weekend_trips_on_time"]   += on_time
            d["weekend_trips_scheduled"] += scheduled
        d["months"].add(month)

    result = []
    for line, d in sorted(line_data.items()):
        wd_sched = d["weekday_trips_scheduled"]
        we_sched = d["weekend_trips_scheduled"]
        all_sched = wd_sched + we_sched
        result.append({
            "line":                  line,
            "months_of_data":        len(d["months"]),
            "weekday_otp":           round(d["weekday_trips_on_time"] / wd_sched, 4) if wd_sched else None,
            "weekend_otp":           round(d["weekend_trips_on_time"] / we_sched, 4) if we_sched else None,
            "overall_otp":           round(
                (d["weekday_trips_on_time"] + d["weekend_trips_on_time"]) / all_sched, 4
            ) if all_sched else None,
            "total_trips_scheduled": all_sched,
            "total_trips_on_time":   d["weekday_trips_on_time"] + d["weekend_trips_on_time"],
        })

    result.sort(key=lambda x: x["overall_otp"] or 0, reverse=True)
    print(f"  {len(result)} lines with OTP data.")
    return result


# ---------------------------------------------------------------------------
# 4. Subway line geometry from MTA GTFS shapes
# ---------------------------------------------------------------------------

GTFS_URL = "http://web.mta.info/developers/data/nyct/subway/google_transit.zip"


def fetch_subway_lines_geojson() -> dict:
    """
    Download the MTA GTFS zip, extract shapes.txt / trips.txt / routes.txt,
    and build a GeoJSON FeatureCollection where each feature is one route
    shape, with rt_symbol = route_short_name (e.g. 'A', '1', 'L').
    """
    print("Fetching MTA GTFS for subway line geometry ...")
    resp = requests.get(GTFS_URL, timeout=120)
    resp.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
        def read_csv(name):
            with zf.open(name) as f:
                return list(csv.DictReader(io.TextIOWrapper(f, encoding="utf-8")))

        shapes_rows  = read_csv("shapes.txt")
        trips_rows   = read_csv("trips.txt")
        routes_rows  = read_csv("routes.txt")

    # shape_id → ordered list of (lat, lng)
    shape_points: dict = defaultdict(list)
    for row in shapes_rows:
        shape_points[row["shape_id"]].append((
            int(row["shape_pt_sequence"]),
            float(row["shape_pt_lat"]),
            float(row["shape_pt_lon"]),
        ))
    # Sort each shape by sequence
    for sid in shape_points:
        shape_points[sid].sort(key=lambda x: x[0])

    # shape_id → route_id (first trip wins)
    shape_to_route: dict = {}
    for row in trips_rows:
        sid = row.get("shape_id", "")
        if sid and sid not in shape_to_route:
            shape_to_route[sid] = row["route_id"]

    # route_id → route_short_name
    route_name: dict = {r["route_id"]: r["route_short_name"] for r in routes_rows}

    features = []
    for shape_id, pts in shape_points.items():
        route_id = shape_to_route.get(shape_id, "")
        rt_symbol = route_name.get(route_id, route_id)
        coords = [[lng, lat] for _, lat, lng in pts]
        features.append({
            "type": "Feature",
            "geometry": {"type": "LineString", "coordinates": coords},
            "properties": {"rt_symbol": rt_symbol, "shape_id": shape_id},
        })

    print(f"  {len(features)} shapes across {len(set(shape_to_route.values()))} routes.")
    return {"type": "FeatureCollection", "features": features}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(ridership_year: int) -> None:
    print("=== MTA Scraper ===\n")

    stations = fetch_stations()
    with open(PROCESSED_DIR / "mta_stations.json", "w") as f:
        json.dump(stations, f)
    print(f"  Saved mta_stations.json ({len(stations)} stations)\n")

    ridership = fetch_ridership(ridership_year)
    with open(PROCESSED_DIR / "mta_ridership.json", "w") as f:
        json.dump(ridership, f)
    print(f"  Saved mta_ridership.json ({len(ridership)} complexes)\n")

    otp = fetch_otp()
    with open(PROCESSED_DIR / "mta_ontime.json", "w") as f:
        json.dump(otp, f, indent=2)
    print(f"  Saved mta_ontime.json ({len(otp)} lines)\n")

    geojson = fetch_subway_lines_geojson()
    with open(PROCESSED_DIR / "subway_lines.geojson", "w") as f:
        json.dump(geojson, f)
    print(f"  Saved subway_lines.geojson ({len(geojson['features'])} shapes)\n")

    print("=== MTA Scraper complete ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--ridership-year", type=int, default=2024,
        help="Year to aggregate ridership for (default: 2024)"
    )
    args = parser.parse_args()
    main(ridership_year=args.ridership_year)
