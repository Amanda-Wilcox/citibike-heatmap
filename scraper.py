"""
scraper.py — Download and aggregate Citibike trip CSVs into processed JSON.

Usage:
    python scraper.py --months 3
    python scraper.py --months 1 --keep-raw
"""

import argparse
import io
import json
import math
import os
import re
import shutil
import zipfile
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import requests
import pandas as pd
from tqdm import tqdm

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Column normalisation — older exports use different names
# ---------------------------------------------------------------------------
COLUMN_ALIASES = {
    # IDs
    "start station id": "start_station_id",
    "end station id": "end_station_id",
    # Timestamps
    "starttime": "started_at",
    "stoptime": "ended_at",
    # Station names
    "start station name": "start_station_name",
    "end station name": "end_station_name",
    # Coordinates
    "start station latitude": "start_lat",
    "start station longitude": "start_lng",
    "end station latitude": "end_lat",
    "end station longitude": "end_lng",
}

REQUIRED_COLS = {
    "start_station_id",
    "end_station_id",
    "start_station_name",
    "end_station_name",
    "start_lat",
    "start_lng",
    "end_lat",
    "end_lng",
}

S3_BASE = "https://s3.amazonaws.com/tripdata/"
GBFS_STATIONS = "https://gbfs.citibikenyc.com/gbfs/en/station_information.json"

# ---------------------------------------------------------------------------
# S3 listing helpers
# ---------------------------------------------------------------------------

def list_s3_files() -> list[str]:
    """Parse the S3 bucket XML listing and return all .zip keys."""
    resp = requests.get(S3_BASE, timeout=30)
    resp.raise_for_status()
    # Extract <Key>...</Key> values
    keys = re.findall(r"<Key>([^<]+)</Key>", resp.text)
    return keys


def filter_recent_zips(keys: list[str], months: int) -> list[str]:
    """
    Return the most recent `months` zip files matching the NYC trip data pattern.
    Excludes Jersey City (JC-) files.
    Pattern: YYYYMM-citibike-tripdata.zip
    """
    pattern = re.compile(r"^(\d{6})-citibike-tripdata\.zip$")
    matched = []
    for key in keys:
        m = pattern.match(key)
        if m:
            matched.append((m.group(1), key))  # (YYYYMM, key)

    matched.sort(key=lambda x: x[0], reverse=True)
    return [key for _, key in matched[:months]]


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_file(key: str, dest: Path) -> Path:
    """Stream-download a file from the S3 bucket to dest."""
    url = S3_BASE + key
    dest_path = dest / key
    if dest_path.exists():
        print(f"  [cache] {key} already downloaded, skipping.")
        return dest_path

    print(f"  [download] {url}")
    with requests.get(url, stream=True, timeout=120) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with open(dest_path, "wb") as f, tqdm(
            total=total, unit="B", unit_scale=True, desc=key, leave=False
        ) as bar:
            for chunk in r.iter_content(chunk_size=1 << 20):  # 1 MB chunks
                f.write(chunk)
                bar.update(len(chunk))
    return dest_path


# ---------------------------------------------------------------------------
# GBFS station fallback
# ---------------------------------------------------------------------------

def fetch_gbfs_stations() -> dict:
    """
    Return {station_id: {name, lat, lng}} from GBFS endpoint.
    Used to fill in coordinates missing from CSV rows.
    """
    try:
        resp = requests.get(GBFS_STATIONS, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        stations = {}
        for s in data.get("data", {}).get("stations", []):
            sid = str(s.get("station_id", ""))
            if sid:
                stations[sid] = {
                    "name": s.get("name", ""),
                    "lat": s.get("lat"),
                    "lng": s.get("lon"),
                }
        return stations
    except Exception as e:
        print(f"  [warn] Could not fetch GBFS stations: {e}")
        return {}


# ---------------------------------------------------------------------------
# Column helpers
# ---------------------------------------------------------------------------

def normalise_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename legacy column names to modern names."""
    rename_map = {k: v for k, v in COLUMN_ALIASES.items() if k in df.columns}
    return df.rename(columns=rename_map)


OPTIONAL_COLS = {"rideable_type", "member_casual"}

def usecols_filter(col: str) -> bool:
    """Keep only columns we actually need (modern or legacy names)."""
    modern = REQUIRED_COLS
    legacy = set(COLUMN_ALIASES.keys())
    return col in modern or col in legacy or col in OPTIONAL_COLS


# ---------------------------------------------------------------------------
# Core aggregation
# ---------------------------------------------------------------------------

def stream_and_aggregate(
    zip_path: Path,
    od_counts: "defaultdict[tuple, int]",
    station_info: "defaultdict[str, dict]",
    station_trip_counts: "defaultdict[str, int]",
    chunksize: int = 50_000,
) -> None:
    """
    Stream a trip data ZIP without fully extracting it.
    Updates od_counts and station_info in-place.
    """
    with zipfile.ZipFile(zip_path) as zf:
        csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not csv_names:
            print(f"  [warn] No CSV found inside {zip_path.name}")
            return

        for csv_name in csv_names:
            print(f"    Streaming {csv_name} ...")
            with zf.open(csv_name) as raw_file:
                # Wrap in TextIOWrapper so pandas can seek/read normally
                text_file = io.TextIOWrapper(raw_file, encoding="utf-8", errors="replace")
                try:
                    reader = pd.read_csv(
                        text_file,
                        usecols=usecols_filter,
                        dtype={
                            "start_station_id": str,
                            "end_station_id": str,
                            "start station id": str,
                            "end station id": str,
                        },
                        chunksize=chunksize,
                        on_bad_lines="skip",
                    )
                    for chunk in reader:
                        chunk = normalise_columns(chunk)
                        # Drop rows missing critical fields
                        chunk = chunk.dropna(
                            subset=["start_station_id", "end_station_id",
                                    "start_lat", "start_lng",
                                    "end_lat", "end_lng"]
                        )
                        # Skip self-loops
                        chunk = chunk[chunk["start_station_id"] != chunk["end_station_id"]]

                        for row in chunk.itertuples(index=False):
                            sid = str(row.start_station_id)
                            eid = str(row.end_station_id)
                            pair = (sid, eid) if sid < eid else (eid, sid)
                            counts = od_counts[pair]
                            counts["total"] += 1
                            station_trip_counts[sid] += 1
                            station_trip_counts[eid] += 1

                            rideable = (getattr(row, "rideable_type", "") or "").lower()
                            if "electric" in rideable:
                                counts["electric"] += 1
                            else:
                                counts["classic"] += 1

                            member = (getattr(row, "member_casual", "") or "").lower()
                            if member == "member":
                                counts["member"] += 1
                            else:
                                counts["casual"] += 1

                            # Store station metadata (lat/lng/name)
                            if sid not in station_info:
                                station_info[sid] = {
                                    "name": getattr(row, "start_station_name", ""),
                                    "lat": float(row.start_lat),
                                    "lng": float(row.start_lng),
                                }
                            if eid not in station_info:
                                station_info[eid] = {
                                    "name": getattr(row, "end_station_name", ""),
                                    "lat": float(row.end_lat),
                                    "lng": float(row.end_lng),
                                }
                except Exception as e:
                    print(f"  [warn] Error reading {csv_name}: {e}")


# ---------------------------------------------------------------------------
# Colour mapping
# ---------------------------------------------------------------------------

def count_to_color(norm: float) -> str:
    """
    Map a normalised value [0,1] to a blue→yellow→red hex colour.
    0 → blue (#3182bd), 0.5 → yellow (#fec44f), 1 → red (#de2d26)
    """
    if norm <= 0.5:
        t = norm * 2  # 0→1 over [0, 0.5]
        r = int(49 + t * (254 - 49))
        g = int(130 + t * (196 - 130))
        b = int(189 + t * (79 - 189))
    else:
        t = (norm - 0.5) * 2  # 0→1 over [0.5, 1]
        r = int(254 + t * (222 - 254))
        g = int(196 + t * (45 - 196))
        b = int(79 + t * (38 - 79))
    return f"#{r:02x}{g:02x}{b:02x}"


def count_to_weight(norm: float) -> float:
    """Map a normalised value [0,1] to line weight 1–6."""
    return 1 + norm * 5


# ---------------------------------------------------------------------------
# JSON output builders
# ---------------------------------------------------------------------------

def build_od_json(
    od_counts: "defaultdict[tuple, defaultdict]",
    station_info: dict,
    gbfs_stations: dict,
    top_n: int = 1000,
) -> list[dict]:
    """
    Build a list of top-N O-D pair dicts with coordinates, color, weight,
    and rideable_type / member_casual breakdowns.
    """
    if not od_counts:
        return []

    # Sort by total count descending, take top N
    sorted_pairs = sorted(
        od_counts.items(), key=lambda x: x[1]["total"], reverse=True
    )[:top_n]

    totals = [c["total"] for _, c in sorted_pairs]
    log_min = math.log1p(min(totals))
    log_max = math.log1p(max(totals))
    log_range = max(log_max - log_min, 1)

    result = []
    for (sid, eid), counts in sorted_pairs:
        s = station_info.get(sid) or gbfs_stations.get(sid)
        e = station_info.get(eid) or gbfs_stations.get(eid)
        if not s or not e:
            continue
        if not (s.get("lat") and s.get("lng") and e.get("lat") and e.get("lng")):
            continue

        total = counts["total"]
        electric = counts["electric"]
        classic = counts["classic"]
        member = counts["member"]
        casual = counts["casual"]

        norm = (math.log1p(total) - log_min) / log_range
        result.append({
            "start_id": sid,
            "end_id": eid,
            "start_name": s.get("name", sid),
            "end_name": e.get("name", eid),
            "start_lat": s["lat"],
            "start_lng": s["lng"],
            "end_lat": e["lat"],
            "end_lng": e["lng"],
            "count": total,
            "electric": electric,
            "classic": classic,
            "electric_pct": round(electric / total, 4) if total else 0,
            "member": member,
            "casual": casual,
            "member_pct": round(member / total, 4) if total else 0,
            "color": count_to_color(norm),
            "weight": round(count_to_weight(norm), 2),
        })

    return result


def build_station_density(
    station_info: dict,
    station_trip_counts: "defaultdict[str, int]",
) -> list[list]:
    """
    Return [[lat, lng, intensity], ...] for Leaflet.heat.
    Intensity is log-normalised to [0, 1].
    """
    if not station_trip_counts:
        return []

    counts = list(station_trip_counts.values())
    log_min = math.log1p(min(counts))
    log_max = math.log1p(max(counts))
    log_range = max(log_max - log_min, 1)

    result = []
    for sid, count in station_trip_counts.items():
        info = station_info.get(sid)
        if not info:
            continue
        lat, lng = info.get("lat"), info.get("lng")
        if lat is None or lng is None:
            continue
        norm = (math.log1p(count) - log_min) / log_range
        result.append([round(lat, 6), round(lng, 6), round(norm, 4)])

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(months: int, keep_raw: bool) -> None:
    print(f"=== Citibike Scraper — fetching {months} month(s) ===")

    print("Listing S3 bucket ...")
    all_keys = list_s3_files()
    zips_to_fetch = filter_recent_zips(all_keys, months)

    if not zips_to_fetch:
        print("ERROR: No matching zip files found on S3.")
        return

    print(f"Found {len(zips_to_fetch)} file(s): {zips_to_fetch}")

    print("Fetching GBFS station list as coordinate fallback ...")
    gbfs_stations = fetch_gbfs_stations()
    print(f"  {len(gbfs_stations)} stations loaded from GBFS.")

    od_counts: defaultdict = defaultdict(lambda: defaultdict(int))
    station_info: dict = {}
    station_trip_counts: defaultdict = defaultdict(int)

    downloaded_paths = []
    for key in zips_to_fetch:
        print(f"\nDownloading {key} ...")
        path = download_file(key, RAW_DIR)
        downloaded_paths.append(path)

        print(f"  Aggregating {key} ...")
        stream_and_aggregate(path, od_counts, station_info, station_trip_counts)

    total_trips = sum(c["total"] for c in od_counts.values())
    print(f"\nAggregation complete. Total O-D pairs: {len(od_counts):,}, total trips: {total_trips:,}")

    print("Building od_pairs.json ...")
    od_list = build_od_json(od_counts, station_info, gbfs_stations, top_n=1000)
    od_path = PROCESSED_DIR / "od_pairs.json"
    with open(od_path, "w") as f:
        json.dump(od_list, f)
    print(f"  Wrote {len(od_list)} O-D pairs to {od_path}")

    print("Building station_density.json ...")
    density = build_station_density(station_info, station_trip_counts)
    density_path = PROCESSED_DIR / "station_density.json"
    with open(density_path, "w") as f:
        json.dump(density, f)
    print(f"  Wrote {len(density)} station points to {density_path}")

    print("Writing metadata.json ...")
    meta = {
        "months_processed": months,
        "files": zips_to_fetch,
        "total_trips": total_trips,
        "od_pairs_count": len(od_list),
        "stations_count": len(density),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    with open(PROCESSED_DIR / "metadata.json", "w") as f:
        json.dump(meta, f, indent=2)

    if not keep_raw:
        print("Cleaning up raw ZIPs ...")
        for p in downloaded_paths:
            p.unlink(missing_ok=True)
        print("  Done.")

    print("\n=== Scraper finished successfully ===")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and aggregate Citibike trip data.")
    parser.add_argument("--months", type=int, default=3, help="Number of recent months to process (default: 3)")
    parser.add_argument("--keep-raw", action="store_true", help="Keep raw ZIP files after processing")
    args = parser.parse_args()
    main(months=args.months, keep_raw=args.keep_raw)
