"""
app.py — Flask server for the Citibike NYC Route Heatmap.

Endpoints:
    GET /                       → index.html
    GET /api/routes?limit=200   → top N O-D pairs
    GET /api/stations           → station density heatmap points
    GET /api/subway             → NYC subway GeoJSON (proxied + cached)
    GET /api/mta/stations       → MTA subway station locations + lines
    GET /api/metadata           → scraper run metadata
"""

import json
from functools import lru_cache
from pathlib import Path

import requests
from flask import Flask, jsonify, render_template, request

app = Flask(__name__)

PROCESSED_DIR = Path(__file__).parent / "data" / "processed"

SUBWAY_GEOJSON_URL = (
    "https://data.cityofnewyork.us/api/geospatial/3qem-6v3v"
    "?method=export&type=GeoJSON&format=geojson"
)
SUBWAY_CACHE_PATH = PROCESSED_DIR / "subway_lines.geojson"


# ---------------------------------------------------------------------------
# Cached data loaders (static between scraper runs)
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _load_od_pairs() -> list:
    path = PROCESSED_DIR / "od_pairs.json"
    with open(path) as f:
        return json.load(f)


@lru_cache(maxsize=1)
def _load_stations() -> list:
    path = PROCESSED_DIR / "station_density.json"
    with open(path) as f:
        return json.load(f)


@lru_cache(maxsize=1)
def _load_metadata() -> dict:
    path = PROCESSED_DIR / "metadata.json"
    with open(path) as f:
        return json.load(f)


def _data_ready() -> bool:
    """Return True if all processed files exist."""
    needed = ["od_pairs.json", "station_density.json", "metadata.json"]
    return all((PROCESSED_DIR / n).exists() for n in needed)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/routes")
def api_routes():
    if not _data_ready():
        return jsonify({"error": "Data not ready. Run: python scraper.py --months 1"}), 503

    try:
        limit = min(int(request.args.get("limit", 200)), 1000)
    except ValueError:
        limit = 200

    pairs = _load_od_pairs()
    return jsonify(pairs[:limit])


@app.route("/api/stations")
def api_stations():
    if not _data_ready():
        return jsonify({"error": "Data not ready. Run: python scraper.py --months 1"}), 503

    return jsonify(_load_stations())


@app.route("/api/subway")
def api_subway():
    """Proxy + disk-cache the NYC subway GeoJSON from NYC Open Data."""
    # Serve from disk cache if available
    if SUBWAY_CACHE_PATH.exists():
        with open(SUBWAY_CACHE_PATH) as f:
            data = json.load(f)
        return jsonify(data)

    # Otherwise fetch from upstream
    try:
        resp = requests.get(SUBWAY_GEOJSON_URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        # Cache to disk
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        with open(SUBWAY_CACHE_PATH, "w") as f:
            json.dump(data, f)
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": f"Subway data unavailable: {e}"}), 502


@lru_cache(maxsize=1)
def _load_mta_stations() -> list:
    path = PROCESSED_DIR / "mta_stations.json"
    with open(path) as f:
        return json.load(f)


@app.route("/api/mta/stations")
def api_mta_stations():
    """Return MTA subway station locations and served lines."""
    path = PROCESSED_DIR / "mta_stations.json"
    if not path.exists():
        return jsonify({"error": "MTA station data not found. Run: python mta_scraper.py"}), 503
    return jsonify(_load_mta_stations())


@app.route("/api/metadata")
def api_metadata():
    if not _data_ready():
        return jsonify({"error": "Data not ready. Run: python scraper.py --months 1"}), 503

    return jsonify(_load_metadata())


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
