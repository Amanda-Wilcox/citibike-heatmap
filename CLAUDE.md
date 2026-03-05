# Citibike NYC Route Heatmap — Project Notes

## What This Is

A Flask web app that downloads Citibike trip history, aggregates popular origin-destination (O-D) pairs, and renders them as an interactive Leaflet.js map. The goal is to visualize cycling corridors across NYC, with a subway overlay for comparison.

## Architecture

```
scraper.py        → downloads Citibike trip ZIPs from S3, outputs processed JSON
mta_scraper.py    → fetches MTA subway data (stations, ridership, OTP, line geometry)
app.py            → Flask API server (5 endpoints)
templates/        → index.html (map shell + control panel)
static/js/map.js  → all Leaflet rendering logic
static/css/       → dark-theme styles
data/processed/   → output of scrapers (gitignored)
data/raw/         → temp ZIPs during scrape (gitignored)
```

## Running

```bash
pip install -r requirements.txt

# Pull Citibike trip data (adjust months as needed)
python scraper.py --months 3

# Pull MTA subway data + line geometry
python mta_scraper.py

# Start Flask
python app.py
# → http://localhost:5000
```

## Data Sources

| Data | Source |
|---|---|
| Citibike trips | S3: `s3.amazonaws.com/tripdata/` (monthly ZIPs) |
| Citibike station info | GBFS: `gbfs.citibikenyc.com` (coordinate fallback) |
| MTA subway stations | Socrata: `data.ny.gov/resource/39hk-dx4f` |
| MTA ridership | Socrata: `data.ny.gov/resource/wujg-7c2s` |
| MTA on-time performance | Socrata: `vtvh-gimj` (2020–24) + `ks33-g5ze` (2025+) |
| Subway line geometry | MTA GTFS: `web.mta.info/developers/data/nyct/subway/google_transit.zip` |

## Key Implementation Details

### scraper.py
- Streams CSVs from inside ZIPs without full extraction (`zipfile` + `pd.read_csv(chunksize=50_000)`)
- `COLUMN_ALIASES` dict normalizes legacy column names (older Citibike exports use `start station id`, etc.)
- `usecols` filter keeps only needed columns; tracks `rideable_type` and `member_casual` sub-counts
- Top 1000 O-D pairs written to `od_pairs.json` with log-normalized color and `electric_pct`/`member_pct`
- Station density written as `[[lat, lng, intensity], ...]` for Leaflet.heat

### mta_scraper.py
- Ridership uses single-day GROUP BY (15th of each month) × days_in_month to avoid Socrata timeout
- OTP aggregates both datasets (2020–24 + 2025+) into weekday/weekend/overall per line
- Subway line geometry built from GTFS `shapes.txt` → 252 shapes across 29 routes

### app.py
- All processed data loaded via `lru_cache`; returns 503 with message if scraper hasn't run
- `/api/subway` serves from `subway_lines.geojson` (written by `mta_scraper.py`)

### map.js
- Routes rendered as quadratic bezier arcs (manual math, no plugin); fixed 2px weight
- Color modes: trip volume (pre-computed), e-bike share, member share (computed from pcts)
- Popularity slider filters by `count <= threshold` (left = least popular pairs only)
- Subway overlay lazy-loads on first toggle: GTFS lines (colored by MTA official palette) + station dots

## Changes Made in This Session

1. **Built the full app** from scratch: scraper, Flask API, Leaflet map, dark-theme UI
2. **Scraper updated** to track `rideable_type` and `member_casual` per trip
3. **Color mode radio buttons** added: color routes by volume, e-bike share, or member share
4. **Route rendering** iterated through heatmap → straight lines → arc lines (final)
5. **Popularity slider** filters from least → most popular O-D pairs
6. **MTA data pipeline** added (`mta_scraper.py`): stations, ridership, OTP
7. **Subway overlay** added: 252 GTFS route shapes colored by MTA line + 496 station dots
   - NYC Open Data geospatial export API was broken (returns truncated response); switched to MTA GTFS `shapes.txt` for accurate track geometry
