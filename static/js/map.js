/**
 * map.js — All Leaflet logic for the Citibike NYC Route Heatmap.
 */

"use strict";

// ---------------------------------------------------------------------------
// MTA official subway line colors keyed by rt_symbol
// ---------------------------------------------------------------------------
const SUBWAY_COLORS = {
  "1": "#EE352E", "2": "#EE352E", "3": "#EE352E",
  "4": "#00933C", "5": "#00933C", "6": "#00933C",
  "7": "#B933AD",
  "A": "#0039A6", "C": "#0039A6", "E": "#0039A6",
  "B": "#FF6319", "D": "#FF6319", "F": "#FF6319", "M": "#FF6319",
  "G": "#6CBE45",
  "J": "#996633", "Z": "#996633",
  "L": "#A7A9AC",
  "N": "#FCCC0A", "Q": "#FCCC0A", "R": "#FCCC0A", "W": "#FCCC0A",
  "S": "#808183",
  "SI": "#0039A6",
};

// ---------------------------------------------------------------------------
// Application state
// ---------------------------------------------------------------------------
const state = {
  allRoutes: [],             // full list from API (up to 1000)
  routeLayer: null,          // L.LayerGroup for arc polylines
  heatLayer: null,           // L.HeatLayer for station density
  subwayLayer: null,         // L.GeoJSON subway lines (lazy)
  subwayStationsLayer: null, // L.LayerGroup subway station dots (lazy)
  subwayLoaded: false,
  subwayLoading: false,
  colorMode: "volume", // "volume" | "electric" | "member"
  map: null,
};

// ---------------------------------------------------------------------------
// Map initialisation
// ---------------------------------------------------------------------------
function initMap() {
  state.map = L.map("map", {
    center: [40.73, -73.98],
    zoom: 13,
    preferCanvas: true,
  });

  L.tileLayer(
    "https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png",
    {
      attribution:
        '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
      subdomains: "abcd",
      maxZoom: 19,
    }
  ).addTo(state.map);

  state.routeLayer = L.layerGroup().addTo(state.map);
}

// ---------------------------------------------------------------------------
// Data fetching
// ---------------------------------------------------------------------------
async function fetchRoutes() {
  const resp = await fetch("/api/routes?limit=1000");
  if (!resp.ok) throw new Error(`Routes API error: ${resp.status}`);
  return resp.json();
}

async function fetchStations() {
  const resp = await fetch("/api/stations");
  if (!resp.ok) throw new Error(`Stations API error: ${resp.status}`);
  return resp.json();
}

async function fetchMetadata() {
  const resp = await fetch("/api/metadata");
  if (!resp.ok) throw new Error(`Metadata API error: ${resp.status}`);
  return resp.json();
}

async function fetchSubway() {
  const resp = await fetch("/api/subway");
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({}));
    throw new Error(body.error || `Subway API error: ${resp.status}`);
  }
  return resp.json();
}

async function fetchMtaStations() {
  const resp = await fetch("/api/mta/stations");
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({}));
    throw new Error(body.error || `MTA stations API error: ${resp.status}`);
  }
  return resp.json();
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/**
 * Render station density heatmap using Leaflet.heat.
 * @param {Array} points - [[lat, lng, intensity], ...]
 */
function renderHeatmap(points) {
  if (state.heatLayer) {
    state.map.removeLayer(state.heatLayer);
  }
  state.heatLayer = L.heatLayer(points, {
    radius: 20,
    blur: 15,
    maxZoom: 17,
    gradient: { 0.2: "#2166ac", 0.5: "#fdae61", 1.0: "#d73027" },
  }).addTo(state.map);
}

// ---------------------------------------------------------------------------
// Color helpers
// ---------------------------------------------------------------------------

/** Blue → yellow → red, matching scraper's count_to_color. */
function interpolateColor(norm) {
  let r, g, b;
  if (norm <= 0.5) {
    const t = norm * 2;
    r = Math.round(49  + t * (254 - 49));
    g = Math.round(130 + t * (196 - 130));
    b = Math.round(189 + t * (79  - 189));
  } else {
    const t = (norm - 0.5) * 2;
    r = Math.round(254 + t * (222 - 254));
    g = Math.round(196 + t * (45  - 196));
    b = Math.round(79  + t * (38  - 79));
  }
  return `rgb(${r},${g},${b})`;
}

/** Return the display color for a route based on the current color mode. */
function getRouteColor(route) {
  if (state.colorMode === "electric") return interpolateColor(route.electric_pct ?? 0);
  if (state.colorMode === "member")   return interpolateColor(route.member_pct   ?? 0);
  return route.color; // volume — pre-computed by scraper
}

const LEGEND_LABELS = {
  volume:   { title: "Trip volume",   low: "Low",     high: "High" },
  electric: { title: "E-bike share",  low: "0%",      high: "100%" },
  member:   { title: "Member share",  low: "0%",      high: "100%" },
};

function updateLegend() {
  const { title, low, high } = LEGEND_LABELS[state.colorMode];
  document.getElementById("legend-title").textContent = title;
  document.getElementById("legend-low").textContent   = low;
  document.getElementById("legend-high").textContent  = high;
}

/**
 * Sample N points along a quadratic bezier curve.
 * Control point is offset perpendicularly from the midpoint,
 * creating a consistent arc direction for all routes.
 */
function bezierArc(lat1, lng1, lat2, lng2, steps = 24) {
  const mlat = (lat1 + lat2) / 2;
  const mlng = (lng1 + lng2) / 2;
  const dlat = lat2 - lat1;
  const dlng = lng2 - lng1;
  // Perpendicular offset — rotate direction 90° and scale by 25% of length
  const clat = mlat - dlng * 0.25;
  const clng = mlng + dlat * 0.25;

  const pts = [];
  for (let i = 0; i <= steps; i++) {
    const t = i / steps;
    const u = 1 - t;
    pts.push([
      u * u * lat1 + 2 * u * t * clat + t * t * lat2,
      u * u * lng1 + 2 * u * t * clng + t * t * lng2,
    ]);
  }
  return pts;
}

/**
 * Render O-D routes as curved arcs, colored and weighted by trip volume.
 * Slider left = least popular only; slider right = all routes.
 * @param {number} maxTrips - show routes with count <= this value
 */
function renderRoutes(maxTrips) {
  state.routeLayer.clearLayers();

  const renderer = L.canvas();
  const routes = state.allRoutes.filter((r) => r.count <= maxTrips);

  routes.forEach((route) => {
    const latlngs = bezierArc(
      route.start_lat, route.start_lng,
      route.end_lat, route.end_lng
    );

    const line = L.polyline(latlngs, {
      color: getRouteColor(route),
      weight: 2,
      opacity: 0.65,
      renderer,
    });

    line.on("mouseover", function () {
      this.setStyle({ opacity: 1, weight: 3.5 });
    });
    line.on("mouseout", function () {
      this.setStyle({ opacity: 0.65, weight: 2 });
    });

    line.bindPopup(
      `<strong>${route.start_name}</strong> → <strong>${route.end_name}</strong><br/>
       Trips: <strong>${route.count.toLocaleString()}</strong>`,
      { maxWidth: 260 }
    );

    state.routeLayer.addLayer(line);
  });
}

/**
 * Render the NYC subway GeoJSON overlay (called once on first toggle).
 * @param {Object} geojson
 */
function renderSubway(geojson) {
  state.subwayLayer = L.geoJSON(geojson, {
    style: (feature) => {
      const rt = (feature.properties && feature.properties.rt_symbol) || "";
      return {
        color: SUBWAY_COLORS[rt] || "#ffffff",
        weight: 2.5,
        opacity: 0.85,
      };
    },
    onEachFeature: (feature, layer) => {
      const p = feature.properties || {};
      const lines = p.rt_symbol || p.name || "Subway";
      layer.bindPopup(`<strong>${lines}</strong>`);
    },
  }).addTo(state.map);
}

/**
 * Render MTA subway station dots, colored by the first served line.
 * @param {Array} stations - from /api/mta/stations
 */
function renderSubwayStations(stations) {
  state.subwayStationsLayer = L.layerGroup();
  stations.forEach((s) => {
    const firstLine = (s.lines && s.lines[0]) || "";
    const color = SUBWAY_COLORS[firstLine] || "#ffffff";
    const marker = L.circleMarker([s.lat, s.lng], {
      radius: 4,
      color: "#000",
      weight: 1,
      fillColor: color,
      fillOpacity: 0.9,
    });
    const lineList = (s.lines || []).join(" ");
    marker.bindPopup(
      `<strong>${s.name}</strong><br/>${lineList}`,
      { maxWidth: 200 }
    );
    state.subwayStationsLayer.addLayer(marker);
  });
  state.subwayStationsLayer.addTo(state.map);
}

// ---------------------------------------------------------------------------
// Metadata bar
// ---------------------------------------------------------------------------
function updateMetadataBar(meta) {
  const bar = document.getElementById("metadata-bar");
  const d = new Date(meta.generated_at).toLocaleDateString();
  bar.textContent =
    `${meta.months_processed} month(s) · ${(meta.total_trips || 0).toLocaleString()} trips · Updated ${d}`;
}

// ---------------------------------------------------------------------------
// Toast notifications
// ---------------------------------------------------------------------------
function showToast(message, type = "info") {
  const area = document.getElementById("toast-area");
  const toast = document.createElement("div");
  toast.className = `toast toast-${type}`;
  toast.textContent = message;
  area.appendChild(toast);

  // Animate in
  requestAnimationFrame(() => toast.classList.add("toast-visible"));

  // Auto-remove after 5s
  setTimeout(() => {
    toast.classList.remove("toast-visible");
    toast.addEventListener("transitionend", () => toast.remove());
  }, 5000);
}

// ---------------------------------------------------------------------------
// Controls
// ---------------------------------------------------------------------------

/**
 * Configure the popularity slider range from loaded route data and wire up its event.
 */
function initPopularitySlider() {
  const slider = document.getElementById("popularity-slider");
  const label = document.getElementById("popularity-label");

  const minCount = state.allRoutes[state.allRoutes.length - 1].count;
  const maxCount = state.allRoutes[0].count;

  slider.min = minCount;
  slider.max = maxCount;
  slider.value = maxCount;
  slider.step = Math.max(1, Math.round((maxCount - minCount) / 100));
  label.textContent = "Most";

  slider.addEventListener("input", () => {
    const threshold = parseInt(slider.value, 10);
    label.textContent =
      threshold === parseInt(slider.max) ? "Most" :
      threshold === parseInt(slider.min) ? "Least" :
      threshold.toLocaleString() + " trips";
    renderRoutes(threshold);
  });
}

function initControls() {

  // Color mode radio buttons
  document.querySelectorAll("input[name='color-mode']").forEach((radio) => {
    radio.addEventListener("change", (e) => {
      state.colorMode = e.target.value;
      updateLegend();
      const threshold = parseInt(document.getElementById("popularity-slider").value, 10);
      renderRoutes(threshold);
    });
  });

  // Routes toggle
  document.getElementById("toggle-routes").addEventListener("change", (e) => {
    if (e.target.checked) {
      state.map.addLayer(state.routeLayer);
    } else {
      state.map.removeLayer(state.routeLayer);
    }
  });

  // Station heatmap toggle
  document.getElementById("toggle-heatmap").addEventListener("change", (e) => {
    if (!state.heatLayer) return;
    if (e.target.checked) {
      state.map.addLayer(state.heatLayer);
    } else {
      state.map.removeLayer(state.heatLayer);
    }
  });

  // Subway map toggle — lazy load on first check
  const subwayCheckbox = document.getElementById("toggle-subway");
  subwayCheckbox.addEventListener("change", async (e) => {
    if (!e.target.checked) {
      if (state.subwayLayer)         state.map.removeLayer(state.subwayLayer);
      if (state.subwayStationsLayer) state.map.removeLayer(state.subwayStationsLayer);
      return;
    }

    // If already loaded, just re-add layers
    if (state.subwayLoaded) {
      if (state.subwayLayer)         state.map.addLayer(state.subwayLayer);
      if (state.subwayStationsLayer) state.map.addLayer(state.subwayStationsLayer);
      return;
    }

    // Prevent double-load
    if (state.subwayLoading) return;
    state.subwayLoading = true;
    subwayCheckbox.disabled = true;

    try {
      const [geojson, mtaStations] = await Promise.all([
        fetchSubway(),
        fetchMtaStations(),
      ]);
      renderSubway(geojson);
      renderSubwayStations(mtaStations);
      state.subwayLoaded = true;
    } catch (err) {
      console.error("Subway load failed:", err);
      showToast("Could not load subway data.", "error");
      subwayCheckbox.checked = false;
    } finally {
      state.subwayLoading = false;
      subwayCheckbox.disabled = false;
    }
  });

}

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------
async function init() {
  initMap();
  initControls();

  try {
    const [routes, stations, meta] = await Promise.all([
      fetchRoutes(),
      fetchStations(),
      fetchMetadata(),
    ]);

    state.allRoutes = routes;
    updateMetadataBar(meta);

    renderHeatmap(stations);

    initPopularitySlider();
    renderRoutes(state.allRoutes[0].count);
  } catch (err) {
    console.error("Init failed:", err);
    showToast(
      "Could not load data. Run: python scraper.py --months 1, then restart Flask.",
      "error"
    );
    document.getElementById("metadata-bar").textContent =
      "Data not available — run scraper first.";
  }
}

// Kick off when DOM is ready
document.addEventListener("DOMContentLoaded", init);
