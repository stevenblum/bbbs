#!/usr/bin/env python3
"""Plot date-range driver routes with day slider and autoplay."""

from __future__ import annotations

import argparse
import html
import json
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

try:
    import pandas as pd
except ModuleNotFoundError:  # pragma: no cover - runtime dependency guard
    pd = None

# ---- Config (edit these) ----
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
CSV_PATH = PROJECT_ROOT / "data_geocode" / "latest" / "data_geocode.csv"
START_DATE = "2023-01-01"  # YYYY-MM-DD; leave blank to auto-pick range
END_DATE = "2023-02-01"    # YYYY-MM-DD; leave blank to auto-pick range
DEFAULT_RANGE_DAYS = 30
OUTPUT_HTML = SCRIPT_DIR / "dash_route_map_range.html"
SAVERS_CSV = SCRIPT_DIR / "data_savers_addresses.csv"  # set to None to skip
OSRM_BASE_URL = "http://localhost:5000"
REQUEST_TIMEOUT_SECONDS = 25
ALLOW_STRAIGHT_LINE_FALLBACK = False
# -----------------------------

COL_DRIVER = "Driver"
COL_DATE = "Planned Date"
COL_STOP = "Actual Stop Number"
COL_LAT = "latitude"
COL_LON = "longitude"
COL_ADDRESS = "Address"
COL_OSM_DISPLAY_NAME = "display_name"

COLORS = [
    "#1f77b4",
    "#ff7f0e",
    "#2ca02c",
    "#d62728",
    "#9467bd",
    "#8c564b",
    "#e377c2",
    "#7f7f7f",
    "#bcbd22",
    "#17becf",
]


def _fmt_address(value: Any) -> str:
    if value is None:
        return "(missing)"
    text = str(value).strip()
    return text if text else "(missing)"


def _fmt_distance(distance_m: float | None) -> str:
    if distance_m is None:
        return "n/a"
    return f"{distance_m / 1000.0:.2f} km"


def _fmt_duration(duration_s: float | None) -> str:
    if duration_s is None:
        return "n/a"
    total_minutes = duration_s / 60.0
    if total_minutes < 60:
        return f"{total_minutes:.1f} min"
    hours = int(total_minutes // 60)
    minutes = int(round(total_minutes % 60))
    return f"{hours}h {minutes}m"


def fetch_osrm_route(coords_lat_lon: list[list[float]]) -> dict[str, Any]:
    coords_lon_lat = [f"{lon},{lat}" for lat, lon in coords_lat_lon]
    coord_segment = ";".join(coords_lon_lat)
    query = urlencode(
        {
            "overview": "full",
            "geometries": "geojson",
            "steps": "false",
        }
    )
    url = f"{OSRM_BASE_URL.rstrip('/')}/route/v1/driving/{coord_segment}?{query}"

    try:
        with urlopen(url, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            payload = json.load(response)
    except HTTPError as exc:
        raise RuntimeError(f"OSRM request failed with status {exc.code}") from exc
    except URLError as exc:
        reason = getattr(exc, "reason", None)
        if reason:
            raise RuntimeError(
                f"OSRM connection failed to {OSRM_BASE_URL}: {reason}"
            ) from exc
        raise RuntimeError(f"OSRM connection failed to {OSRM_BASE_URL}") from exc

    if payload.get("code") != "Ok":
        raise RuntimeError(f"OSRM response code: {payload.get('code')}")

    routes = payload.get("routes", [])
    if not routes:
        raise RuntimeError("OSRM response did not include routes")

    route = routes[0]
    geometry = route.get("geometry", {}).get("coordinates", [])
    if not geometry:
        raise RuntimeError("OSRM response route geometry is empty")

    points_lat_lon = [[lat, lon] for lon, lat in geometry]
    return {
        "distance_m": float(route["distance"]),
        "duration_s": float(route["duration"]),
        "points_lat_lon": points_lat_lon,
    }


def load_savers_points() -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    if not SAVERS_CSV:
        return points
    if not SAVERS_CSV.exists():
        print(f"Savers CSV not found: {SAVERS_CSV}")
        return points

    savers_df = pd.read_csv(SAVERS_CSV)
    required = {"address", "latitude", "longitude"}
    if not required.issubset(savers_df.columns):
        print("Savers CSV missing required columns: address, latitude, longitude")
        return points

    savers_df["latitude"] = pd.to_numeric(savers_df["latitude"], errors="coerce")
    savers_df["longitude"] = pd.to_numeric(savers_df["longitude"], errors="coerce")
    savers_df = savers_df.dropna(subset=["latitude", "longitude"])

    for _, row in savers_df.iterrows():
        points.append(
            {
                "lat": float(row["latitude"]),
                "lon": float(row["longitude"]),
                "address": html.escape(_fmt_address(row.get("address"))),
            }
        )
    return points


def build_html(
    day_payloads: list[dict[str, Any]],
    savers_points: list[dict[str, Any]],
    center_lat: float,
    center_lon: float,
    range_start: pd.Timestamp,
    range_end: pd.Timestamp,
) -> str:
    days_json = json.dumps(day_payloads)
    savers_json = json.dumps(savers_points)
    range_text = f"{range_start.strftime('%Y-%m-%d')} to {range_end.strftime('%Y-%m-%d')}"

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>OSRM Route Range Map</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=" crossorigin="" />
  <style>
    html, body {{
      margin: 0;
      padding: 0;
      height: 100%;
      font-family: Arial, sans-serif;
    }}
    #map {{
      width: 100%;
      height: 100%;
    }}
    .panel {{
      position: absolute;
      z-index: 1000;
      background: rgba(255, 255, 255, 0.95);
      border: 1px solid #ccc;
      border-radius: 8px;
      box-shadow: 0 1px 8px rgba(0, 0, 0, 0.16);
      padding: 10px 12px;
      font-size: 12px;
      line-height: 1.35;
    }}
    #summary {{
      top: 14px;
      left: 14px;
      min-width: 260px;
      max-width: 420px;
    }}
    #legend {{
      left: 14px;
      bottom: 14px;
      min-width: 320px;
      max-width: 560px;
      max-height: 44vh;
      overflow-y: auto;
    }}
    #date-control {{
      position: fixed;
      top: 14px;
      left: 50%;
      transform: translateX(-50%);
      z-index: 9999;
      background: rgba(255, 255, 255, 0.96);
      border: 1px solid #ccc;
      border-radius: 8px;
      box-shadow: 0 1px 8px rgba(0, 0, 0, 0.16);
      padding: 8px 12px;
      font-size: 12px;
      display: flex;
      align-items: center;
      gap: 8px;
    }}
  </style>
</head>
<body>
  <div id="date-control">
    <button id="play-toggle" style="padding:2px 8px;font-size:12px;">Play</button>
    <input id="date-slider" type="range" min="0" max="{max(len(day_payloads) - 1, 0)}" value="0" style="width:260px;" />
    <div id="date-label" style="font-weight:600;min-width:90px;text-align:center;"></div>
    <label for="speed-select">Delay</label>
    <select id="speed-select" style="padding:2px 4px;font-size:12px;">
      <option value="1" selected>1s</option>
      <option value="3">3s</option>
      <option value="5">5s</option>
      <option value="10">10s</option>
    </select>
    <div style="margin-left:6px;font-size:11px;color:#555;">
      Range: {range_text}
    </div>
  </div>

  <div id="summary" class="panel">
    <div><strong>Date:</strong> <span id="summary-date">-</span></div>
    <div><strong>Total Distance:</strong> <span id="summary-distance">-</span></div>
    <div><strong>Total Duration:</strong> <span id="summary-duration">-</span></div>
    <div><strong>Routes:</strong> <span id="summary-routes">-</span></div>
    <div><strong>Missing Coords:</strong> <span id="summary-missing">-</span></div>
    <div><strong>Savers Locations:</strong> {len(savers_points)}</div>
  </div>

  <div id="legend" class="panel">
    <div style="margin-bottom:8px;"><strong>Route Legend (Driver)</strong></div>
    <div id="legend-content"></div>
  </div>

  <div id="map"></div>

  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
  <script>
    const center = [{center_lat}, {center_lon}];
    const dayData = {days_json};
    const saversPoints = {savers_json};
    const map = L.map('map').setView(center, 10);

    L.tileLayer('https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
      maxZoom: 19,
      attribution: '&copy; OpenStreetMap contributors'
    }}).addTo(map);

    function escapeHtml(text) {{
      return String(text)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
    }}

    function firstStopIcon(color) {{
      return L.divIcon({{
        className: '',
        iconSize: [22, 22],
        iconAnchor: [11, 11],
        html: '<div style="width:22px;height:22px;border-radius:50%;background:' + color + ';color:#fff;font-weight:700;display:flex;align-items:center;justify-content:center;border:2px solid #fff;box-shadow:0 0 0 1px ' + color + ';font-size:12px;">1</div>'
      }});
    }}

    function lastStopIcon(color) {{
      return L.divIcon({{
        className: '',
        iconSize: [22, 22],
        iconAnchor: [11, 11],
        html: '<div style="width:22px;height:22px;border-radius:50%;background:#fff;border:2px solid ' + color + ';display:flex;align-items:center;justify-content:center;box-shadow:0 0 0 1px ' + color + ';"><div style="width:10px;height:10px;background:' + color + ';"></div></div>'
      }});
    }}

    function saversIcon() {{
      return L.divIcon({{
        className: '',
        iconSize: [24, 24],
        iconAnchor: [12, 12],
        html: '<div style="width:24px;height:24px;border-radius:50%;background:#d62728;color:#fff;font-weight:700;display:flex;align-items:center;justify-content:center;border:2px solid #fff;box-shadow:0 0 0 1px #b51f1f;font-size:13px;">S</div>'
      }});
    }}

    function stopDotIcon(color) {{
      return L.divIcon({{
        className: '',
        iconSize: [10, 10],
        iconAnchor: [5, 5],
        html: '<div style="width:10px;height:10px;border-radius:50%;background:' + color + ';display:flex;align-items:center;justify-content:center;"><div style="width:5.6px;height:5.6px;border-radius:50%;background:#000;"></div></div>'
      }});
    }}

    const bounds = [];

    for (const s of saversPoints) {{
      L.marker([s.lat, s.lon], {{ icon: saversIcon() }}).addTo(map).bindPopup(
        'Savers Drop-off<br>' + s.address
      );
      bounds.push([s.lat, s.lon]);
    }}

    const dateLayers = {{}};
    for (const day of dayData) {{
      const layer = L.layerGroup();
      for (const route of day.routes) {{
        if (route.route_points.length > 0) {{
          for (const p of route.route_points) bounds.push(p);
        }}

        if (route.route_points.length >= 2) {{
          const baseLineStyle = {{
            color: route.color,
            weight: 4,
            opacity: 0.9
          }};
          const focusLineStyle = {{
            color: route.color,
            weight: 7,
            opacity: 1.0
          }};
          const line = L.polyline(route.route_points, baseLineStyle).addTo(layer);
          line.on('mouseover', function() {{
            this.setStyle(focusLineStyle);
            this.bringToFront();
          }});
          line.on('mouseout', function() {{
            this.setStyle(baseLineStyle);
          }});
        }}

        for (const stop of route.stops) {{
          L.marker([stop.lat, stop.lon], {{
            icon: stopDotIcon(route.color)
          }}).addTo(layer).bindPopup(
            route.driver + ' | Stop ' + stop.stop +
            '<br>Raw Address: ' + stop.raw_address +
            '<br>OSM Display Name: ' + stop.osm_display_name
          );
        }}

        if (route.first_stop) {{
          L.marker(route.first_stop, {{
            icon: firstStopIcon(route.color)
          }}).addTo(layer).bindPopup(route.driver + ' | First stop');
          bounds.push(route.first_stop);
        }}

        if (route.last_stop) {{
          L.marker(route.last_stop, {{
            icon: lastStopIcon(route.color)
          }}).addTo(layer).bindPopup(route.driver + ' | Last stop');
          bounds.push(route.last_stop);
        }}
      }}
      dateLayers[day.date] = layer;
    }}

    const slider = document.getElementById('date-slider');
    const label = document.getElementById('date-label');
    const playBtn = document.getElementById('play-toggle');
    const speedSelect = document.getElementById('speed-select');
    const legendContent = document.getElementById('legend-content');
    const summaryDate = document.getElementById('summary-date');
    const summaryDistance = document.getElementById('summary-distance');
    const summaryDuration = document.getElementById('summary-duration');
    const summaryRoutes = document.getElementById('summary-routes');
    const summaryMissing = document.getElementById('summary-missing');

    let timer = null;
    let playing = false;

    function renderLegend(day) {{
      if (!day || !day.routes || day.routes.length === 0) {{
        legendContent.innerHTML = '<div>No routes for this date.</div>';
        return;
      }}

      const rows = day.routes.map((route) => {{
        const osrmTag = route.osrm_error
          ? (route.straight_fallback_used ? ' (Straight-line fallback)' : ' (OSRM route unavailable)')
          : '';
        return (
          "<div style='margin-bottom:6px;'>" +
          "<span style='display:inline-block;width:12px;height:12px;background:" + route.color + ";margin-right:6px;vertical-align:middle;'></span>" +
          "<strong>" + escapeHtml(route.driver) + "</strong> " +
          "(stops " + route.stops_total + ", missing " + route.missing_stops + "): " +
          escapeHtml(route.distance_text) + ", " + escapeHtml(route.duration_text) +
          osrmTag +
          "</div>"
        );
      }});
      rows.push(
        "<div style='margin-top:8px;font-size:11px;color:#555;'>Missing coord rows (total): " +
        day.missing_total +
        "</div>"
      );
      legendContent.innerHTML = rows.join('');
    }}

    function renderSummary(day) {{
      summaryDate.textContent = day.date;
      summaryDistance.textContent = day.total_distance_text;
      summaryDuration.textContent = day.total_duration_text;
      summaryRoutes.textContent = String(day.routes_count);
      summaryMissing.textContent = String(day.missing_total);
    }}

    function showDateByIndex(idx) {{
      if (dayData.length === 0) {{
        return;
      }}
      const clamped = Math.max(0, Math.min(dayData.length - 1, idx));
      const selected = dayData[clamped];

      for (const day of dayData) {{
        const layer = dateLayers[day.date];
        if (layer && map.hasLayer(layer)) {{
          map.removeLayer(layer);
        }}
      }}

      const selectedLayer = dateLayers[selected.date];
      if (selectedLayer) {{
        selectedLayer.addTo(map);
      }}

      slider.value = String(clamped);
      label.textContent = selected.date;
      renderLegend(selected);
      renderSummary(selected);
    }}

    function stopPlayback() {{
      playing = false;
      if (timer) {{
        clearInterval(timer);
        timer = null;
      }}
      playBtn.textContent = 'Play';
    }}

    function startPlayback() {{
      if (dayData.length === 0) return;
      playing = true;
      playBtn.textContent = 'Pause';
      const delaySec = parseInt(speedSelect.value || '1', 10) || 1;
      timer = setInterval(() => {{
        const current = parseInt(slider.value || '0', 10);
        const next = (current + 1) % dayData.length;
        showDateByIndex(next);
      }}, delaySec * 1000);
    }}

    slider.addEventListener('input', (e) => {{
      stopPlayback();
      showDateByIndex(parseInt(e.target.value, 10));
    }});

    speedSelect.addEventListener('change', () => {{
      if (playing) {{
        stopPlayback();
        startPlayback();
      }}
    }});

    playBtn.addEventListener('click', () => {{
      if (playing) stopPlayback();
      else startPlayback();
    }});

    if (bounds.length > 0) {{
      map.fitBounds(bounds, {{ padding: [24, 24] }});
    }}
    showDateByIndex(0);
  </script>
</body>
</html>
"""


def main(argv: list[str] | None = None) -> None:
    global CSV_PATH
    global START_DATE
    global END_DATE
    global DEFAULT_RANGE_DAYS
    global OUTPUT_HTML
    global SAVERS_CSV
    global OSRM_BASE_URL
    global ALLOW_STRAIGHT_LINE_FALLBACK

    parser = argparse.ArgumentParser(
        description="Plot date-range driver routes with OSRM road geometry."
    )
    parser.add_argument("--input", default=str(CSV_PATH), help="Input stop-level CSV")
    parser.add_argument(
        "--output",
        "--output-file",
        dest="output",
        default=str(OUTPUT_HTML),
        help="Output HTML path",
    )
    parser.add_argument(
        "--range",
        default="",
        help="Date range as START,END (YYYY-MM-DD,YYYY-MM-DD); overrides --start-date/--end-date",
    )
    parser.add_argument("--start-date", default=START_DATE, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", default=END_DATE, help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--default-range-days",
        type=int,
        default=DEFAULT_RANGE_DAYS,
        help="Fallback range size when start/end are blank",
    )
    parser.add_argument(
        "--osrm-base-url",
        default=OSRM_BASE_URL,
        help="OSRM base URL (for example: http://localhost:5000)",
    )
    parser.add_argument(
        "--savers-csv",
        default=str(SAVERS_CSV),
        help="Optional Savers address CSV path",
    )
    parser.add_argument(
        "--no-savers",
        action="store_true",
        help="Disable Savers overlay markers",
    )
    parser.add_argument(
        "--allow-straight-line-fallback",
        action="store_true",
        help="If OSRM fails, connect stops with straight lines (disabled by default).",
    )
    args = parser.parse_args(argv)

    if pd is None:
        raise SystemExit("Missing dependency: pandas. Install it to run this script.")

    CSV_PATH = Path(args.input).expanduser()
    OUTPUT_HTML = Path(args.output).expanduser()
    range_arg = str(args.range or "").strip()
    if range_arg:
        normalized = range_arg.replace(":", ",")
        parts = [p.strip() for p in normalized.split(",") if p.strip()]
        if len(parts) != 2:
            raise SystemExit(
                "Invalid --range format. Use START,END (for example 2023-01-01,2023-01-31)."
            )
        START_DATE, END_DATE = parts
    else:
        START_DATE = args.start_date
        END_DATE = args.end_date
    DEFAULT_RANGE_DAYS = max(int(args.default_range_days), 1)
    OSRM_BASE_URL = args.osrm_base_url
    ALLOW_STRAIGHT_LINE_FALLBACK = bool(args.allow_straight_line_fallback)
    if args.no_savers or not str(args.savers_csv).strip():
        SAVERS_CSV = None
    else:
        SAVERS_CSV = Path(args.savers_csv).expanduser()

    if not CSV_PATH.exists():
        raise SystemExit(f"CSV not found: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH)
    required_cols = {COL_DRIVER, COL_DATE, COL_STOP, COL_LAT, COL_LON, COL_ADDRESS}
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise SystemExit(f"Missing required columns: {missing_cols}")

    df[COL_DRIVER] = df[COL_DRIVER].fillna("Unknown").astype(str).str.strip()
    df[COL_STOP] = pd.to_numeric(df[COL_STOP], errors="coerce")
    df[COL_LAT] = pd.to_numeric(df[COL_LAT], errors="coerce")
    df[COL_LON] = pd.to_numeric(df[COL_LON], errors="coerce")
    df["_planned_date"] = pd.to_datetime(df[COL_DATE], errors="coerce")
    df = df[~df["_planned_date"].isna()].copy()
    if df.empty:
        raise SystemExit("No valid Planned Date values found in CSV")

    min_date = df["_planned_date"].min()
    max_date = df["_planned_date"].max()

    if START_DATE.strip() or END_DATE.strip():
        start = pd.to_datetime(START_DATE, errors="coerce") if START_DATE.strip() else min_date
        end = pd.to_datetime(END_DATE, errors="coerce") if END_DATE.strip() else max_date
    else:
        end = max_date
        start = end - pd.Timedelta(days=max(DEFAULT_RANGE_DAYS - 1, 0))

    if pd.isna(start) or pd.isna(end):
        raise SystemExit("Invalid START_DATE/END_DATE; use YYYY-MM-DD")
    if start > end:
        start, end = end, start

    df_range = df[(df["_planned_date"] >= start) & (df["_planned_date"] <= end)].copy()
    if df_range.empty:
        print(
            "No rows found in selected range "
            f"{start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}. "
            "Writing empty map."
        )
        savers_points = load_savers_points()
        valid_all = df.dropna(subset=[COL_LAT, COL_LON])
        if not valid_all.empty:
            center_lat = float(valid_all[COL_LAT].mean())
            center_lon = float(valid_all[COL_LON].mean())
        elif savers_points:
            center_lat = sum(p["lat"] for p in savers_points) / len(savers_points)
            center_lon = sum(p["lon"] for p in savers_points) / len(savers_points)
        else:
            center_lat, center_lon = 41.7, -71.5

        html_doc = build_html(
            day_payloads=[],
            savers_points=savers_points,
            center_lat=center_lat,
            center_lon=center_lon,
            range_start=start,
            range_end=end,
        )
        OUTPUT_HTML.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT_HTML.write_text(html_doc, encoding="utf-8")
        print(f"Saved map: {OUTPUT_HTML}")
        return

    df_range["_date_str"] = df_range["_planned_date"].dt.strftime("%Y-%m-%d")
    dates = sorted(df_range["_date_str"].unique())
    if not dates:
        print("No Planned Dates found in selected range. Writing empty map.")
        savers_points = load_savers_points()
        valid_all = df.dropna(subset=[COL_LAT, COL_LON])
        if not valid_all.empty:
            center_lat = float(valid_all[COL_LAT].mean())
            center_lon = float(valid_all[COL_LON].mean())
        elif savers_points:
            center_lat = sum(p["lat"] for p in savers_points) / len(savers_points)
            center_lon = sum(p["lon"] for p in savers_points) / len(savers_points)
        else:
            center_lat, center_lon = 41.7, -71.5

        html_doc = build_html(
            day_payloads=[],
            savers_points=savers_points,
            center_lat=center_lat,
            center_lon=center_lon,
            range_start=start,
            range_end=end,
        )
        OUTPUT_HTML.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT_HTML.write_text(html_doc, encoding="utf-8")
        print(f"Saved map: {OUTPUT_HTML}")
        return

    savers_points = load_savers_points()

    valid_all = df_range.dropna(subset=[COL_LAT, COL_LON])
    if not valid_all.empty:
        center_lat = float(valid_all[COL_LAT].mean())
        center_lon = float(valid_all[COL_LON].mean())
    elif savers_points:
        center_lat = sum(p["lat"] for p in savers_points) / len(savers_points)
        center_lon = sum(p["lon"] for p in savers_points) / len(savers_points)
    else:
        center_lat, center_lon = 41.7, -71.5

    day_payloads: list[dict[str, Any]] = []
    print(f"Date range: {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}")
    print(f"Days in range with routes: {len(dates)}")
    print(f"Savers locations: {len(savers_points)}")

    for date in dates:
        date_df = df_range[df_range["_date_str"] == date].copy()
        missing_mask = date_df[COL_LAT].isna() | date_df[COL_LON].isna()
        missing_total = int(missing_mask.sum())

        route_specs: list[dict[str, Any]] = []
        total_distance_m = 0.0
        total_duration_s = 0.0
        metric_routes_count = 0
        missing_metric_routes_count = 0

        drivers = sorted(date_df[COL_DRIVER].unique())
        print(f"Date: {date} | Routes (drivers): {len(drivers)} | Missing coords: {missing_total}")

        for idx, driver in enumerate(drivers):
            color = COLORS[idx % len(COLORS)]
            driver_df = date_df[date_df[COL_DRIVER] == driver].copy()
            driver_valid = (
                driver_df.dropna(subset=[COL_LAT, COL_LON]).sort_values(
                    COL_STOP, na_position="last"
                )
            )

            missing_stops = int((driver_df[COL_LAT].isna() | driver_df[COL_LON].isna()).sum())
            coords_lat_lon = [
                [float(row[COL_LAT]), float(row[COL_LON])] for _, row in driver_valid.iterrows()
            ]

            osrm_error = ""
            if len(coords_lat_lon) >= 2:
                try:
                    osrm_result = fetch_osrm_route(coords_lat_lon)
                    route_points = osrm_result["points_lat_lon"]
                    distance_m = float(osrm_result["distance_m"])
                    duration_s = float(osrm_result["duration_s"])
                    straight_fallback_used = False
                except Exception as exc:
                    if ALLOW_STRAIGHT_LINE_FALLBACK:
                        route_points = coords_lat_lon
                        straight_fallback_used = True
                    else:
                        route_points = []
                        straight_fallback_used = False
                    distance_m = None
                    duration_s = None
                    osrm_error = str(exc)
            elif len(coords_lat_lon) == 1:
                route_points = coords_lat_lon
                distance_m = 0.0
                duration_s = 0.0
                straight_fallback_used = False
            else:
                route_points = []
                distance_m = None
                duration_s = None
                straight_fallback_used = False

            if distance_m is not None:
                total_distance_m += distance_m
                metric_routes_count += 1
            else:
                missing_metric_routes_count += 1
            if duration_s is not None:
                total_duration_s += duration_s

            stops = []
            for _, row in driver_valid.iterrows():
                stop_num = row[COL_STOP]
                if pd.isna(stop_num):
                    stop_label = "?"
                else:
                    stop_label = (
                        str(int(stop_num)) if float(stop_num).is_integer() else str(stop_num)
                    )
                stops.append(
                    {
                        "lat": float(row[COL_LAT]),
                        "lon": float(row[COL_LON]),
                        "stop": stop_label,
                        "raw_address": html.escape(_fmt_address(row.get(COL_ADDRESS))),
                        "osm_display_name": html.escape(_fmt_address(row.get(COL_OSM_DISPLAY_NAME))),
                    }
                )

            first_stop = stops[0] if stops else None
            last_stop = stops[-1] if stops else None

            route_specs.append(
                {
                    "driver": driver,
                    "color": color,
                    "stops_total": int(len(driver_df)),
                    "missing_stops": missing_stops,
                    "distance_text": _fmt_distance(distance_m),
                    "duration_text": _fmt_duration(duration_s),
                    "route_points": route_points,
                    "stops": stops,
                    "first_stop": [first_stop["lat"], first_stop["lon"]] if first_stop else None,
                    "last_stop": [last_stop["lat"], last_stop["lon"]] if last_stop else None,
                    "osrm_error": osrm_error,
                    "straight_fallback_used": straight_fallback_used,
                }
            )

            print(
                f"  {driver}: stops={len(driver_df)}, valid={len(driver_valid)}, "
                f"missing={missing_stops}, distance={_fmt_distance(distance_m)}, "
                f"duration={_fmt_duration(duration_s)}"
            )
            if osrm_error:
                if straight_fallback_used:
                    print(f"    OSRM fallback to straight line: {osrm_error}")
                else:
                    print(f"    OSRM route unavailable: {osrm_error}")

        if metric_routes_count == 0:
            total_distance_text = "n/a"
            total_duration_text = "n/a"
        elif missing_metric_routes_count > 0:
            total_distance_text = f"{_fmt_distance(total_distance_m)} (partial)"
            total_duration_text = f"{_fmt_duration(total_duration_s)} (partial)"
        else:
            total_distance_text = _fmt_distance(total_distance_m)
            total_duration_text = _fmt_duration(total_duration_s)

        day_payloads.append(
            {
                "date": date,
                "routes": route_specs,
                "missing_total": missing_total,
                "total_distance_text": total_distance_text,
                "total_duration_text": total_duration_text,
                "routes_count": len(drivers),
            }
        )

    html_doc = build_html(
        day_payloads=day_payloads,
        savers_points=savers_points,
        center_lat=center_lat,
        center_lon=center_lon,
        range_start=start,
        range_end=end,
    )
    OUTPUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_HTML.write_text(html_doc, encoding="utf-8")
    print(f"Saved map: {OUTPUT_HTML}")


if __name__ == "__main__":
    main()
