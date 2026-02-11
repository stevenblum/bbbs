#!/usr/bin/env python3
"""Plot one day of driver routes with OSRM geometry and per-route totals."""

from __future__ import annotations

import html
import json
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

import pandas as pd

# ---- Config (edit these) ----
CSV_PATH = Path("data_geocode/data_geocode.csv")
TARGET_DATE = "2024-11-01"  # YYYY-MM-DD; set to "" to use latest date in CSV
OUTPUT_HTML = Path(__file__).with_name("osrm_day_routes_map.html")
SAVERS_CSV = Path("visualizations/data_savers_addresses.csv")  # set to None to skip
OSRM_BASE_URL = "http://localhost:5000"
REQUEST_TIMEOUT_SECONDS = 25
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
    selected_date: str,
    route_specs: list[dict[str, Any]],
    savers_points: list[dict[str, Any]],
    center_lat: float,
    center_lon: float,
    total_distance_text: str,
    total_duration_text: str,
    missing_total: int,
) -> str:
    routes_json = json.dumps(route_specs)
    savers_json = json.dumps(savers_points)

    legend_rows: list[str] = []
    for route in route_specs:
        error_suffix = ""
        if route.get("osrm_error"):
            error_suffix = " (OSRM fallback)"
        legend_rows.append(
            "<div style='margin-bottom:6px;'>"
            f"<span style='display:inline-block;width:12px;height:12px;background:{route['color']};"
            "margin-right:6px;vertical-align:middle;'></span>"
            f"<strong>{html.escape(route['driver'])}</strong> "
            f"(stops {route['stops_total']}, missing {route['missing_stops']}): "
            f"{html.escape(route['distance_text'])}, {html.escape(route['duration_text'])}"
            f"{error_suffix}"
            "</div>"
        )

    if not legend_rows:
        legend_rows.append("<div>No routes for this date.</div>")
    legend_rows.append(
        f"<div style='margin-top:8px;font-size:11px;color:#555;'>"
        f"Missing coord rows (total): {missing_total}</div>"
    )
    legend_html = "".join(legend_rows)

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>OSRM Day Routes</title>
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
      min-width: 300px;
      max-width: 520px;
      max-height: 44vh;
      overflow-y: auto;
    }}
  </style>
</head>
<body>
  <div id="summary" class="panel">
    <div><strong>Date:</strong> {html.escape(selected_date)}</div>
    <div><strong>Total Distance:</strong> {html.escape(total_distance_text)}</div>
    <div><strong>Total Duration:</strong> {html.escape(total_duration_text)}</div>
    <div><strong>Routes:</strong> {len(route_specs)}</div>
    <div><strong>Missing Coords:</strong> {missing_total}</div>
    <div><strong>Savers Locations:</strong> {len(savers_points)}</div>
  </div>
  <div id="legend" class="panel">
    <div style="margin-bottom:8px;"><strong>Route Legend (Driver)</strong></div>
    {legend_html}
  </div>
  <div id="map"></div>

  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
  <script>
    const center = [{center_lat}, {center_lon}];
    const routeSpecs = {routes_json};
    const saversPoints = {savers_json};

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

    const map = L.map('map').setView(center, 10);
    L.tileLayer('https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
      maxZoom: 19,
      attribution: '&copy; OpenStreetMap contributors'
    }}).addTo(map);

    const bounds = [];

    for (const s of saversPoints) {{
      L.marker([s.lat, s.lon], {{ icon: saversIcon() }}).addTo(map).bindPopup(
        'Savers Drop-off<br>' + s.address
      );
      bounds.push([s.lat, s.lon]);
    }}

    for (const route of routeSpecs) {{
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
        const line = L.polyline(route.route_points, baseLineStyle).addTo(map);
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
        }}).addTo(map).bindPopup(
          route.driver + ' | Stop ' + stop.stop +
          '<br>Raw Address: ' + stop.raw_address +
          '<br>OSM Display Name: ' + stop.osm_display_name
        );
      }}

      if (route.first_stop) {{
        L.marker(route.first_stop, {{
          icon: firstStopIcon(route.color)
        }}).addTo(map).bindPopup(route.driver + ' | First stop');
        bounds.push(route.first_stop);
      }}

      if (route.last_stop) {{
        L.marker(route.last_stop, {{
          icon: lastStopIcon(route.color)
        }}).addTo(map).bindPopup(route.driver + ' | Last stop');
        bounds.push(route.last_stop);
      }}
    }}

    if (bounds.length > 0) {{
      map.fitBounds(bounds, {{ padding: [24, 24] }});
    }}
  </script>
</body>
</html>
"""


def main() -> None:
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

    if TARGET_DATE.strip():
        chosen = pd.to_datetime(TARGET_DATE, errors="coerce")
        if pd.isna(chosen):
            raise SystemExit("Invalid TARGET_DATE format. Use YYYY-MM-DD")
        selected_date = chosen.strftime("%Y-%m-%d")
    else:
        selected_date = df["_planned_date"].max().strftime("%Y-%m-%d")

    day_df = df[df["_planned_date"].dt.strftime("%Y-%m-%d") == selected_date].copy()
    if day_df.empty:
        min_date = df["_planned_date"].min().strftime("%Y-%m-%d")
        max_date = df["_planned_date"].max().strftime("%Y-%m-%d")
        raise SystemExit(
            f"No rows found for {selected_date}. Available range is {min_date} to {max_date}."
        )

    missing_total = int((day_df[COL_LAT].isna() | day_df[COL_LON].isna()).sum())
    savers_points = load_savers_points()

    valid_all = day_df.dropna(subset=[COL_LAT, COL_LON])
    if not valid_all.empty:
        center_lat = float(valid_all[COL_LAT].mean())
        center_lon = float(valid_all[COL_LON].mean())
    elif savers_points:
        center_lat = sum(p["lat"] for p in savers_points) / len(savers_points)
        center_lon = sum(p["lon"] for p in savers_points) / len(savers_points)
    else:
        center_lat, center_lon = 41.7, -71.5

    route_specs: list[dict[str, Any]] = []
    total_distance_m = 0.0
    total_duration_s = 0.0
    metric_routes_count = 0
    missing_metric_routes_count = 0

    drivers = sorted(day_df[COL_DRIVER].unique())
    print(f"Date: {selected_date}")
    print(f"Routes (drivers): {len(drivers)}")
    print(f"Savers locations: {len(savers_points)}")
    print(f"Missing coord rows: {missing_total}")

    for idx, driver in enumerate(drivers):
        color = COLORS[idx % len(COLORS)]
        driver_df = day_df[day_df[COL_DRIVER] == driver].copy()
        driver_valid = (
            driver_df.dropna(subset=[COL_LAT, COL_LON]).sort_values(COL_STOP, na_position="last")
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
            except Exception as exc:
                route_points = coords_lat_lon
                distance_m = None
                duration_s = None
                osrm_error = str(exc)
        elif len(coords_lat_lon) == 1:
            route_points = coords_lat_lon
            distance_m = 0.0
            duration_s = 0.0
        else:
            route_points = []
            distance_m = None
            duration_s = None

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
                stop_label = str(int(stop_num)) if float(stop_num).is_integer() else str(stop_num)
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
            }
        )

        print(
            f"  {driver}: stops={len(driver_df)}, valid={len(driver_valid)}, "
            f"missing={missing_stops}, distance={_fmt_distance(distance_m)}, "
            f"duration={_fmt_duration(duration_s)}"
        )
        if osrm_error:
            print(f"    OSRM fallback: {osrm_error}")

    if metric_routes_count == 0:
        total_distance_text = "n/a"
        total_duration_text = "n/a"
    elif missing_metric_routes_count > 0:
        total_distance_text = f"{_fmt_distance(total_distance_m)} (partial)"
        total_duration_text = f"{_fmt_duration(total_duration_s)} (partial)"
    else:
        total_distance_text = _fmt_distance(total_distance_m)
        total_duration_text = _fmt_duration(total_duration_s)

    html_doc = build_html(
        selected_date=selected_date,
        route_specs=route_specs,
        savers_points=savers_points,
        center_lat=center_lat,
        center_lon=center_lon,
        total_distance_text=total_distance_text,
        total_duration_text=total_duration_text,
        missing_total=missing_total,
    )
    OUTPUT_HTML.write_text(html_doc, encoding="utf-8")
    print(f"Saved map: {OUTPUT_HTML}")


if __name__ == "__main__":
    main()
