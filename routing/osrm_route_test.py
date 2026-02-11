#!/usr/bin/env python3
"""Minimal OSRM route test that renders an interactive HTML map."""

from __future__ import annotations

import json
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

# Edit these two points for quick route checks.
START_LAT, START_LON = (41.4505770,-71.4862690) # South County YMCA
END_LAT, END_LON = (41.8599823,-71.4593133) # North Providence High School

OSRM_BASE_URL = "http://localhost:5000"
OUTPUT_HTML = Path(__file__).with_name("osrm_route_test_map.html")
REQUEST_TIMEOUT_SECONDS = 20


def fetch_route() -> dict:
    coords = f"{START_LON},{START_LAT};{END_LON},{END_LAT}"
    query = urlencode(
        {
            "overview": "full",
            "geometries": "geojson",
            "steps": "false",
        }
    )
    url = f"{OSRM_BASE_URL.rstrip('/')}/route/v1/driving/{coords}?{query}"

    try:
        with urlopen(url, timeout=REQUEST_TIMEOUT_SECONDS) as response:
            payload = json.load(response)
    except HTTPError as exc:
        raise RuntimeError(f"OSRM request failed with status {exc.code}: {url}") from exc
    except URLError as exc:
        raise RuntimeError(
            "Could not connect to OSRM at "
            f"{OSRM_BASE_URL}. Make sure the Docker container is running."
        ) from exc

    if payload.get("code") != "Ok":
        raise RuntimeError(f"OSRM response error: {payload}")

    routes = payload.get("routes", [])
    if not routes:
        raise RuntimeError("OSRM response did not include any routes.")

    route = routes[0]
    geometry = route.get("geometry", {}).get("coordinates", [])
    if not geometry:
        raise RuntimeError("OSRM route geometry is empty.")

    # OSRM returns [lon, lat], Leaflet expects [lat, lon].
    points_lat_lon = [[lat, lon] for lon, lat in geometry]

    return {
        "distance_m": float(route["distance"]),
        "duration_s": float(route["duration"]),
        "points_lat_lon": points_lat_lon,
        "request_url": url,
    }


def build_html(route_data: dict) -> str:
    points_json = json.dumps(route_data["points_lat_lon"])
    start_json = json.dumps([START_LAT, START_LON])
    end_json = json.dumps([END_LAT, END_LON])
    distance_km = route_data["distance_m"] / 1000.0
    duration_min = route_data["duration_s"] / 60.0
    request_url = route_data["request_url"]

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>OSRM Route Test</title>
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" integrity="sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=" crossorigin="" />
  <style>
    html, body {{
      margin: 0;
      padding: 0;
      height: 100%;
      font-family: Arial, sans-serif;
    }}
    #map {{
      height: 100%;
      width: 100%;
    }}
    .summary {{
      position: absolute;
      top: 12px;
      left: 12px;
      z-index: 1000;
      background: rgba(255, 255, 255, 0.95);
      border: 1px solid #cfcfcf;
      border-radius: 8px;
      padding: 10px 12px;
      max-width: 420px;
      font-size: 14px;
      line-height: 1.4;
    }}
    .summary code {{
      word-break: break-all;
      font-size: 12px;
    }}
  </style>
</head>
<body>
  <div class="summary">
    <div><strong>Distance:</strong> {distance_km:.2f} km</div>
    <div><strong>Duration:</strong> {duration_min:.1f} minutes</div>
    <div><strong>OSRM URL:</strong><br><code>{request_url}</code></div>
  </div>
  <div id="map"></div>

  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
  <script>
    const routePoints = {points_json};
    const startPoint = {start_json};
    const endPoint = {end_json};

    const map = L.map('map');
    L.tileLayer('https://tile.openstreetmap.org/{{z}}/{{x}}/{{y}}.png', {{
      maxZoom: 19,
      attribution: '&copy; OpenStreetMap contributors'
    }}).addTo(map);

    const polyline = L.polyline(routePoints, {{
      color: '#1f77b4',
      weight: 5,
      opacity: 0.9
    }}).addTo(map);

    L.circleMarker(startPoint, {{
      radius: 7,
      color: '#2ca02c',
      fillColor: '#2ca02c',
      fillOpacity: 1
    }}).addTo(map).bindPopup('Start');

    L.circleMarker(endPoint, {{
      radius: 7,
      color: '#d62728',
      fillColor: '#d62728',
      fillOpacity: 1
    }}).addTo(map).bindPopup('End');

    map.fitBounds(polyline.getBounds(), {{ padding: [24, 24] }});
  </script>
</body>
</html>
"""


def main() -> None:
    route_data = fetch_route()
    html = build_html(route_data)
    OUTPUT_HTML.write_text(html, encoding="utf-8")
    print(f"Wrote route map: {OUTPUT_HTML}")
    print(f"Distance: {route_data['distance_m'] / 1000.0:.2f} km")
    print(f"Duration: {route_data['duration_s'] / 60.0:.1f} minutes")


if __name__ == "__main__":
    main()
