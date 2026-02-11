#!/usr/bin/env python3
"""Generate an HTML dashboard for city-level metrics."""

import argparse
import csv
import json
from typing import Dict, List, Optional

INPUT_CSV = "data_city.csv"
OUTPUT_HTML = "dash_city.html"


def to_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def to_int(value: Optional[str]) -> Optional[int]:
    number = to_float(value)
    if number is None:
        return None
    try:
        return int(number)
    except (TypeError, ValueError):
        return None


def compute_center(points: List[Dict[str, float]]) -> List[float]:
    if not points:
        return [41.7, -71.5]
    lat = sum(p["lat"] for p in points) / len(points)
    lon = sum(p["lon"] for p in points) / len(points)
    return [lat, lon]


def main() -> None:
    parser = argparse.ArgumentParser(description="Create city dashboard HTML")
    parser.add_argument("--input", default=INPUT_CSV, help="Input city CSV")
    parser.add_argument("--output", default=OUTPUT_HTML, help="Output HTML path")
    args = parser.parse_args()

    with open(args.input, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    location_counts: List[int] = []
    stop_counts: List[int] = []
    map_points: List[Dict[str, object]] = []
    valid_geo_points: List[Dict[str, float]] = []
    unknown_location_total = 0
    unknown_stop_total = 0

    for row in rows:
        city_name = (row.get("city") or "").strip()
        locations = to_int(row.get("number_of_locations"))
        stops = to_int(row.get("number_of_stops"))
        lat = to_float(row.get("latitude"))
        lon = to_float(row.get("longitude"))

        is_unknown = (not city_name) or (city_name.upper() == "UNKNOWN")
        if is_unknown:
            unknown_location_total += locations if locations is not None else 0
            unknown_stop_total += stops if stops is not None else 0
            continue

        if locations is not None:
            location_counts.append(locations)
        if stops is not None:
            stop_counts.append(stops)

        if lat is None or lon is None or stops is None:
            continue

        valid_geo_points.append({"lat": lat, "lon": lon})
        map_points.append(
            {
                "city": city_name,
                "lat": lat,
                "lon": lon,
                "number_of_locations": locations if locations is not None else 0,
                "number_of_stops": stops,
            }
        )

    center = compute_center(valid_geo_points)

    html = """<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>City Data Dashboard</title>
  <script src=\"https://cdn.plot.ly/plotly-2.30.0.min.js\"></script>
  <link rel=\"stylesheet\" href=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.css\" integrity=\"sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=\" crossorigin=\"\"/>
  <script src=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.js\" integrity=\"sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=\" crossorigin=\"\"></script>
  <style>
    :root {
      --bg: #0e1117;
      --panel: #171c24;
      --panel-border: #2a3240;
      --text: #e6edf3;
      --muted: #a4b1c4;
    }
    body {
      margin: 0;
      font-family: \"IBM Plex Sans\", \"Segoe UI\", sans-serif;
      background: radial-gradient(circle at top, #182033, var(--bg));
      color: var(--text);
    }
    header {
      padding: 24px 32px 8px 32px;
    }
    h1 {
      margin: 0 0 6px 0;
      font-size: 28px;
    }
    p.subtitle {
      margin: 0;
      color: var(--muted);
      font-size: 14px;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 16px;
      padding: 20px 32px 32px 32px;
    }
    .card {
      position: relative;
      background: var(--panel);
      border: 1px solid var(--panel-border);
      border-radius: 12px;
      padding: 12px;
      min-height: 320px;
      box-shadow: 0 10px 30px rgba(0,0,0,0.3);
      transition: opacity 0.2s ease;
    }
    .card-wide {
      grid-column: span 2;
    }
    body.expanded-active {
      overflow: hidden;
    }
    .grid.expanded-active .card {
      opacity: 0.2;
      pointer-events: none;
    }
    .grid.expanded-active .card.expanded {
      opacity: 1;
      pointer-events: auto;
      position: fixed;
      top: 16px;
      right: 16px;
      bottom: 16px;
      left: 16px;
      z-index: 1000;
      min-height: 0;
    }
    .card-toolbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      margin: 2px 4px 8px 4px;
    }
    .chart-title-btn {
      font-size: 14px;
      color: var(--muted);
      background: transparent;
      border: 0;
      padding: 0;
      text-align: left;
      cursor: pointer;
      flex: 1;
    }
    .chart-title-btn:hover {
      color: var(--text);
    }
    .chart-title-btn:focus-visible,
    .chart-expand-btn:focus-visible {
      outline: 1px solid #5ad2f4;
      outline-offset: 2px;
      border-radius: 4px;
    }
    .chart-expand-btn {
      width: 24px;
      height: 24px;
      border: 1px solid var(--panel-border);
      border-radius: 6px;
      background: #111722;
      color: var(--text);
      font-size: 14px;
      line-height: 1;
      cursor: pointer;
    }
    .chart-content {
      width: 100%;
    }
    .map {
      height: 420px;
      border-radius: 10px;
      overflow: hidden;
    }
    @media (max-width: 1000px) {
      .card-wide { grid-column: span 1; }
    }
  </style>
</head>
<body>
  <header>
    <h1>City Data Dashboard</h1>
    <p class=\"subtitle\">Source: data_town.csv</p>
    <p class=\"subtitle\">Unknown City Totals: __UNKNOWN_LOCATION_TOTAL__ locations, __UNKNOWN_STOP_TOTAL__ stops</p>
  </header>

  <section class=\"grid\">
    <div class=\"card\">
      <div class=\"card-toolbar\">
        <button class=\"chart-title-btn\">Histogram: Number of Locations per City</button>
        <button class=\"chart-expand-btn\" aria-label=\"Expand chart\">⤢</button>
      </div>
      <div id=\"hist_locations\" class=\"chart-content\" style=\"height:280px;\"></div>
    </div>
    <div class=\"card\">
      <div class=\"card-toolbar\">
        <button class=\"chart-title-btn\">Histogram: Number of Stops per City</button>
        <button class=\"chart-expand-btn\" aria-label=\"Expand chart\">⤢</button>
      </div>
      <div id=\"hist_stops\" class=\"chart-content\" style=\"height:280px;\"></div>
    </div>
    <div class=\"card card-wide\">
      <div class=\"card-toolbar\">
        <button class=\"chart-title-btn\">City Map: Dot Size Scaled by Number of Stops</button>
        <button class=\"chart-expand-btn\" aria-label=\"Expand chart\">⤢</button>
      </div>
      <div id=\"city_map\" class=\"chart-content map\"></div>
    </div>
  </section>

  <script>
    const locationCounts = __LOCATION_COUNTS__;
    const stopCounts = __STOP_COUNTS__;
    const mapCenter = __MAP_CENTER__;
    const mapPoints = __MAP_POINTS__;

    const baseLayout = {
      margin: {t: 10, r: 10, b: 45, l: 45},
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      font: {color: '#e6edf3'}
    };

    Plotly.newPlot('hist_locations', [{
      x: locationCounts,
      type: 'histogram',
      marker: {color: '#5ad2f4'},
      autobinx: true,
      hovertemplate: 'Locations: %{x}<br>Cities: %{y}<extra></extra>'
    }], {
      ...baseLayout,
      xaxis: {title: 'Number of Locations'},
      yaxis: {title: 'Cities (log scale)', type: 'log'}
    }, {displayModeBar: false});

    Plotly.newPlot('hist_stops', [{
      x: stopCounts,
      type: 'histogram',
      marker: {color: '#8bd17c'},
      autobinx: true,
      hovertemplate: 'Stops: %{x}<br>Cities: %{y}<extra></extra>'
    }], {
      ...baseLayout,
      xaxis: {title: 'Number of Stops'},
      yaxis: {title: 'Cities (log scale)', type: 'log'}
    }, {displayModeBar: false});

    const map = L.map('city_map', {zoomControl: true}).setView(mapCenter, 9);
    L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
      attribution: '&copy; OpenStreetMap contributors',
      maxZoom: 19
    }).addTo(map);

    const stopValues = mapPoints.map((p) => p.number_of_stops);
    const minStops = stopValues.length ? Math.min(...stopValues) : 0;
    const maxStops = stopValues.length ? Math.max(...stopValues) : 1;
    const minRadius = 3;
    const maxRadius = 28;

    mapPoints.forEach((p) => {
      let radius = minRadius;
      if (maxStops > minStops) {
        const normalized = (p.number_of_stops - minStops) / (maxStops - minStops);
        radius = minRadius + normalized * (maxRadius - minRadius);
      }
      L.circleMarker([p.lat, p.lon], {
        radius,
        color: '#6fe3ff',
        weight: 1,
        fillColor: '#2ec4ff',
        fillOpacity: 0.62
      })
      .bindPopup(
        `<b>${p.city}</b><br/>Locations: ${p.number_of_locations}<br/>Stops: ${p.number_of_stops}`
      )
      .addTo(map);
    });

    const grid = document.querySelector('.grid');
    const cards = Array.from(document.querySelectorAll('.card'));
    let expandedCard = null;

    function resizeCardContent(card) {
      const content = card ? card.querySelector('.chart-content') : null;
      if (!content) return;
      if (content.classList.contains('map')) {
        if (content.id === 'city_map') {
          map.invalidateSize();
        }
        return;
      }
      Plotly.Plots.resize(content);
    }

    function setExpandedUI(card, expanded) {
      const expandBtn = card.querySelector('.chart-expand-btn');
      if (!expandBtn) return;
      expandBtn.textContent = expanded ? '✕' : '⤢';
      expandBtn.setAttribute('aria-label', expanded ? 'Collapse chart' : 'Expand chart');
    }

    function collapseCard(card) {
      if (!card) return;
      const content = card.querySelector('.chart-content');
      card.classList.remove('expanded');
      setExpandedUI(card, false);
      if (content && content.dataset.defaultHeight) {
        content.style.height = content.dataset.defaultHeight;
      }
      grid.classList.remove('expanded-active');
      document.body.classList.remove('expanded-active');
      expandedCard = null;
      setTimeout(() => resizeCardContent(card), 40);
    }

    function expandCard(card) {
      const content = card.querySelector('.chart-content');
      if (!content) return;
      if (expandedCard && expandedCard !== card) {
        collapseCard(expandedCard);
      }
      card.classList.add('expanded');
      setExpandedUI(card, true);
      content.style.height = 'calc(100vh - 130px)';
      grid.classList.add('expanded-active');
      document.body.classList.add('expanded-active');
      expandedCard = card;
      setTimeout(() => resizeCardContent(card), 40);
    }

    cards.forEach((card) => {
      const content = card.querySelector('.chart-content');
      const titleBtn = card.querySelector('.chart-title-btn');
      const expandBtn = card.querySelector('.chart-expand-btn');
      if (!content || !titleBtn || !expandBtn) return;
      content.dataset.defaultHeight = content.style.height || (content.classList.contains('map') ? '420px' : '320px');
      const toggle = () => {
        if (expandedCard === card) {
          collapseCard(card);
        } else {
          expandCard(card);
        }
      };
      titleBtn.addEventListener('click', toggle);
      expandBtn.addEventListener('click', (e) => {
        e.stopPropagation();
        toggle();
      });
    });

    document.addEventListener('keydown', (e) => {
      if (e.key === 'Escape' && expandedCard) {
        collapseCard(expandedCard);
      }
    });

    window.addEventListener('resize', () => {
      if (expandedCard) {
        resizeCardContent(expandedCard);
      }
    });
  </script>
</body>
</html>
"""

    html = html.replace("__LOCATION_COUNTS__", json.dumps(location_counts))
    html = html.replace("__STOP_COUNTS__", json.dumps(stop_counts))
    html = html.replace("__MAP_CENTER__", json.dumps(center))
    html = html.replace("__MAP_POINTS__", json.dumps(map_points))
    html = html.replace("__UNKNOWN_LOCATION_TOTAL__", str(unknown_location_total))
    html = html.replace("__UNKNOWN_STOP_TOTAL__", str(unknown_stop_total))

    with open(args.output, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Wrote dashboard to {args.output}")


if __name__ == "__main__":
    main()
