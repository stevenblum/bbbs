#!/usr/bin/env python3
"""Generate an HTML dashboard for location-level metrics."""

import argparse
import csv
import json
from typing import Dict, List, Optional


INPUT_CSV = "data_locations.csv"
OUTPUT_HTML = "dash_location.html"


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
    parser = argparse.ArgumentParser(description="Create location dashboard HTML")
    parser.add_argument("--input", default=INPUT_CSV, help="Input location CSV")
    parser.add_argument("--output", default=OUTPUT_HTML, help="Output HTML path")
    args = parser.parse_args()

    rows: List[Dict[str, str]] = []
    with open(args.input, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    unique_locations = len(rows)
    raw_2 = 0
    raw_3 = 0
    raw_4_plus = 0

    total_stops_all: List[int] = []
    total_stops_ge3: List[int] = []
    avg_planned: List[float] = []
    avg_actual: List[float] = []
    avg_delay: List[float] = []
    scatter_custom: List[List[str]] = []
    scatter_planned_log: List[float] = []
    scatter_actual_log: List[float] = []
    scatter_custom_log: List[List[str]] = []

    map_gt5: List[Dict[str, float]] = []
    heat_points_raw: List[List[float]] = []
    all_geo_points: List[Dict[str, float]] = []

    for row in rows:
        raw_count = to_int(row.get("raw_address_variants_count"))
        if raw_count == 2:
            raw_2 += 1
        elif raw_count == 3:
            raw_3 += 1
        elif raw_count is not None and raw_count >= 4:
            raw_4_plus += 1

        stops = to_int(row.get("total_number_of_stops"))
        if stops is not None:
            total_stops_all.append(stops)
            if stops >= 3:
                total_stops_ge3.append(stops)

        planned = to_float(row.get("average_planned_stop_duration"))
        actual = to_float(row.get("average_actual_stop_duration"))
        delay = to_float(row.get("average_stop_delay"))

        if planned is not None:
            avg_planned.append(planned)
        if delay is not None:
            avg_delay.append(delay)
        if planned is not None and actual is not None:
            avg_actual.append(actual)
            scatter_custom.append([
                row.get("location_id", ""),
                row.get("address_nominatim", ""),
                f"{planned:.2f}",
                f"{actual:.2f}",
            ])
            if planned > 0 and actual > 0:
                scatter_planned_log.append(planned)
                scatter_actual_log.append(actual)
                scatter_custom_log.append([
                    row.get("location_id", ""),
                    row.get("address_nominatim", ""),
                    f"{planned:.2f}",
                    f"{actual:.2f}",
                ])

        lat = to_float(row.get("latitude"))
        lon = to_float(row.get("longitude"))
        if lat is None or lon is None:
            continue

        point = {"lat": lat, "lon": lon}
        all_geo_points.append(point)

        weight = float(stops) if stops is not None else 1.0
        heat_points_raw.append([lat, lon, weight])

        if stops is not None and stops > 5:
            map_gt5.append(
                {
                    "lat": lat,
                    "lon": lon,
                    "stops": float(stops),
                    "location_id": row.get("location_id", ""),
                    "address": row.get("address_nominatim", ""),
                }
            )

    center = compute_center(all_geo_points)
    max_weight = max((p[2] for p in heat_points_raw), default=1.0)
    # Compress very high-stop locations so color variation is easier to see.
    # Resulting intensity is in [0.08, 0.55], where low-stop locations stay dim.
    heat_points: List[List[float]] = []
    for lat, lon, raw_weight in heat_points_raw:
        scaled = (raw_weight / max_weight) ** 0.65
        intensity = 0.08 + (0.47 * scaled)
        heat_points.append([lat, lon, intensity])

    html = """<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Location Data Dashboard</title>
  <script src=\"https://cdn.plot.ly/plotly-2.30.0.min.js\"></script>
  <link rel=\"stylesheet\" href=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.css\" integrity=\"sha256-p4NxAoJBhIIN+hmNHrzRCf9tD/miZyoHS5obTRR9BMY=\" crossorigin=\"\"/>
  <script src=\"https://unpkg.com/leaflet@1.9.4/dist/leaflet.js\" integrity=\"sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=\" crossorigin=\"\"></script>
  <script src=\"https://unpkg.com/leaflet.heat/dist/leaflet-heat.js\"></script>
  <style>
    :root {
      --bg: #0e1117;
      --panel: #171c24;
      --panel-border: #2a3240;
      --text: #e6edf3;
      --muted: #a4b1c4;
      --accent: #5ad2f4;
      --good: #8bd17c;
      --warn: #ffcf6e;
      --bad: #ff8e72;
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
    .stats {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 12px;
      padding: 10px 32px 0 32px;
    }
    .stat-card {
      background: var(--panel);
      border: 1px solid var(--panel-border);
      border-radius: 12px;
      padding: 14px 16px;
      box-shadow: 0 10px 30px rgba(0,0,0,0.25);
    }
    .stat-label {
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 6px;
    }
    .stat-value {
      font-size: 24px;
      font-weight: 700;
      line-height: 1;
    }
    .grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(340px, 1fr));
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
      outline: 1px solid var(--accent);
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
      height: 340px;
      border-radius: 10px;
      overflow: hidden;
    }
    @media (max-width: 1100px) {
      .card-wide { grid-column: span 1; }
    }
  </style>
</head>
<body>
  <header>
    <h1>Location Data Dashboard</h1>
    <p class=\"subtitle\">Location frequency, duration behavior, and geographic concentration | Source: data_locations.csv</p>
  </header>

  <section class=\"stats\">
    <div class=\"stat-card\">
      <div class=\"stat-label\">Total Unique Locations</div>
      <div class=\"stat-value\">__UNIQUE_LOCATIONS__</div>
    </div>
    <div class=\"stat-card\">
      <div class=\"stat-label\">Locations with 2 Raw Addresses</div>
      <div class=\"stat-value\" style=\"color:var(--good)\">__RAW_2__</div>
    </div>
    <div class=\"stat-card\">
      <div class=\"stat-label\">Locations with 3 Raw Addresses</div>
      <div class=\"stat-value\" style=\"color:var(--warn)\">__RAW_3__</div>
    </div>
    <div class=\"stat-card\">
      <div class=\"stat-label\">Locations with 4+ Raw Addresses</div>
      <div class=\"stat-value\" style=\"color:var(--bad)\">__RAW_4_PLUS__</div>
    </div>
  </section>

  <section class=\"grid\">
    <div class=\"card\">
      <div class=\"card-toolbar\">
        <button class=\"chart-title-btn\">Histogram: Total Stops per Location</button>
        <button class=\"chart-expand-btn\" aria-label=\"Expand chart\">⤢</button>
      </div>
      <div id=\"hist_total_stops\" class=\"chart-content\" style=\"height:280px;\"></div>
    </div>
    <div class=\"card\">
      <div class=\"card-toolbar\">
        <button class=\"chart-title-btn\">Histogram: Total Stops per Location (>= 3 stops)</button>
        <button class=\"chart-expand-btn\" aria-label=\"Expand chart\">⤢</button>
      </div>
      <div id=\"hist_total_stops_ge3\" class=\"chart-content\" style=\"height:280px;\"></div>
    </div>
    <div class=\"card card-wide\">
      <div class=\"card-toolbar\">
        <button class=\"chart-title-btn\">Map: Locations with More Than 5 Stops</button>
        <button class=\"chart-expand-btn\" aria-label=\"Expand chart\">⤢</button>
      </div>
      <div id=\"map_gt5\" class=\"chart-content map\"></div>
    </div>
    <div class=\"card card-wide\">
      <div class=\"card-toolbar\">
        <button class=\"chart-title-btn\">Heatmap: All Stops (weighted by total number of stops at each location)</button>
        <button class=\"chart-expand-btn\" aria-label=\"Expand chart\">⤢</button>
      </div>
      <div id=\"map_heat\" class=\"chart-content map\"></div>
    </div>
    <div class=\"card\">
      <div class=\"card-toolbar\">
        <button class=\"chart-title-btn\">Histogram: Average Planned Stop Duration</button>
        <button class=\"chart-expand-btn\" aria-label=\"Expand chart\">⤢</button>
      </div>
      <div id=\"hist_avg_planned\" class=\"chart-content\" style=\"height:280px;\"></div>
    </div>
    <div class=\"card\">
      <div class=\"card-toolbar\">
        <button class=\"chart-title-btn\">Scatter: Avg Planned vs Avg Actual Stop Duration</button>
        <button class=\"chart-expand-btn\" aria-label=\"Expand chart\">⤢</button>
      </div>
      <div id=\"scatter_planned_actual\" class=\"chart-content\" style=\"height:320px;\"></div>
    </div>
    <div class=\"card\">
      <div class=\"card-toolbar\">
        <button class=\"chart-title-btn\">Histogram: Average Stop Delay (Actual - Planned)</button>
        <button class=\"chart-expand-btn\" aria-label=\"Expand chart\">⤢</button>
      </div>
      <div id=\"hist_avg_delay\" class=\"chart-content\" style=\"height:280px;\"></div>
    </div>
  </section>

  <script>
    const totalStopsAll = __TOTAL_STOPS_ALL__;
    const totalStopsGe3 = __TOTAL_STOPS_GE3__;
    const avgPlanned = __AVG_PLANNED__;
    const avgActual = __AVG_ACTUAL__;
    const avgDelay = __AVG_DELAY__;
    const scatterCustom = __SCATTER_CUSTOM__;
    const scatterPlannedLog = __SCATTER_PLANNED_LOG__;
    const scatterActualLog = __SCATTER_ACTUAL_LOG__;
    const scatterCustomLog = __SCATTER_CUSTOM_LOG__;

    const mapCenter = __MAP_CENTER__;
    const pointsGt5 = __POINTS_GT5__;
    const heatPoints = __HEAT_POINTS__;

    const baseLayout = {
      margin: {t: 10, r: 10, b: 45, l: 45},
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      font: {color: '#e6edf3'}
    };

    Plotly.newPlot('hist_total_stops', [{
      x: totalStopsAll,
      type: 'histogram',
      marker: {color: '#5ad2f4'},
      autobinx: true,
      hovertemplate: 'Stops: %{x}<br>Count: %{y}<extra></extra>'
    }], {
      ...baseLayout,
      xaxis: {title: 'Total Number of Stops'},
      yaxis: {title: 'Locations (log scale)', type: 'log'}
    }, {displayModeBar: false});

    Plotly.newPlot('hist_total_stops_ge3', [{
      x: totalStopsGe3,
      type: 'histogram',
      marker: {color: '#8bd17c'},
      autobinx: true,
      hovertemplate: 'Stops: %{x}<br>Count: %{y}<extra></extra>'
    }], {
      ...baseLayout,
      xaxis: {title: 'Total Number of Stops (>=3)'},
      yaxis: {title: 'Locations (log scale)', type: 'log'}
    }, {displayModeBar: false});

    Plotly.newPlot('hist_avg_planned', [{
      x: avgPlanned,
      type: 'histogram',
      marker: {color: '#ffcf6e'},
      autobinx: true,
      hovertemplate: 'Avg planned duration: %{x:.2f}<br>Count: %{y}<extra></extra>'
    }], {
      ...baseLayout,
      xaxis: {title: 'Average Planned Stop Duration'},
      yaxis: {title: 'Locations (log scale)', type: 'log'}
    }, {displayModeBar: false});

    Plotly.newPlot('scatter_planned_actual', [{
      x: scatterPlannedLog,
      y: scatterActualLog,
      mode: 'markers',
      type: 'scatter',
      marker: {size: 8, color: '#f8b195', opacity: 0.85},
      customdata: scatterCustomLog,
      hovertemplate: 'Location: %{customdata[0]}<br>Address: %{customdata[1]}<br>Avg Planned: %{customdata[2]}<br>Avg Actual: %{customdata[3]}<extra></extra>'
    }, {
      x: [Math.min(...scatterPlannedLog, 1), Math.max(...scatterPlannedLog, 1)],
      y: [Math.min(...scatterPlannedLog, 1), Math.max(...scatterPlannedLog, 1)],
      mode: 'lines',
      line: {dash: 'dot', color: '#6b7280'},
      hoverinfo: 'skip'
    }], {
      ...baseLayout,
      xaxis: {title: 'Average Planned Stop Duration (log)', type: 'log'},
      yaxis: {title: 'Average Actual Stop Duration (log)', type: 'log'}
    }, {displayModeBar: false});

    Plotly.newPlot('hist_avg_delay', [{
      x: avgDelay,
      type: 'histogram',
      marker: {color: '#ff8e72'},
      autobinx: true,
      hovertemplate: 'Average delay: %{x:.2f}<br>Count: %{y}<extra></extra>'
    }], {
      ...baseLayout,
      xaxis: {title: 'Average Stop Delay'},
      yaxis: {title: 'Locations (log scale)', type: 'log'}
    }, {displayModeBar: false});

    const tileUrl = 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png';
    const tileAttr = '&copy; OpenStreetMap contributors';

    const map1 = L.map('map_gt5', {zoomControl: true}).setView(mapCenter, 10);
    L.tileLayer(tileUrl, {attribution: tileAttr, maxZoom: 19}).addTo(map1);
    pointsGt5.forEach((p) => {
      const radius = Math.max(4, Math.min(14, Math.sqrt(p.stops)));
      L.circleMarker([p.lat, p.lon], {
        radius,
        color: '#6fe3ff',
        weight: 1,
        fillColor: '#2ec4ff',
        fillOpacity: 0.65
      })
      .bindPopup(`<b>${p.location_id}</b><br/>Stops: ${p.stops}<br/>${p.address}`)
      .addTo(map1);
    });

    const map2 = L.map('map_heat', {zoomControl: true}).setView(mapCenter, 10);
    L.tileLayer(tileUrl, {attribution: tileAttr, maxZoom: 19}).addTo(map2);
    L.heatLayer(heatPoints, {
      radius: 15,
      blur: 12,
      maxZoom: 12,
      minOpacity: 0.18,
      max: 0.95,
      gradient: {0.10: '#2ec4ff', 0.35: '#63d471', 0.65: '#ffcf6e', 1.0: '#ff6f61'}
    }).addTo(map2);

    const grid = document.querySelector('.grid');
    const cards = Array.from(document.querySelectorAll('.card'));
    let expandedCard = null;

    function resizeCardContent(card) {
      const content = card ? card.querySelector('.chart-content') : null;
      if (!content) return;
      if (content.classList.contains('map')) {
        if (content.id === 'map_gt5') {
          map1.invalidateSize();
        } else if (content.id === 'map_heat') {
          map2.invalidateSize();
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
      content.dataset.defaultHeight = content.style.height || (content.classList.contains('map') ? '340px' : '320px');
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

    html = html.replace("__UNIQUE_LOCATIONS__", str(unique_locations))
    html = html.replace("__RAW_2__", str(raw_2))
    html = html.replace("__RAW_3__", str(raw_3))
    html = html.replace("__RAW_4_PLUS__", str(raw_4_plus))

    html = html.replace("__TOTAL_STOPS_ALL__", json.dumps(total_stops_all))
    html = html.replace("__TOTAL_STOPS_GE3__", json.dumps(total_stops_ge3))
    html = html.replace("__AVG_PLANNED__", json.dumps(avg_planned))
    html = html.replace("__AVG_ACTUAL__", json.dumps(avg_actual))
    html = html.replace("__AVG_DELAY__", json.dumps(avg_delay))
    html = html.replace("__SCATTER_CUSTOM__", json.dumps(scatter_custom))
    html = html.replace("__SCATTER_PLANNED_LOG__", json.dumps(scatter_planned_log))
    html = html.replace("__SCATTER_ACTUAL_LOG__", json.dumps(scatter_actual_log))
    html = html.replace("__SCATTER_CUSTOM_LOG__", json.dumps(scatter_custom_log))

    html = html.replace("__MAP_CENTER__", json.dumps(center))
    html = html.replace("__POINTS_GT5__", json.dumps(map_gt5))
    html = html.replace("__HEAT_POINTS__", json.dumps(heat_points))

    with open(args.output, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Wrote dashboard to {args.output}")


if __name__ == "__main__":
    main()
