#!/usr/bin/env python3
"""Generate an HTML dashboard for route-level metrics."""

import argparse
import csv
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple


def to_float(value: str) -> Optional[float]:
    if value is None:
        return None
    value = str(value).strip()
    if value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def to_int(value: str) -> Optional[int]:
    number = to_float(value)
    if number is None:
        return None
    try:
        return int(number)
    except (TypeError, ValueError):
        return None


def time_to_minutes(value: str) -> Optional[float]:
    if value is None:
        return None
    value = str(value).strip()
    if value == "":
        return None
    parts = value.split(":")
    if len(parts) < 2:
        return None
    try:
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = int(parts[2]) if len(parts) > 2 else 0
    except ValueError:
        return None
    return hours * 60 + minutes + (seconds / 60.0)


def minmax(values: List[float]) -> Tuple[float, float]:
    if not values:
        return 0.0, 1.0
    return min(values), max(values)


def normalize_date(value: str) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    if text == "":
        return None
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Create an HTML dashboard for route data.")
    parser.add_argument("--input", default="data_route.csv", help="Input route CSV")
    parser.add_argument("--output", default="dash_route.html", help="Output HTML file")
    args = parser.parse_args()

    num_stops: List[int] = []
    edges_pct: List[float] = []

    start_planned: List[float] = []
    start_actual: List[float] = []
    start_custom: List[List[str]] = []

    end_planned: List[float] = []
    end_actual: List[float] = []
    end_custom: List[List[str]] = []

    dist_planned: List[float] = []
    dist_actual: List[float] = []
    dist_custom: List[List[str]] = []
    driver_day_dates: List[str] = []
    driver_day_names: List[str] = []
    driver_day_custom: List[List[str]] = []
    stops_by_driver: Dict[str, List[Tuple[str, int]]] = {}

    with open(args.input, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            driver = row.get("Driver", "").strip()
            route_date_raw = row.get("Route Date", "").strip()
            label = f"{driver} | {route_date_raw}"
            route_date = normalize_date(route_date_raw)

            ns = to_int(row.get("number_of_stops"))
            if ns is not None:
                num_stops.append(ns)
                if driver and route_date:
                    driver_day_dates.append(route_date)
                    driver_day_names.append(driver)
                    driver_day_custom.append([driver, route_date, str(ns)])
                    stops_by_driver.setdefault(driver, []).append((route_date, ns))

            edge_pct = to_float(row.get("edges_executed_as_planned_pct"))
            if edge_pct is not None:
                edges_pct.append(edge_pct)

            sp = time_to_minutes(row.get("first_stop_planned_time"))
            sa = time_to_minutes(row.get("first_stop_actual_time"))
            if sp is not None and sa is not None:
                start_planned.append(sp)
                start_actual.append(sa)
                start_custom.append(
                    [label, row.get("first_stop_planned_time", ""), row.get("first_stop_actual_time", "")]
                )

            ep = time_to_minutes(row.get("last_stop_planned_time"))
            ea = time_to_minutes(row.get("last_stop_actual_time"))
            if ep is not None and ea is not None:
                end_planned.append(ep)
                end_actual.append(ea)
                end_custom.append(
                    [label, row.get("last_stop_planned_time", ""), row.get("last_stop_actual_time", "")]
                )

            dp = to_float(row.get("route_distance_straight_line_planned_miles"))
            da = to_float(row.get("route_distance_straight_line_actual_miles"))
            if dp is not None and da is not None:
                dist_planned.append(dp)
                dist_actual.append(da)
                dist_custom.append([label, f"{dp:.2f}", f"{da:.2f}"])

    dist_min, dist_max = minmax(dist_planned + dist_actual)
    dist_pad = (dist_max - dist_min) * 0.05 if dist_max > dist_min else 1.0
    dist_range = [dist_min - dist_pad, dist_max + dist_pad]
    driver_last_date: Dict[str, str] = {
        driver: max(date for date, _ in pairs) for driver, pairs in stops_by_driver.items()
    }
    driver_categories = sorted(
        stops_by_driver.keys(), key=lambda driver: (driver_last_date[driver], driver), reverse=True
    )
    driver_stop_lines: List[Dict[str, object]] = []
    for driver in driver_categories:
        pairs = sorted(stops_by_driver[driver], key=lambda item: item[0])
        driver_stop_lines.append(
            {
                "name": driver,
                "x": [p[0] for p in pairs],
                "y": [p[1] for p in pairs],
            }
        )

    html = """<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>Route Data Dashboard</title>
  <script src=\"https://cdn.plot.ly/plotly-2.30.0.min.js\"></script>
  <style>
    :root {
      --bg: #0e1117;
      --panel: #171c24;
      --panel-border: #2a3240;
      --text: #e6edf3;
      --muted: #a4b1c4;
      --accent: #5ad2f4;
    }
    body {
      margin: 0;
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
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
      transition: opacity 0.2s ease, transform 0.2s ease;
    }
    body.expanded-active {
      overflow: hidden;
    }
    .grid.expanded-active .card {
      opacity: 0.22;
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
      transform: translateZ(0);
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
      text-align: left;
      padding: 0;
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
    .chart-expand-btn:hover {
      border-color: var(--accent);
    }
    .chart-content {
      width: 100%;
    }
  </style>
</head>
<body>
  <header>
    <h1>Route Data Dashboard</h1>
    <p class=\"subtitle\">Histograms and planned vs. actual comparisons</p>
  </header>

  <section class=\"grid\">
    <div class=\"card\">
      <div class=\"card-toolbar\">
        <button class=\"chart-title-btn\">Histogram: Number of Stops</button>
        <button class=\"chart-expand-btn\" aria-label=\"Expand chart\">⤢</button>
      </div>
      <div id=\"hist_stops\" class=\"chart-content\" style=\"height:280px;\"></div>
    </div>
    <div class=\"card\">
      <div class=\"card-toolbar\">
        <button class=\"chart-title-btn\">Histogram: Edges Executed as Planned</button>
        <button class=\"chart-expand-btn\" aria-label=\"Expand chart\">⤢</button>
      </div>
      <div id=\"hist_edges\" class=\"chart-content\" style=\"height:280px;\"></div>
    </div>
    <div class=\"card\">
      <div class=\"card-toolbar\">
        <button class=\"chart-title-btn\">Planned vs Actual Start Time</button>
        <button class=\"chart-expand-btn\" aria-label=\"Expand chart\">⤢</button>
      </div>
      <div id=\"scatter_start\" class=\"chart-content\" style=\"height:320px;\"></div>
    </div>
    <div class=\"card\">
      <div class=\"card-toolbar\">
        <button class=\"chart-title-btn\">Planned vs Actual End Time</button>
        <button class=\"chart-expand-btn\" aria-label=\"Expand chart\">⤢</button>
      </div>
      <div id=\"scatter_end\" class=\"chart-content\" style=\"height:320px;\"></div>
    </div>
    <div class=\"card\">
      <div class=\"card-toolbar\">
        <button class=\"chart-title-btn\">Planned vs Actual Straight-Line Distance (miles)</button>
        <button class=\"chart-expand-btn\" aria-label=\"Expand chart\">⤢</button>
      </div>
      <div id=\"scatter_distance\" class=\"chart-content\" style=\"height:320px;\"></div>
    </div>
    <div class=\"card\">
      <div class=\"card-toolbar\">
        <button class=\"chart-title-btn\">Driver Route Days Across Timeline</button>
        <button class=\"chart-expand-btn\" aria-label=\"Expand chart\">⤢</button>
      </div>
      <div id=\"timeline_driver_days\" class=\"chart-content\" style=\"height:320px;\"></div>
    </div>
    <div class=\"card\">
      <div class=\"card-toolbar\">
        <button class=\"chart-title-btn\">Stops Per Route by Driver Across Timeline</button>
        <button class=\"chart-expand-btn\" aria-label=\"Expand chart\">⤢</button>
      </div>
      <div id=\"timeline_stops_by_driver\" class=\"chart-content\" style=\"height:320px;\"></div>
    </div>
  </section>

  <script>
    const numStops = __NUM_STOPS__;
    const edgesPct = __EDGES_PCT__;

    const startPlanned = __START_PLANNED__;
    const startActual = __START_ACTUAL__;
    const startCustom = __START_CUSTOM__;

    const endPlanned = __END_PLANNED__;
    const endActual = __END_ACTUAL__;
    const endCustom = __END_CUSTOM__;

    const distPlanned = __DIST_PLANNED__;
    const distActual = __DIST_ACTUAL__;
    const distCustom = __DIST_CUSTOM__;
    const driverDayDates = __DRIVER_DAY_DATES__;
    const driverDayNames = __DRIVER_DAY_NAMES__;
    const driverDayCustom = __DRIVER_DAY_CUSTOM__;
    const driverCategories = __DRIVER_CATEGORIES__;
    const driverStopLines = __DRIVER_STOP_LINES__;
    const driverMarkerColor = '#ef4444';
    const driverDayBaseMarkerSize = 9;
    const driverStopsBaseMarkerSize = 6;

    const histStops = [{
      x: numStops,
      type: 'histogram',
      marker: {color: '#5ad2f4'},
      autobinx: true,
      hovertemplate: 'Stops: %{x}<br>Count: %{y}<extra></extra>'
    }];

    Plotly.newPlot('hist_stops', histStops, {
      margin: {t: 10, r: 10, b: 40, l: 40},
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      font: {color: '#e6edf3'},
      xaxis: {title: 'Number of Stops'},
      yaxis: {title: 'Routes'}
    }, {displayModeBar: false});

    const histEdges = [{
      x: edgesPct,
      type: 'histogram',
      marker: {color: '#8bd17c'},
      autobinx: true,
      hovertemplate: 'Percent: %{x:.1f}%<br>Count: %{y}<extra></extra>'
    }];

    Plotly.newPlot('hist_edges', histEdges, {
      margin: {t: 10, r: 10, b: 40, l: 40},
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      font: {color: '#e6edf3'},
      xaxis: {title: 'Edges Executed as Planned (%)'},
      yaxis: {title: 'Routes'}
    }, {displayModeBar: false});

    const timeTicks = [0, 360, 720, 1080, 1440];
    const timeLabels = ['0:00', '6:00', '12:00', '18:00', '24:00'];

    const startTrace = [{
      x: startPlanned,
      y: startActual,
      mode: 'markers',
      type: 'scatter',
      marker: {size: 9, color: '#f8b195', opacity: 0.9},
      customdata: startCustom,
      hovertemplate: 'Route: %{customdata[0]}<br>Planned: %{customdata[1]}<br>Actual: %{customdata[2]}<extra></extra>'
    }];

    const startLine = {
      x: [0, 1440],
      y: [0, 1440],
      mode: 'lines',
      line: {dash: 'dot', color: '#6b7280'},
      hoverinfo: 'skip'
    };

    Plotly.newPlot('scatter_start', startTrace.concat([startLine]), {
      margin: {t: 10, r: 10, b: 50, l: 50},
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      font: {color: '#e6edf3'},
      xaxis: {title: 'Planned Start Time', range: [0, 1440], tickvals: timeTicks, ticktext: timeLabels},
      yaxis: {title: 'Actual Start Time', range: [0, 1440], tickvals: timeTicks, ticktext: timeLabels}
    }, {displayModeBar: false});

    const endTrace = [{
      x: endPlanned,
      y: endActual,
      mode: 'markers',
      type: 'scatter',
      marker: {size: 9, color: '#c77dff', opacity: 0.9},
      customdata: endCustom,
      hovertemplate: 'Route: %{customdata[0]}<br>Planned: %{customdata[1]}<br>Actual: %{customdata[2]}<extra></extra>'
    }];

    const endLine = {
      x: [0, 1440],
      y: [0, 1440],
      mode: 'lines',
      line: {dash: 'dot', color: '#6b7280'},
      hoverinfo: 'skip'
    };

    Plotly.newPlot('scatter_end', endTrace.concat([endLine]), {
      margin: {t: 10, r: 10, b: 50, l: 50},
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      font: {color: '#e6edf3'},
      xaxis: {title: 'Planned End Time', range: [0, 1440], tickvals: timeTicks, ticktext: timeLabels},
      yaxis: {title: 'Actual End Time', range: [0, 1440], tickvals: timeTicks, ticktext: timeLabels}
    }, {displayModeBar: false});

    const distTrace = [{
      x: distPlanned,
      y: distActual,
      mode: 'markers',
      type: 'scatter',
      marker: {size: 9, color: '#4dd599', opacity: 0.9},
      customdata: distCustom,
      hovertemplate: 'Route: %{customdata[0]}<br>Planned: %{customdata[1]} mi<br>Actual: %{customdata[2]} mi<extra></extra>'
    }];

    const distLine = {
      x: [__DIST_MIN__, __DIST_MAX__],
      y: [__DIST_MIN__, __DIST_MAX__],
      mode: 'lines',
      line: {dash: 'dot', color: '#6b7280'},
      hoverinfo: 'skip'
    };

    Plotly.newPlot('scatter_distance', distTrace.concat([distLine]), {
      margin: {t: 10, r: 10, b: 50, l: 50},
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      font: {color: '#e6edf3'},
      xaxis: {title: 'Planned Distance (mi)', range: [__DIST_MIN__, __DIST_MAX__]},
      yaxis: {title: 'Actual Distance (mi)', range: [__DIST_MIN__, __DIST_MAX__]}
    }, {displayModeBar: false});

    Plotly.newPlot('timeline_driver_days', [{
      x: driverDayDates,
      y: driverDayNames,
      mode: 'markers',
      type: 'scatter',
      marker: {size: driverDayBaseMarkerSize, color: driverMarkerColor, opacity: 0.95},
      customdata: driverDayCustom,
      hovertemplate: 'Driver: %{customdata[0]}<br>Date: %{customdata[1]}<br>Stops: %{customdata[2]}<extra></extra>'
    }], {
      margin: {t: 10, r: 10, b: 50, l: 120},
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      font: {color: '#e6edf3'},
      xaxis: {title: 'Route Date', type: 'date'},
      yaxis: {title: 'Driver', type: 'category', categoryorder: 'array', categoryarray: driverCategories}
    }, {displayModeBar: false});

    const stopTraces = driverStopLines.map((series) => ({
      x: series.x,
      y: series.y,
      mode: 'lines+markers',
      type: 'scatter',
      name: series.name,
      line: {width: 2},
      marker: {size: driverStopsBaseMarkerSize, color: driverMarkerColor, opacity: 0.95},
      hovertemplate: `Driver: ${series.name}<br>Date: %{x}<br>Stops: %{y}<extra></extra>`
    }));

    Plotly.newPlot('timeline_stops_by_driver', stopTraces, {
      margin: {t: 10, r: 10, b: 50, l: 60},
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      font: {color: '#e6edf3'},
      xaxis: {title: 'Route Date', type: 'date'},
      yaxis: {title: 'Stops on Route'},
      legend: {orientation: 'h', y: 1.12}
    }, {displayModeBar: false});

    const responsiveMarkerConfig = {
      timeline_driver_days: {
        baseArea: 160000,
        baseSize: driverDayBaseMarkerSize,
        minSize: driverDayBaseMarkerSize,
        maxSize: 22,
        traceIndexes: [0]
      },
      timeline_stops_by_driver: {
        baseArea: 160000,
        baseSize: driverStopsBaseMarkerSize,
        minSize: driverStopsBaseMarkerSize,
        maxSize: 16,
        traceIndexes: stopTraces.map((_, idx) => idx)
      }
    };

    function clamp(value, minValue, maxValue) {
      return Math.max(minValue, Math.min(maxValue, value));
    }

    function updateResponsiveMarkerSizes(chart) {
      if (!chart) return;
      const config = responsiveMarkerConfig[chart.id];
      if (!config || !config.traceIndexes.length) return;
      const width = Math.max(chart.clientWidth || 0, 1);
      const height = Math.max(chart.clientHeight || 0, 1);
      const areaScale = Math.sqrt((width * height) / config.baseArea);
      const markerScale = clamp(areaScale, 1, config.maxSize / config.baseSize);
      const markerSize = Number((config.baseSize * markerScale).toFixed(1));
      Plotly.restyle(
        chart,
        {'marker.size': config.traceIndexes.map(() => markerSize)},
        config.traceIndexes
      );
    }

    const grid = document.querySelector('.grid');
    const cards = Array.from(document.querySelectorAll('.card'));
    let expandedCard = null;

    function resizeCardPlot(card) {
      const chart = card ? card.querySelector('.chart-content') : null;
      if (chart) {
        Plotly.Plots.resize(chart);
        updateResponsiveMarkerSizes(chart);
      }
    }

    updateResponsiveMarkerSizes(document.getElementById('timeline_driver_days'));
    updateResponsiveMarkerSizes(document.getElementById('timeline_stops_by_driver'));

    function setExpandedUI(card, expanded) {
      const expandBtn = card.querySelector('.chart-expand-btn');
      if (!expandBtn) return;
      expandBtn.textContent = expanded ? '✕' : '⤢';
      expandBtn.setAttribute('aria-label', expanded ? 'Collapse chart' : 'Expand chart');
    }

    function collapseCard(card) {
      if (!card) return;
      const chart = card.querySelector('.chart-content');
      const defaultHeight = chart ? chart.dataset.defaultHeight : null;
      card.classList.remove('expanded');
      setExpandedUI(card, false);
      if (chart && defaultHeight) {
        chart.style.height = defaultHeight;
      }
      grid.classList.remove('expanded-active');
      document.body.classList.remove('expanded-active');
      expandedCard = null;
      setTimeout(() => resizeCardPlot(card), 30);
    }

    function expandCard(card) {
      const chart = card.querySelector('.chart-content');
      if (!chart) return;
      if (expandedCard && expandedCard !== card) {
        collapseCard(expandedCard);
      }
      card.classList.add('expanded');
      setExpandedUI(card, true);
      chart.style.height = 'calc(100vh - 130px)';
      grid.classList.add('expanded-active');
      document.body.classList.add('expanded-active');
      expandedCard = card;
      setTimeout(() => resizeCardPlot(card), 30);
    }

    cards.forEach((card) => {
      const chart = card.querySelector('.chart-content');
      const titleBtn = card.querySelector('.chart-title-btn');
      const expandBtn = card.querySelector('.chart-expand-btn');
      if (!chart || !titleBtn || !expandBtn) return;
      chart.dataset.defaultHeight = chart.style.height || '320px';
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
        resizeCardPlot(expandedCard);
      }
    });
  </script>
</body>
</html>
"""
    html = html.replace("__NUM_STOPS__", json.dumps(num_stops))
    html = html.replace("__EDGES_PCT__", json.dumps(edges_pct))

    html = html.replace("__START_PLANNED__", json.dumps(start_planned))
    html = html.replace("__START_ACTUAL__", json.dumps(start_actual))
    html = html.replace("__START_CUSTOM__", json.dumps(start_custom))

    html = html.replace("__END_PLANNED__", json.dumps(end_planned))
    html = html.replace("__END_ACTUAL__", json.dumps(end_actual))
    html = html.replace("__END_CUSTOM__", json.dumps(end_custom))

    html = html.replace("__DIST_PLANNED__", json.dumps(dist_planned))
    html = html.replace("__DIST_ACTUAL__", json.dumps(dist_actual))
    html = html.replace("__DIST_CUSTOM__", json.dumps(dist_custom))
    html = html.replace("__DRIVER_DAY_DATES__", json.dumps(driver_day_dates))
    html = html.replace("__DRIVER_DAY_NAMES__", json.dumps(driver_day_names))
    html = html.replace("__DRIVER_DAY_CUSTOM__", json.dumps(driver_day_custom))
    html = html.replace("__DRIVER_CATEGORIES__", json.dumps(driver_categories))
    html = html.replace("__DRIVER_STOP_LINES__", json.dumps(driver_stop_lines))

    html = html.replace("__DIST_MIN__", f"{dist_range[0]:.4f}")
    html = html.replace("__DIST_MAX__", f"{dist_range[1]:.4f}")

    with open(args.output, "w", encoding="utf-8") as f:
        f.write(html)


if __name__ == "__main__":
    main()
