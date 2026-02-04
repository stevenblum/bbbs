#!/usr/bin/env python3
"""Generate an HTML dashboard for route-level metrics."""

import argparse
import csv
import json
from typing import List, Optional, Tuple


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


def main() -> None:
    parser = argparse.ArgumentParser(description="Create an HTML dashboard for route data.")
    parser.add_argument("--input", default="route_data.csv", help="Input route CSV")
    parser.add_argument("--output", default="route_dashboard.html", help="Output HTML file")
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

    with open(args.input, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            label = f"{row.get('Driver', '').strip()} | {row.get('Route Date', '').strip()}"

            ns = to_int(row.get("number_of_stops"))
            if ns is not None:
                num_stops.append(ns)

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
      background: var(--panel);
      border: 1px solid var(--panel-border);
      border-radius: 12px;
      padding: 12px;
      min-height: 320px;
      box-shadow: 0 10px 30px rgba(0,0,0,0.3);
    }
    .chart-title {
      font-size: 14px;
      color: var(--muted);
      margin: 6px 8px 0 8px;
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
      <div class=\"chart-title\">Histogram: Number of Stops</div>
      <div id=\"hist_stops\" style=\"width:100%;height:280px;\"></div>
    </div>
    <div class=\"card\">
      <div class=\"chart-title\">Histogram: Edges Executed as Planned</div>
      <div id=\"hist_edges\" style=\"width:100%;height:280px;\"></div>
    </div>
    <div class=\"card\">
      <div class=\"chart-title\">Planned vs Actual Start Time</div>
      <div id=\"scatter_start\" style=\"width:100%;height:320px;\"></div>
    </div>
    <div class=\"card\">
      <div class=\"chart-title\">Planned vs Actual End Time</div>
      <div id=\"scatter_end\" style=\"width:100%;height:320px;\"></div>
    </div>
    <div class=\"card\">
      <div class=\"chart-title\">Planned vs Actual Straight-Line Distance (miles)</div>
      <div id=\"scatter_distance\" style=\"width:100%;height:320px;\"></div>
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

    html = html.replace("__DIST_MIN__", f"{dist_range[0]:.4f}")
    html = html.replace("__DIST_MAX__", f"{dist_range[1]:.4f}")

    with open(args.output, "w", encoding="utf-8") as f:
        f.write(html)


if __name__ == "__main__":
    main()
