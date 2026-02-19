#!/usr/bin/env python3
"""Plot date-range driver routes with day slider and autoplay."""

from __future__ import annotations

import argparse
import ast
import html
import json
from colorsys import hsv_to_rgb
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
SAVERS_CSV = SCRIPT_DIR / "persistent_savers_addresses.csv"  # set to None to skip
DEPOT_CSV = SCRIPT_DIR / "persistent_depot_address.csv"  # set to None to disable depot start/end enforcement
BINS_CSV = SCRIPT_DIR / "data_bins.csv"  # set to None to skip
ROUTINE_CSV = SCRIPT_DIR / "data_routine.csv"  # set to None to skip
ACTIVE_BINS_CSV = SCRIPT_DIR / "data_active_bins.csv"  # set to None to skip
ACTIVE_ROUTINE_CSV = SCRIPT_DIR / "data_active_routine.csv"  # set to None to skip
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
COL_ACTUAL_DURATION = "Actual Duration"

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


def build_driver_color_map(drivers: list[str]) -> dict[str, str]:
    unique_drivers = sorted(set(drivers))
    color_map: dict[str, str] = {}

    for idx, driver in enumerate(unique_drivers):
        if idx < len(COLORS):
            color_map[driver] = COLORS[idx]
            continue

        # Generate extra deterministic colors if unique drivers exceed base palette.
        hue = (idx * 0.61803398875) % 1.0
        red, green, blue = hsv_to_rgb(hue, 0.72, 0.92)
        color_map[driver] = f"#{int(red * 255):02x}{int(green * 255):02x}{int(blue * 255):02x}"

    return color_map


def _fmt_address(value: Any) -> str:
    if value is None:
        return "(missing)"
    text = str(value).strip()
    return text if text else "(missing)"


def _is_missing(value: Any) -> bool:
    if value is None:
        return True
    if pd is None:
        return False
    return bool(pd.isna(value))


def _normalize_display_name(value: Any) -> str:
    if _is_missing(value):
        return ""
    return str(value).strip().casefold()


def _parse_list_field(value: Any) -> list[str]:
    if _is_missing(value):
        return []
    text = str(value).strip()
    if not text or text == "[]":
        return []

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        parsed = None

    if parsed is None:
        try:
            parsed = ast.literal_eval(text)
        except (ValueError, SyntaxError):
            parsed = None

    if not isinstance(parsed, list):
        return []

    values: list[str] = []
    for item in parsed:
        norm = _normalize_display_name(item)
        if norm:
            values.append(norm)
    return values


def _parse_bool_field(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"1", "true", "t", "yes", "y"}


def _parse_int_field(value: Any) -> int | None:
    if _is_missing(value):
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def _parse_active_schedule_payload(value: Any) -> dict[str, Any]:
    if _is_missing(value):
        return {}
    text = str(value).strip()
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}
    return {
        "active": _parse_bool_field(parsed.get("active")),
        "stops_in_previous_28": _parse_int_field(parsed.get("stops_in_previous_28")),
        "stops_in_next_28": _parse_int_field(parsed.get("stops_in_next_28")),
        "previous_days_since": _parse_int_field(parsed.get("previous_days_since")),
        "next_days_to": _parse_int_field(parsed.get("next_days_to")),
    }


def load_active_schedule_by_date(
    active_csv: Path | None,
    *,
    entity_label: str,
    normalize_entity_keys: bool,
) -> tuple[dict[str, dict[str, dict[str, Any]]], bool]:
    by_date: dict[str, dict[str, dict[str, Any]]] = {}
    if not active_csv:
        return by_date, False
    if not active_csv.exists():
        print(f"{entity_label} active schedule CSV not found: {active_csv}")
        return by_date, False

    active_df = pd.read_csv(active_csv, dtype=str).fillna("")
    if "date" not in active_df.columns:
        print(f"{entity_label} active schedule CSV missing required column: date")
        return by_date, False
    if active_df.empty:
        return by_date, True

    entity_cols = [col for col in active_df.columns if col != "date"]
    for _, row in active_df.iterrows():
        date_raw = str(row.get("date", "")).strip()
        if not date_raw:
            continue
        parsed_date = pd.to_datetime(date_raw, errors="coerce")
        if pd.isna(parsed_date):
            continue
        date_key = parsed_date.strftime("%Y-%m-%d")

        entity_map: dict[str, dict[str, Any]] = {}
        for col in entity_cols:
            entity_key = _normalize_display_name(col) if normalize_entity_keys else str(col).strip()
            if not entity_key:
                continue
            payload = _parse_active_schedule_payload(row.get(col))
            if payload and payload.get("active"):
                entity_map[entity_key] = payload
        by_date[date_key] = entity_map

    return by_date, True


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


def _parse_duration_minutes(value: Any) -> float:
    if _is_missing(value):
        return 0.0
    text = str(value).strip()
    if not text:
        return 0.0
    if ":" in text:
        parts = text.split(":")
        try:
            if len(parts) == 3:
                hours = float(parts[0])
                minutes = float(parts[1])
                seconds = float(parts[2])
                total_minutes = (hours * 60.0) + minutes + (seconds / 60.0)
                return max(total_minutes, 0.0)
            if len(parts) == 2:
                minutes = float(parts[0])
                seconds = float(parts[1])
                total_minutes = minutes + (seconds / 60.0)
                return max(total_minutes, 0.0)
        except ValueError:
            return 0.0
    try:
        minutes = float(text)
    except ValueError:
        return 0.0
    if minutes < 0:
        return 0.0
    return minutes


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
    lat_col = next((c for c in ("latitude", "latitude_raw") if c in savers_df.columns), None)
    lon_col = next((c for c in ("longitude", "longitude_raw") if c in savers_df.columns), None)
    address_col = next(
        (c for c in ("address", "address_raw", "display_name") if c in savers_df.columns),
        None,
    )
    if not lat_col or not lon_col:
        print(
            "Savers CSV missing latitude/longitude columns. "
            "Expected one of: latitude|latitude_raw and longitude|longitude_raw."
        )
        return points

    savers_df["latitude_norm"] = pd.to_numeric(savers_df[lat_col], errors="coerce")
    savers_df["longitude_norm"] = pd.to_numeric(savers_df[lon_col], errors="coerce")
    savers_df = savers_df.dropna(subset=["latitude_norm", "longitude_norm"])

    for _, row in savers_df.iterrows():
        address_value = row.get(address_col) if address_col else None
        if _is_missing(address_value) and "display_name" in savers_df.columns:
            address_value = row.get("display_name")
        points.append(
            {
                "lat": float(row["latitude_norm"]),
                "lon": float(row["longitude_norm"]),
                "address": html.escape(_fmt_address(address_value)),
            }
        )
    return points


def load_depot_point() -> dict[str, Any] | None:
    if not DEPOT_CSV:
        return None
    if not DEPOT_CSV.exists():
        print(f"Depot CSV not found: {DEPOT_CSV}")
        return None

    depot_df = pd.read_csv(DEPOT_CSV)
    required = {"display_name", "latitude", "longitude"}
    if not required.issubset(depot_df.columns):
        print("Depot CSV missing required columns: display_name, latitude, longitude")
        return None

    depot_df["latitude"] = pd.to_numeric(depot_df["latitude"], errors="coerce")
    depot_df["longitude"] = pd.to_numeric(depot_df["longitude"], errors="coerce")
    depot_df = depot_df.dropna(subset=["display_name", "latitude", "longitude"])
    depot_df["display_name"] = depot_df["display_name"].astype(str).str.strip()
    depot_df = depot_df[depot_df["display_name"] != ""].copy()
    if depot_df.empty:
        print(f"Depot CSV has no valid rows: {DEPOT_CSV}")
        return None
    if len(depot_df) > 1:
        print(f"Depot CSV has multiple rows. Using first row from: {DEPOT_CSV}")

    row = depot_df.iloc[0]
    display_name = _fmt_address(row.get("display_name"))
    return {
        "display_name": display_name,
        "display_name_key": _normalize_display_name(display_name),
        "lat": float(row["latitude"]),
        "lon": float(row["longitude"]),
    }


def make_depot_stop_payload(depot_point: dict[str, Any]) -> dict[str, Any]:
    display_name = _fmt_address(depot_point.get("display_name"))
    return {
        "lat": float(depot_point["lat"]),
        "lon": float(depot_point["lon"]),
        "stop": "DEPOT",
        "raw_address": html.escape(display_name),
        "osm_display_name": html.escape(display_name),
        "display_name_key": _normalize_display_name(display_name),
        "marker_shape": "depot",
        "actual_duration_minutes": 0.0,
        "is_depot": True,
    }


def load_bins_points() -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    if not BINS_CSV:
        return points
    if not BINS_CSV.exists():
        print(f"BINs CSV not found: {BINS_CSV}")
        return points

    bins_df = pd.read_csv(BINS_CSV)
    if bins_df.empty:
        return points

    if "centroid_lat" in bins_df.columns:
        centroid_lat = pd.to_numeric(bins_df["centroid_lat"], errors="coerce")
    else:
        centroid_lat = pd.Series(index=bins_df.index, dtype="float64")
    if "centroid_lon" in bins_df.columns:
        centroid_lon = pd.to_numeric(bins_df["centroid_lon"], errors="coerce")
    else:
        centroid_lon = pd.Series(index=bins_df.index, dtype="float64")
    if "primary_lat" in bins_df.columns:
        primary_lat = pd.to_numeric(bins_df["primary_lat"], errors="coerce")
    else:
        primary_lat = pd.Series(index=bins_df.index, dtype="float64")
    if "primary_lon" in bins_df.columns:
        primary_lon = pd.to_numeric(bins_df["primary_lon"], errors="coerce")
    else:
        primary_lon = pd.Series(index=bins_df.index, dtype="float64")

    bins_df["bin_lat"] = centroid_lat.fillna(primary_lat)
    bins_df["bin_lon"] = centroid_lon.fillna(primary_lon)
    bins_df = bins_df.dropna(subset=["bin_lat", "bin_lon"]).copy()

    if "associated_stop_count" in bins_df.columns:
        bins_df["associated_stop_count"] = pd.to_numeric(
            bins_df["associated_stop_count"], errors="coerce"
        ).fillna(0)
    else:
        bins_df["associated_stop_count"] = 0

    bins_df = bins_df.sort_values(
        ["associated_stop_count", "bin_id"],
        ascending=[False, True],
    )

    for _, row in bins_df.iterrows():
        points.append(
            {
                "lat": float(row["bin_lat"]),
                "lon": float(row["bin_lon"]),
                "bin_id": _fmt_address(row.get("bin_id")),
                "location_name": _fmt_address(row.get("location_name_primary")),
                "primary_display_name": _fmt_address(row.get("primary_display_name")),
                "visit_count": int(float(row.get("associated_stop_count", 0) or 0)),
            }
        )
    return points


def load_routine_points() -> list[dict[str, Any]]:
    points: list[dict[str, Any]] = []
    if not ROUTINE_CSV:
        return points
    if not ROUTINE_CSV.exists():
        print(f"Routine CSV not found: {ROUTINE_CSV}")
        return points

    routine_df = pd.read_csv(ROUTINE_CSV)
    required = {"display_name_final", "lat", "lon"}
    if not required.issubset(routine_df.columns):
        print("Routine CSV missing required columns: display_name_final, lat, lon")
        return points

    routine_df["lat"] = pd.to_numeric(routine_df["lat"], errors="coerce")
    routine_df["lon"] = pd.to_numeric(routine_df["lon"], errors="coerce")
    routine_df = routine_df.dropna(subset=["lat", "lon"]).copy()

    if "total_stop_count" in routine_df.columns:
        routine_df["total_stop_count"] = pd.to_numeric(
            routine_df["total_stop_count"], errors="coerce"
        ).fillna(0)
    else:
        routine_df["total_stop_count"] = 0

    routine_df = routine_df.sort_values(
        ["total_stop_count", "display_name_final"],
        ascending=[False, True],
    )

    for _, row in routine_df.iterrows():
        display_name = _fmt_address(row.get("display_name_final"))
        points.append(
            {
                "lat": float(row["lat"]),
                "lon": float(row["lon"]),
                "display_name": display_name,
                "display_name_key": _normalize_display_name(display_name),
                "visit_count": int(float(row.get("total_stop_count", 0) or 0)),
            }
        )
    return points


def load_bins_display_name_to_bin_id_map() -> dict[str, str]:
    name_to_bin_id: dict[str, str] = {}
    if not BINS_CSV or not BINS_CSV.exists():
        return name_to_bin_id

    bins_df = pd.read_csv(BINS_CSV)
    if bins_df.empty:
        return name_to_bin_id

    for _, row in bins_df.iterrows():
        bin_id = str(row.get("bin_id", "")).strip()
        if not bin_id:
            continue

        primary_norm = _normalize_display_name(row.get("primary_display_name"))
        if primary_norm and primary_norm not in name_to_bin_id:
            name_to_bin_id[primary_norm] = bin_id
        for col_name in (
            "all_grouped_display_names",
            "seed_display_names",
            "distance_display_names",
            "other_display_names",
        ):
            for parsed_name in _parse_list_field(row.get(col_name)):
                if parsed_name not in name_to_bin_id:
                    name_to_bin_id[parsed_name] = bin_id

    return name_to_bin_id


def load_routine_display_name_set() -> set[str]:
    names: set[str] = set()
    if not ROUTINE_CSV or not ROUTINE_CSV.exists():
        return names

    routine_df = pd.read_csv(ROUTINE_CSV)
    if routine_df.empty or "display_name_final" not in routine_df.columns:
        return names

    for value in routine_df["display_name_final"]:
        norm = _normalize_display_name(value)
        if norm:
            names.add(norm)
    return names


def resolve_center(
    valid_stop_df: pd.DataFrame,
    savers_points: list[dict[str, Any]],
    bins_points: list[dict[str, Any]],
    routine_points: list[dict[str, Any]],
) -> tuple[float, float]:
    if not valid_stop_df.empty:
        return float(valid_stop_df[COL_LAT].mean()), float(valid_stop_df[COL_LON].mean())

    fallback_points = [*savers_points, *bins_points, *routine_points]
    if fallback_points:
        center_lat = sum(float(p["lat"]) for p in fallback_points) / len(fallback_points)
        center_lon = sum(float(p["lon"]) for p in fallback_points) / len(fallback_points)
        return center_lat, center_lon

    return 41.7, -71.5


def build_html(
    day_payloads: list[dict[str, Any]],
    savers_points: list[dict[str, Any]],
    depot_point: dict[str, Any] | None,
    bins_points: list[dict[str, Any]],
    routine_points: list[dict[str, Any]],
    center_lat: float,
    center_lon: float,
    range_start: pd.Timestamp,
    range_end: pd.Timestamp,
) -> str:
    days_json = json.dumps(day_payloads)
    savers_json = json.dumps(savers_points)
    depot_json = json.dumps(depot_point)
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
      padding-bottom: 30px;
    }}
    #summary-extra {{
      display: none;
    }}
    #summary.expanded #summary-extra {{
      display: block;
    }}
    #summary-toggle {{
      position: absolute;
      right: 8px;
      bottom: 6px;
      border: 1px solid #9ca3af;
      background: #e5e7eb;
      color: #374151;
      border-radius: 4px;
      font-size: 10px;
      line-height: 1;
      padding: 2px 6px;
      cursor: pointer;
    }}
    #summary-toggle:hover {{
      background: #d1d5db;
    }}
    #driver-legend {{
      left: 14px;
      bottom: 14px;
      min-width: 320px;
      max-width: 560px;
      max-height: 44vh;
      overflow-y: auto;
    }}
    #legend-content table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 11px;
    }}
    #legend-content th {{
      text-align: left;
      padding: 3px 4px;
      border-bottom: 1px solid #d1d5db;
      background: #f8fafc;
      font-weight: 700;
    }}
    #legend-content td {{
      padding: 3px 4px;
      border-bottom: 1px solid #e5e7eb;
      vertical-align: top;
    }}
    .legend-driver {{
      display: flex;
      align-items: center;
      gap: 6px;
      white-space: nowrap;
    }}
    .legend-chip {{
      width: 10px;
      height: 10px;
      border-radius: 50%;
      display: inline-block;
      flex: 0 0 auto;
    }}
    .legend-note {{
      font-size: 10px;
      color: #92400e;
      margin-top: 2px;
      line-height: 1.2;
    }}
    #overlay-legend {{
      right: 14px;
      bottom: 14px;
      min-width: 220px;
      max-width: 280px;
    }}
    #date-control {{
      position: fixed;
      top: 14px;
      right: 14px;
      z-index: 9999;
      background: rgba(255, 255, 255, 0.96);
      border: 1px solid #ccc;
      border-radius: 8px;
      box-shadow: 0 1px 8px rgba(0, 0, 0, 0.16);
      padding: 10px 12px;
      font-size: 12px;
      width: min(180px, calc(100vw - 28px));
      box-sizing: border-box;
    }}
    #play-control {{
      position: fixed;
      top: 14px;
      left: 50%;
      transform: translateX(-50%);
      z-index: 9999;
      background: rgba(255, 255, 255, 0.96);
      border: 1px solid #ccc;
      border-radius: 8px;
      box-shadow: 0 1px 8px rgba(0, 0, 0, 0.16);
      padding: 6px 10px;
      font-size: 12px;
      display: flex;
      align-items: center;
      gap: 8px;
      box-sizing: border-box;
    }}
    #play-control .control-row {{
      display: flex;
      align-items: center;
      gap: 8px;
    }}
    #play-toggle {{
      padding: 3px 10px;
      font-size: 12px;
      cursor: pointer;
    }}
    #speed-select {{
      padding: 2px 4px;
      font-size: 12px;
    }}
    .calendar-header {{
      display: grid;
      grid-template-columns: 14px 1fr 14px;
      align-items: center;
      gap: 6px;
      margin-bottom: 6px;
    }}
    #calendar-month-label {{
      text-align: center;
      font-weight: 700;
      font-size: 13px;
    }}
    .calendar-nav-btn {{
      height: 13px;
      border: 1px solid #ccc;
      border-radius: 6px;
      background: #fff;
      font-size: 10px;
      padding: 0;
      line-height: 1;
      cursor: pointer;
    }}
    .calendar-nav-btn:disabled {{
      background: #f3f4f6;
      color: #9ca3af;
      cursor: not-allowed;
    }}
    .calendar-weekdays,
    .calendar-grid {{
      display: grid;
      grid-template-columns: repeat(7, minmax(0, 1fr));
      gap: 4px;
    }}
    .calendar-weekday {{
      text-align: center;
      font-size: 10px;
      color: #666;
      font-weight: 700;
      text-transform: uppercase;
      padding: 2px 0;
    }}
    .calendar-day-spacer {{
      height: 18px;
    }}
    .calendar-day {{
      height: 18px;
      border-radius: 6px;
      border: 1px solid #d1d5db;
      font-size: 10px;
      padding: 0;
      line-height: 1;
      background: #fff;
      color: #111827;
      cursor: pointer;
    }}
    .calendar-day.available {{
      border-color: #000;
    }}
    .calendar-day.available:hover {{
      border-color: #000;
      background: #eff6ff;
    }}
    .calendar-day.selected {{
      background: #2563eb;
      border-color: #000;
      color: #fff;
      font-weight: 700;
    }}
    .calendar-day.unavailable {{
      background: #f3f4f6;
      color: #9ca3af;
      border-color: #e5e7eb;
      cursor: not-allowed;
    }}
    @media (max-width: 700px) {{
      #date-control {{
        top: 10px;
        right: 10px;
        width: min(170px, calc(100vw - 20px));
      }}
      #play-control {{
        top: 10px;
      }}
    }}
  </style>
</head>
<body>
  <div id="play-control">
    <div class="control-row">
      <button id="play-toggle" type="button">Play</button>
      <label for="speed-select">Delay</label>
      <select id="speed-select">
        <option value="1" selected>1s</option>
        <option value="3">3s</option>
        <option value="5">5s</option>
        <option value="10">10s</option>
      </select>
    </div>
  </div>

  <div id="date-control">
    <div class="calendar-header">
      <button id="month-prev" type="button" class="calendar-nav-btn" aria-label="Previous month">‹</button>
      <div id="calendar-month-label">-</div>
      <button id="month-next" type="button" class="calendar-nav-btn" aria-label="Next month">›</button>
    </div>
    <div class="calendar-weekdays">
      <div class="calendar-weekday">Sun</div>
      <div class="calendar-weekday">Mon</div>
      <div class="calendar-weekday">Tue</div>
      <div class="calendar-weekday">Wed</div>
      <div class="calendar-weekday">Thu</div>
      <div class="calendar-weekday">Fri</div>
      <div class="calendar-weekday">Sat</div>
    </div>
    <div id="calendar-grid" class="calendar-grid"></div>
  </div>

  <div id="summary" class="panel">
    <div><strong>Range:</strong> {range_text}</div>
    <div><strong>Date:</strong> <span id="summary-date">-</span></div>
    <div><strong>Total Distance:</strong> <span id="summary-distance">-</span></div>
    <div><strong>Total Duration (drive + stops):</strong> <span id="summary-duration">-</span></div>
    <div><strong>Routes:</strong> <span id="summary-routes">-</span></div>
    <div id="summary-extra">
      <div><strong>Missing Coords:</strong> <span id="summary-missing">-</span></div>
      <div><strong>Savers Locations:</strong> {len(savers_points)}</div>
      <div><strong>Configured BIN Locations:</strong> {len(bins_points)}</div>
      <div><strong>Configured Routine Locations:</strong> {len(routine_points)}</div>
      <div><strong>Active BIN Locations (square):</strong> <span id="summary-active-bins">-</span></div>
      <div><strong>Active Routine Locations (triangle):</strong> <span id="summary-active-routine">-</span></div>
    </div>
    <button id="summary-toggle" type="button" aria-expanded="false" aria-controls="summary-extra">More</button>
  </div>

  <div id="driver-legend" class="panel">
    <div style="margin-bottom:8px;"><strong>Route Legend (Driver)</strong></div>
    <div id="legend-content"></div>
  </div>
  <div id="overlay-legend" class="panel">
    <div style="margin-bottom:8px;"><strong>Location Markers</strong></div>
    <div style="margin-bottom:6px;">
      <span style="display:inline-flex;width:16px;height:16px;border-radius:50%;background:#d62728;color:#fff;align-items:center;justify-content:center;font-size:10px;font-weight:700;border:1px solid #b51f1f;margin-right:6px;vertical-align:middle;">D</span>
      Depot
    </div>
    <div style="margin-bottom:6px;">
      <span style="display:inline-block;width:10px;height:10px;background:#9ca3af;border:1px solid #6b7280;margin-right:6px;vertical-align:middle;"></span>
      Active BIN location (square)
    </div>
    <div style="margin-bottom:6px;">
      <span style="display:inline-block;width:0;height:0;border-left:5px solid transparent;border-right:5px solid transparent;border-bottom:9px solid #9ca3af;margin-right:6px;vertical-align:middle;"></span>
      Active routine location (triangle)
    </div>
    <div>
      <span style="display:inline-block;width:10px;height:10px;border-radius:50%;background:#d62728;border:1px solid #b51f1f;margin-right:6px;vertical-align:middle;"></span>
      Savers location
    </div>
  </div>

  <div id="map"></div>

  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" integrity="sha256-20nQCchB9co0qIjJZRGuk2/Z9VM+kNiyxNV1lvTlZBo=" crossorigin=""></script>
  <script>
    const center = [{center_lat}, {center_lon}];
    const dayData = {days_json};
    const saversPoints = {savers_json};
    const depotPoint = {depot_json};
    const map = L.map('map').setView(center, 10);
    map.createPane('backgroundMarkers');
    map.getPane('backgroundMarkers').style.zIndex = '350';

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

    function metricText(value) {{
      if (value === null || value === undefined || value === '') {{
        return 'n/a';
      }}
      return String(value);
    }}

    function minutesText(value) {{
      if (value === null || value === undefined || value === '') {{
        return 'n/a';
      }}
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) {{
        return 'n/a';
      }}
      const rounded = Math.round(numeric * 10) / 10;
      if (Math.abs(rounded - Math.round(rounded)) < 1e-9) {{
        return String(Math.round(rounded));
      }}
      return rounded.toFixed(1);
    }}

    function stopEntityDetailHtml(stop) {{
      if (!stop || !stop.entity_kind) {{
        return '';
      }}

      if (stop.entity_kind === 'BIN') {{
        return (
          "<br><span style='font-weight:700;text-decoration:underline;'>BIN Context</span>" +
          "<br><strong>ID:</strong> " + escapeHtml(stop.entity_id || '') +
          "<br><strong>Name:</strong> " + escapeHtml(stop.entity_name || '') +
          "<br><strong>Primary Display:</strong> " + escapeHtml(stop.entity_display_name || '') +
          "<br><strong>Total Visits:</strong> " + metricText(stop.entity_visit_count) +
          "<br><strong>Stops In Prev 28 Days:</strong> " + metricText(stop.entity_stops_in_previous_28) +
          "<br><strong>Days Since Last Stop:</strong> " + metricText(stop.entity_previous_days_since) +
          "<br><strong>Days To Next Stop:</strong> " + metricText(stop.entity_next_days_to) +
          "<br><strong>Stops In Next 28 Days:</strong> " + metricText(stop.entity_stops_in_next_28)
        );
      }}

      if (stop.entity_kind === 'Routine') {{
        return (
          "<br><span style='font-weight:700;text-decoration:underline;'>Routine Context</span>" +
          "<br><strong>Display Name:</strong> " + escapeHtml(stop.entity_display_name || '') +
          "<br><strong>Total Visits:</strong> " + metricText(stop.entity_visit_count) +
          "<br><strong>Stops In Prev 28 Days:</strong> " + metricText(stop.entity_stops_in_previous_28) +
          "<br><strong>Days Since Last Stop:</strong> " + metricText(stop.entity_previous_days_since) +
          "<br><strong>Days To Next Stop:</strong> " + metricText(stop.entity_next_days_to) +
          "<br><strong>Stops In Next 28 Days:</strong> " + metricText(stop.entity_stops_in_next_28)
        );
      }}

      return '';
    }}

    function stopPopupHtml(driver, stop, markerNote) {{
      const noteHtml = markerNote
        ? "<br><span style='font-size:11px;color:#555;'>" + markerNote + "</span>"
        : "";
      const entityHtml = stopEntityDetailHtml(stop);
      return (
        "<strong>" + escapeHtml(driver) + "</strong> | " +
        "<strong>Stop " + stop.stop + "</strong>" +
        "<br><strong>Actual Stop Duration:</strong> " + minutesText(stop.actual_duration_minutes) + " min" +
        "<br><span style='font-weight:700;text-decoration:underline;'>Raw Address</span>: " + stop.raw_address +
        "<br><span style='font-weight:700;text-decoration:underline;'>OSM Display Name</span>: " + stop.osm_display_name +
        entityHtml +
        noteHtml
      );
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

    function depotIcon() {{
      return L.divIcon({{
        className: '',
        iconSize: [22, 22],
        iconAnchor: [11, 11],
        html: '<div style="width:22px;height:22px;border-radius:50%;background:#d62728;color:#fff;font-weight:700;display:flex;align-items:center;justify-content:center;border:2px solid #fff;box-shadow:0 0 0 1px #d62728;font-size:12px;">D</div>'
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

    function binSquareIcon() {{
      return L.divIcon({{
        className: '',
        iconSize: [11, 11],
        iconAnchor: [6, 6],
        html: '<div style="width:9px;height:9px;background:#9ca3af;border:1px solid #6b7280;"></div>'
      }});
    }}

    function routineTriangleIcon() {{
      return L.divIcon({{
        className: '',
        iconSize: [13, 13],
        iconAnchor: [7, 12],
        html: '<div style="width:0;height:0;border-left:6px solid transparent;border-right:6px solid transparent;border-bottom:11px solid #6b7280;position:relative;"><div style="position:absolute;left:-5px;top:2px;width:0;height:0;border-left:5px solid transparent;border-right:5px solid transparent;border-bottom:9px solid #9ca3af;"></div></div>'
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

    function stopSquareIcon(color) {{
      return L.divIcon({{
        className: '',
        iconSize: [12, 12],
        iconAnchor: [6, 6],
        html: '<div style="width:12px;height:12px;border:2px solid ' + color + ';background:#000;box-sizing:border-box;"></div>'
      }});
    }}

    function stopTriangleIcon(color) {{
      return L.divIcon({{
        className: '',
        iconSize: [14, 12],
        iconAnchor: [7, 11],
        html: '<div style="width:0;height:0;border-left:7px solid transparent;border-right:7px solid transparent;border-bottom:12px solid ' + color + ';position:relative;"><div style="position:absolute;left:-5px;top:3px;width:0;height:0;border-left:5px solid transparent;border-right:5px solid transparent;border-bottom:8px solid #000;"></div></div>'
      }});
    }}

    const bounds = [];

    if (depotPoint && depotPoint.lat !== undefined && depotPoint.lon !== undefined) {{
      L.marker([depotPoint.lat, depotPoint.lon], {{ icon: depotIcon() }}).addTo(map).bindPopup(
        '<strong>Depot</strong><br>' + escapeHtml(depotPoint.display_name || '')
      );
      bounds.push([depotPoint.lat, depotPoint.lon]);
    }}

    for (const s of saversPoints) {{
      L.marker([s.lat, s.lon], {{ icon: saversIcon() }}).addTo(map).bindPopup(
        'Savers Drop-off<br>' + s.address
      );
      bounds.push([s.lat, s.lon]);
    }}

    const dateLayers = {{}};
    for (const day of dayData) {{
      const layer = L.layerGroup();
      for (const b of (day.active_bins || [])) {{
        L.marker([b.lat, b.lon], {{ icon: binSquareIcon(), pane: 'backgroundMarkers' }}).addTo(layer).bindPopup(
          '<strong>BIN</strong>' +
          '<br><strong>ID:</strong> ' + escapeHtml(b.bin_id) +
          '<br><strong>Name:</strong> ' + escapeHtml(b.location_name) +
          '<br><strong>Primary Display:</strong> ' + escapeHtml(b.primary_display_name) +
          '<br><strong>Total Visits:</strong> ' + String(b.visit_count) +
          '<br><strong>Stops In Prev 28 Days:</strong> ' + metricText(b.stops_in_previous_28) +
          '<br><strong>Days Since Last Stop:</strong> ' + metricText(b.previous_days_since) +
          '<br><strong>Days To Next Stop:</strong> ' + metricText(b.next_days_to) +
          '<br><strong>Stops In Next 28 Days:</strong> ' + metricText(b.stops_in_next_28)
        );
        bounds.push([b.lat, b.lon]);
      }}

      for (const r of (day.active_routine || [])) {{
        L.marker([r.lat, r.lon], {{ icon: routineTriangleIcon(), pane: 'backgroundMarkers' }}).addTo(layer).bindPopup(
          '<strong>Routine</strong>' +
          '<br><strong>Display Name:</strong> ' + escapeHtml(r.display_name) +
          '<br><strong>Total Visits:</strong> ' + String(r.visit_count) +
          '<br><strong>Stops In Prev 28 Days:</strong> ' + metricText(r.stops_in_previous_28) +
          '<br><strong>Days Since Last Stop:</strong> ' + metricText(r.previous_days_since) +
          '<br><strong>Days To Next Stop:</strong> ' + metricText(r.next_days_to) +
          '<br><strong>Stops In Next 28 Days:</strong> ' + metricText(r.stops_in_next_28)
        );
        bounds.push([r.lat, r.lon]);
      }}

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
          if (stop.is_depot) {{
            continue;
          }}
          let stopIcon = stopDotIcon(route.color);
          if (stop.marker_shape === 'bin') {{
            stopIcon = stopSquareIcon(route.color);
          }} else if (stop.marker_shape === 'routine') {{
            stopIcon = stopTriangleIcon(route.color);
          }}
          L.marker([stop.lat, stop.lon], {{
            icon: stopIcon
          }}).addTo(layer).bindPopup(stopPopupHtml(route.driver, stop, ""));
        }}

        if (route.first_stop) {{
          L.marker([route.first_stop.lat, route.first_stop.lon], {{
            icon: firstStopIcon(route.color)
          }}).addTo(layer).bindPopup(stopPopupHtml(route.driver, route.first_stop, "First stop after depot"));
          bounds.push([route.first_stop.lat, route.first_stop.lon]);
        }}

        if (route.last_stop) {{
          L.marker([route.last_stop.lat, route.last_stop.lon], {{
            icon: lastStopIcon(route.color)
          }}).addTo(layer).bindPopup(stopPopupHtml(route.driver, route.last_stop, "Last stop before depot"));
          bounds.push([route.last_stop.lat, route.last_stop.lon]);
        }}
      }}
      dateLayers[day.date] = layer;
    }}

    const playBtn = document.getElementById('play-toggle');
    const speedSelect = document.getElementById('speed-select');
    const monthPrevBtn = document.getElementById('month-prev');
    const monthNextBtn = document.getElementById('month-next');
    const monthLabel = document.getElementById('calendar-month-label');
    const calendarGrid = document.getElementById('calendar-grid');
    const legendContent = document.getElementById('legend-content');
    const summaryPanel = document.getElementById('summary');
    const summaryToggle = document.getElementById('summary-toggle');
    const summaryDate = document.getElementById('summary-date');
    const summaryDistance = document.getElementById('summary-distance');
    const summaryDuration = document.getElementById('summary-duration');
    const summaryRoutes = document.getElementById('summary-routes');
    const summaryMissing = document.getElementById('summary-missing');
    const summaryActiveBins = document.getElementById('summary-active-bins');
    const summaryActiveRoutine = document.getElementById('summary-active-routine');

    const routeDates = dayData.map((day) => day.date);
    const dateToIndex = new Map(routeDates.map((date, idx) => [date, idx]));
    const monthNames = [
      'January', 'February', 'March', 'April', 'May', 'June',
      'July', 'August', 'September', 'October', 'November', 'December'
    ];
    const routeMonthIndexes = routeDates
      .map((dateStr) => parseIsoDateParts(dateStr))
      .filter((parts) => parts !== null)
      .map((parts) => (parts.year * 12) + (parts.month - 1));
    const minMonthIndex = routeMonthIndexes.length > 0 ? Math.min(...routeMonthIndexes) : null;
    const maxMonthIndex = routeMonthIndexes.length > 0 ? Math.max(...routeMonthIndexes) : null;

    let timer = null;
    let playing = false;
    let selectedIndex = 0;
    let currentMonthIndex = minMonthIndex;

    function setSummaryExpanded(expanded) {{
      if (!summaryPanel || !summaryToggle) {{
        return;
      }}
      summaryPanel.classList.toggle('expanded', expanded);
      summaryToggle.textContent = expanded ? 'Less' : 'More';
      summaryToggle.setAttribute('aria-expanded', expanded ? 'true' : 'false');
    }}

    function parseIsoDateParts(dateStr) {{
      const pieces = String(dateStr || '').split('-');
      if (pieces.length !== 3) {{
        return null;
      }}
      const year = parseInt(pieces[0], 10);
      const month = parseInt(pieces[1], 10);
      const day = parseInt(pieces[2], 10);
      if (Number.isNaN(year) || Number.isNaN(month) || Number.isNaN(day)) {{
        return null;
      }}
      return {{ year: year, month: month, day: day }};
    }}

    function monthIndexToLabel(monthIndex) {{
      const year = Math.floor(monthIndex / 12);
      const monthZero = monthIndex % 12;
      return monthNames[monthZero] + ' ' + String(year);
    }}

    function daysInMonth(year, month) {{
      return new Date(Date.UTC(year, month, 0)).getUTCDate();
    }}

    function firstWeekdayOfMonth(year, month) {{
      return new Date(Date.UTC(year, month - 1, 1)).getUTCDay();
    }}

    function toIsoDate(year, month, day) {{
      const mm = String(month).padStart(2, '0');
      const dd = String(day).padStart(2, '0');
      return String(year) + '-' + mm + '-' + dd;
    }}

    function renderCalendar() {{
      if (currentMonthIndex === null || minMonthIndex === null || maxMonthIndex === null) {{
        monthLabel.textContent = 'No route dates';
        calendarGrid.innerHTML = '';
        monthPrevBtn.disabled = true;
        monthNextBtn.disabled = true;
        return;
      }}

      monthLabel.textContent = monthIndexToLabel(currentMonthIndex);
      monthPrevBtn.disabled = currentMonthIndex <= minMonthIndex;
      monthNextBtn.disabled = currentMonthIndex >= maxMonthIndex;

      const year = Math.floor(currentMonthIndex / 12);
      const month = (currentMonthIndex % 12) + 1;
      const leadingSpacers = firstWeekdayOfMonth(year, month);
      const totalDays = daysInMonth(year, month);
      const cells = [];

      for (let i = 0; i < leadingSpacers; i += 1) {{
        cells.push('<div class="calendar-day-spacer" aria-hidden="true"></div>');
      }}

      for (let day = 1; day <= totalDays; day += 1) {{
        const iso = toIsoDate(year, month, day);
        const routeIdx = dateToIndex.get(iso);
        if (routeIdx === undefined) {{
          cells.push(
            '<button type="button" class="calendar-day unavailable" disabled aria-disabled="true">' +
            String(day) +
            '</button>'
          );
          continue;
        }}

        const selectedClass = routeIdx === selectedIndex ? ' selected' : '';
        const ariaPressed = routeIdx === selectedIndex ? 'true' : 'false';
        cells.push(
          '<button type="button" class="calendar-day available' + selectedClass +
          '" data-route-idx="' + String(routeIdx) +
          '" aria-pressed="' + ariaPressed + '">' +
          String(day) +
          '</button>'
        );
      }}

      calendarGrid.innerHTML = cells.join('');
    }}

    function renderLegend(day) {{
      if (!day || !day.routes || day.routes.length === 0) {{
        legendContent.innerHTML = '<div>No routes for this date.</div>';
        return;
      }}

      const rows = day.routes.map((route) => {{
        const osrmTag = route.osrm_error
          ? (route.straight_fallback_used ? 'Straight-line fallback' : 'OSRM route unavailable')
          : '';
        return (
          "<tr>" +
          "<td>" +
            "<div class='legend-driver'>" +
              "<span class='legend-chip' style='background:" + route.color + ";'></span>" +
              "<strong>" + escapeHtml(route.driver) + "</strong>" +
            "</div>" +
            (osrmTag ? "<div class='legend-note'>" + escapeHtml(osrmTag) + "</div>" : "") +
          "</td>" +
          "<td>" + String(route.stops_total) + "</td>" +
          "<td>" + String(route.missing_stops) + "</td>" +
          "<td>" + escapeHtml(route.distance_text) + "</td>" +
          "<td>" + escapeHtml(route.drive_duration_text) + "</td>" +
          "<td>" + escapeHtml(route.total_duration_text) + "</td>" +
          "</tr>"
        );
      }});
      const totalStops = day.routes.reduce(
        (sum, route) => sum + (Number(route.stops_total) || 0),
        0
      );
      const totalMissing = day.routes.reduce(
        (sum, route) => sum + (Number(route.missing_stops) || 0),
        0
      );
      const totalRow =
        "<tr style='font-weight:700;background:#f8fafc;'>" +
        "<td>Total</td>" +
        "<td>" + String(totalStops) + "</td>" +
        "<td>" + String(totalMissing) + "</td>" +
        "<td>" + escapeHtml(day.total_distance_text || 'n/a') + "</td>" +
        "<td>" + escapeHtml(day.total_drive_duration_text || 'n/a') + "</td>" +
        "<td>" + escapeHtml(day.total_duration_text || 'n/a') + "</td>" +
        "</tr>";
      legendContent.innerHTML =
        "<table>" +
        "<thead><tr><th>Driver</th><th>Stops</th><th>Missing</th><th>km</th><th>drive</th><th>total</th></tr></thead>" +
        "<tbody>" + rows.join('') + totalRow + "</tbody>" +
        "</table>";
    }}

    function renderSummary(day) {{
      summaryDate.textContent = day.date;
      summaryDistance.textContent = day.total_distance_text;
      summaryDuration.textContent = day.total_duration_text;
      summaryRoutes.textContent = String(day.routes_count);
      summaryMissing.textContent = String(day.missing_total);
      summaryActiveBins.textContent = String(day.active_bins_count || 0);
      summaryActiveRoutine.textContent = String(day.active_routine_count || 0);
    }}

    function showDateByIndex(idx) {{
      if (dayData.length === 0) {{
        return;
      }}
      const clamped = Math.max(0, Math.min(dayData.length - 1, idx));
      const selected = dayData[clamped];
      selectedIndex = clamped;

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

      const selectedParts = parseIsoDateParts(selected.date);
      if (selectedParts) {{
        currentMonthIndex = (selectedParts.year * 12) + (selectedParts.month - 1);
      }}
      renderCalendar();
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
        const next = (selectedIndex + 1) % dayData.length;
        showDateByIndex(next);
      }}, delaySec * 1000);
    }}

    calendarGrid.addEventListener('click', (e) => {{
      const target = e.target.closest('button.calendar-day.available');
      if (!target) {{
        return;
      }}
      const idx = parseInt(target.dataset.routeIdx || '', 10);
      if (Number.isNaN(idx)) {{
        return;
      }}
      stopPlayback();
      showDateByIndex(idx);
    }});

    monthPrevBtn.addEventListener('click', () => {{
      if (currentMonthIndex === null || minMonthIndex === null) {{
        return;
      }}
      currentMonthIndex = Math.max(minMonthIndex, currentMonthIndex - 1);
      renderCalendar();
    }});

    monthNextBtn.addEventListener('click', () => {{
      if (currentMonthIndex === null || maxMonthIndex === null) {{
        return;
      }}
      currentMonthIndex = Math.min(maxMonthIndex, currentMonthIndex + 1);
      renderCalendar();
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

    if (summaryToggle) {{
      summaryToggle.addEventListener('click', () => {{
        const expanded = summaryPanel ? summaryPanel.classList.contains('expanded') : false;
        setSummaryExpanded(!expanded);
      }});
    }}

    if (bounds.length > 0) {{
      map.fitBounds(bounds, {{ padding: [24, 24] }});
    }}

    setSummaryExpanded(false);
    renderCalendar();
    if (dayData.length === 0) {{
      playBtn.disabled = true;
      speedSelect.disabled = true;
      legendContent.innerHTML = '<div>No routes for selected range.</div>';
    }} else {{
      showDateByIndex(0);
    }}
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
    global BINS_CSV
    global ROUTINE_CSV
    global ACTIVE_BINS_CSV
    global ACTIVE_ROUTINE_CSV
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
        "--bins-csv",
        default=str(BINS_CSV),
        help="Optional BIN locations CSV path",
    )
    parser.add_argument(
        "--routine-csv",
        default=str(ROUTINE_CSV),
        help="Optional routine locations CSV path",
    )
    parser.add_argument(
        "--active-bins-csv",
        default=str(ACTIVE_BINS_CSV),
        help="Optional active BIN schedule CSV path",
    )
    parser.add_argument(
        "--active-routine-csv",
        default=str(ACTIVE_ROUTINE_CSV),
        help="Optional active routine schedule CSV path",
    )
    parser.add_argument(
        "--no-bins",
        action="store_true",
        help="Disable BIN overlay markers",
    )
    parser.add_argument(
        "--no-routine",
        action="store_true",
        help="Disable routine overlay markers",
    )
    parser.add_argument(
        "--no-active-bins",
        action="store_true",
        help="Disable active BIN schedule filtering",
    )
    parser.add_argument(
        "--no-active-routine",
        action="store_true",
        help="Disable active routine schedule filtering",
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
    if args.no_bins or not str(args.bins_csv).strip():
        BINS_CSV = None
    else:
        BINS_CSV = Path(args.bins_csv).expanduser()
    if args.no_routine or not str(args.routine_csv).strip():
        ROUTINE_CSV = None
    else:
        ROUTINE_CSV = Path(args.routine_csv).expanduser()
    if args.no_active_bins or args.no_bins or not str(args.active_bins_csv).strip():
        ACTIVE_BINS_CSV = None
    else:
        ACTIVE_BINS_CSV = Path(args.active_bins_csv).expanduser()
    if args.no_active_routine or args.no_routine or not str(args.active_routine_csv).strip():
        ACTIVE_ROUTINE_CSV = None
    else:
        ACTIVE_ROUTINE_CSV = Path(args.active_routine_csv).expanduser()

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

    savers_points = load_savers_points()
    depot_point = load_depot_point()
    bins_points = load_bins_points()
    routine_points = load_routine_points()
    bins_display_name_to_bin_id = load_bins_display_name_to_bin_id_map()
    bins_display_name_set = set(bins_display_name_to_bin_id.keys())
    routine_display_name_set = load_routine_display_name_set()
    active_bins_by_date, active_bins_schedule_available = load_active_schedule_by_date(
        ACTIVE_BINS_CSV,
        entity_label="BIN",
        normalize_entity_keys=False,
    )
    active_routine_by_date, active_routine_schedule_available = load_active_schedule_by_date(
        ACTIVE_ROUTINE_CSV,
        entity_label="Routine",
        normalize_entity_keys=True,
    )

    df_range = df[(df["_planned_date"] >= start) & (df["_planned_date"] <= end)].copy()
    if df_range.empty:
        print(
            "No rows found in selected range "
            f"{start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}. "
            "Writing empty map."
        )
        valid_all = df.dropna(subset=[COL_LAT, COL_LON])
        center_lat, center_lon = resolve_center(
            valid_stop_df=valid_all,
            savers_points=savers_points,
            bins_points=bins_points,
            routine_points=routine_points,
        )

        html_doc = build_html(
            day_payloads=[],
            savers_points=savers_points,
            depot_point=depot_point,
            bins_points=bins_points,
            routine_points=routine_points,
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
        valid_all = df.dropna(subset=[COL_LAT, COL_LON])
        center_lat, center_lon = resolve_center(
            valid_stop_df=valid_all,
            savers_points=savers_points,
            bins_points=bins_points,
            routine_points=routine_points,
        )

        html_doc = build_html(
            day_payloads=[],
            savers_points=savers_points,
            depot_point=depot_point,
            bins_points=bins_points,
            routine_points=routine_points,
            center_lat=center_lat,
            center_lon=center_lon,
            range_start=start,
            range_end=end,
        )
        OUTPUT_HTML.parent.mkdir(parents=True, exist_ok=True)
        OUTPUT_HTML.write_text(html_doc, encoding="utf-8")
        print(f"Saved map: {OUTPUT_HTML}")
        return

    valid_all = df_range.dropna(subset=[COL_LAT, COL_LON])
    center_lat, center_lon = resolve_center(
        valid_stop_df=valid_all,
        savers_points=savers_points,
        bins_points=bins_points,
        routine_points=routine_points,
    )

    day_payloads: list[dict[str, Any]] = []
    all_drivers_in_range = [str(driver) for driver in df_range[COL_DRIVER].unique()]
    driver_color_map = build_driver_color_map(all_drivers_in_range)
    print(f"Date range: {start.strftime('%Y-%m-%d')} to {end.strftime('%Y-%m-%d')}")
    print(f"Days in range with routes: {len(dates)}")
    print(f"Savers locations: {len(savers_points)}")
    if depot_point:
        print(
            "Depot for route start/end enforcement: "
            f"{depot_point['display_name']} "
            f"({depot_point['lat']:.6f}, {depot_point['lon']:.6f})"
        )
    else:
        print("Depot start/end enforcement disabled (no valid depot loaded).")
    print(f"BIN locations: {len(bins_points)}")
    print(f"Routine locations: {len(routine_points)}")
    print(f"BIN display-name aliases: {len(bins_display_name_set)}")
    print(f"Routine display names: {len(routine_display_name_set)}")
    print(
        f"BIN active schedule loaded: {active_bins_schedule_available} "
        f"(dates: {len(active_bins_by_date)})"
    )
    print(
        f"Routine active schedule loaded: {active_routine_schedule_available} "
        f"(dates: {len(active_routine_by_date)})"
    )
    print(f"Unique drivers in range: {len(driver_color_map)}")

    for date in dates:
        date_df = df_range[df_range["_date_str"] == date].copy()
        missing_mask = date_df[COL_LAT].isna() | date_df[COL_LON].isna()
        missing_total = int(missing_mask.sum())

        route_specs: list[dict[str, Any]] = []
        total_distance_m = 0.0
        total_drive_duration_s = 0.0
        total_duration_s = 0.0
        metric_routes_count = 0
        missing_metric_routes_count = 0

        drivers = sorted(date_df[COL_DRIVER].unique())
        active_bins_for_date = (
            active_bins_by_date.get(date, {}) if active_bins_schedule_available else {}
        )
        active_routine_for_date = (
            active_routine_by_date.get(date, {}) if active_routine_schedule_available else {}
        )

        active_bin_markers: list[dict[str, Any]] = []
        if active_bins_schedule_available:
            for point in bins_points:
                bin_id = str(point.get("bin_id", "")).strip()
                if not bin_id:
                    continue
                schedule_payload = active_bins_for_date.get(bin_id, {})
                if not schedule_payload:
                    continue
                marker = dict(point)
                marker["stops_in_previous_28"] = schedule_payload.get("stops_in_previous_28")
                marker["stops_in_next_28"] = schedule_payload.get("stops_in_next_28")
                marker["previous_days_since"] = schedule_payload.get("previous_days_since")
                marker["next_days_to"] = schedule_payload.get("next_days_to")
                active_bin_markers.append(marker)
        else:
            for point in bins_points:
                marker = dict(point)
                marker["stops_in_previous_28"] = None
                marker["stops_in_next_28"] = None
                marker["previous_days_since"] = None
                marker["next_days_to"] = None
                active_bin_markers.append(marker)
        active_bin_ids = {
            str(marker.get("bin_id", "")).strip()
            for marker in active_bin_markers
            if str(marker.get("bin_id", "")).strip()
        }
        active_bin_markers_by_id = {
            str(marker.get("bin_id", "")).strip(): marker
            for marker in active_bin_markers
            if str(marker.get("bin_id", "")).strip()
        }

        active_routine_markers: list[dict[str, Any]] = []
        active_routine_markers_by_key: dict[str, dict[str, Any]] = {}
        if active_routine_schedule_available:
            for point in routine_points:
                display_name_key = str(point.get("display_name_key", "")).strip()
                if not display_name_key:
                    continue
                schedule_payload = active_routine_for_date.get(display_name_key, {})
                if not schedule_payload:
                    continue
                marker = {
                    "lat": float(point["lat"]),
                    "lon": float(point["lon"]),
                    "display_name": str(point.get("display_name", "")),
                    "display_name_key": display_name_key,
                    "visit_count": int(point.get("visit_count", 0)),
                    "stops_in_previous_28": schedule_payload.get("stops_in_previous_28"),
                    "stops_in_next_28": schedule_payload.get("stops_in_next_28"),
                    "previous_days_since": schedule_payload.get("previous_days_since"),
                    "next_days_to": schedule_payload.get("next_days_to"),
                }
                active_routine_markers.append(marker)
                active_routine_markers_by_key[display_name_key] = marker
        else:
            for point in routine_points:
                display_name_key = str(point.get("display_name_key", "")).strip()
                if not display_name_key:
                    continue
                marker = {
                    "lat": float(point["lat"]),
                    "lon": float(point["lon"]),
                    "display_name": str(point.get("display_name", "")),
                    "display_name_key": display_name_key,
                    "visit_count": int(point.get("visit_count", 0)),
                    "stops_in_previous_28": None,
                    "stops_in_next_28": None,
                    "previous_days_since": None,
                    "next_days_to": None,
                }
                active_routine_markers.append(marker)
                active_routine_markers_by_key[display_name_key] = marker
        if active_routine_schedule_available:
            active_routine_display_keys = set(active_routine_for_date.keys())
        else:
            active_routine_display_keys = set(routine_display_name_set)

        print(
            f"Date: {date} | Routes (drivers): {len(drivers)} | Missing coords: {missing_total} | "
            f"Active BIN markers: {len(active_bin_markers)} | "
            f"Active routine markers: {len(active_routine_markers)}"
        )
        depot_display_name_key = (
            str(depot_point.get("display_name_key", "")).strip() if depot_point else ""
        )

        for driver in drivers:
            color = driver_color_map.get(driver, COLORS[0])
            driver_df = date_df[date_df[COL_DRIVER] == driver].copy()
            driver_valid = (
                driver_df.dropna(subset=[COL_LAT, COL_LON]).sort_values(
                    COL_STOP, na_position="last"
                )
            )

            missing_stops = int((driver_df[COL_LAT].isna() | driver_df[COL_LON].isna()).sum())
            if COL_ACTUAL_DURATION in driver_df.columns:
                stop_duration_minutes_total = float(
                    pd.to_numeric(driver_df[COL_ACTUAL_DURATION], errors="coerce")
                    .fillna(0)
                    .clip(lower=0)
                    .sum()
                )
            else:
                stop_duration_minutes_total = 0.0

            stops = []
            for _, row in driver_valid.iterrows():
                stop_num = row[COL_STOP]
                if pd.isna(stop_num):
                    stop_label = "?"
                else:
                    stop_label = (
                        str(int(stop_num)) if float(stop_num).is_integer() else str(stop_num)
                    )
                display_name_key = _normalize_display_name(row.get(COL_OSM_DISPLAY_NAME))
                bin_id_for_display = bins_display_name_to_bin_id.get(display_name_key, "")
                is_active_bin_stop = bool(
                    bin_id_for_display
                    and (
                        (not active_bins_schedule_available)
                        or (bin_id_for_display in active_bin_ids)
                    )
                )
                is_active_routine_stop = bool(
                    display_name_key in routine_display_name_set
                    and (
                        (not active_routine_schedule_available)
                        or (display_name_key in active_routine_display_keys)
                    )
                )
                if is_active_bin_stop:
                    marker_shape = "bin"
                elif is_active_routine_stop:
                    marker_shape = "routine"
                else:
                    marker_shape = "dot"
                is_depot_stop = bool(
                    depot_display_name_key and display_name_key == depot_display_name_key
                )
                stop_payload: dict[str, Any] = {
                    "lat": float(row[COL_LAT]),
                    "lon": float(row[COL_LON]),
                    "stop": stop_label,
                    "raw_address": html.escape(_fmt_address(row.get(COL_ADDRESS))),
                    "osm_display_name": html.escape(_fmt_address(row.get(COL_OSM_DISPLAY_NAME))),
                    "display_name_key": display_name_key,
                    "marker_shape": "depot" if is_depot_stop else marker_shape,
                    "is_depot": is_depot_stop,
                    "actual_duration_minutes": round(
                        _parse_duration_minutes(row.get(COL_ACTUAL_DURATION)), 2
                    ),
                }
                if is_active_bin_stop:
                    bin_marker = active_bin_markers_by_id.get(bin_id_for_display, {})
                    stop_payload.update(
                        {
                            "entity_kind": "BIN",
                            "entity_id": str(bin_marker.get("bin_id", bin_id_for_display)),
                            "entity_name": str(bin_marker.get("location_name", "")),
                            "entity_display_name": str(bin_marker.get("primary_display_name", "")),
                            "entity_visit_count": bin_marker.get("visit_count"),
                            "entity_stops_in_previous_28": bin_marker.get("stops_in_previous_28"),
                            "entity_stops_in_next_28": bin_marker.get("stops_in_next_28"),
                            "entity_previous_days_since": bin_marker.get("previous_days_since"),
                            "entity_next_days_to": bin_marker.get("next_days_to"),
                        }
                    )
                elif is_active_routine_stop:
                    routine_marker = active_routine_markers_by_key.get(display_name_key, {})
                    stop_payload.update(
                        {
                            "entity_kind": "Routine",
                            "entity_display_name": str(
                                routine_marker.get("display_name", _fmt_address(row.get(COL_OSM_DISPLAY_NAME)))
                            ),
                            "entity_visit_count": routine_marker.get("visit_count"),
                            "entity_stops_in_previous_28": routine_marker.get("stops_in_previous_28"),
                            "entity_stops_in_next_28": routine_marker.get("stops_in_next_28"),
                            "entity_previous_days_since": routine_marker.get("previous_days_since"),
                            "entity_next_days_to": routine_marker.get("next_days_to"),
                        }
                    )
                stops.append(stop_payload)

            depot_inserted_start = False
            depot_inserted_end = False
            if depot_point and stops:
                if depot_display_name_key:
                    if str(stops[0].get("display_name_key", "")).strip() != depot_display_name_key:
                        stops.insert(0, make_depot_stop_payload(depot_point))
                        depot_inserted_start = True
                    if str(stops[-1].get("display_name_key", "")).strip() != depot_display_name_key:
                        stops.append(make_depot_stop_payload(depot_point))
                        depot_inserted_end = True

            coords_lat_lon = [[float(stop["lat"]), float(stop["lon"])] for stop in stops]
            osrm_error = ""
            if len(coords_lat_lon) >= 2:
                try:
                    osrm_result = fetch_osrm_route(coords_lat_lon)
                    route_points = osrm_result["points_lat_lon"]
                    distance_m = float(osrm_result["distance_m"])
                    drive_duration_s = float(osrm_result["duration_s"])
                    straight_fallback_used = False
                except Exception as exc:
                    if ALLOW_STRAIGHT_LINE_FALLBACK:
                        route_points = coords_lat_lon
                        straight_fallback_used = True
                    else:
                        route_points = []
                        straight_fallback_used = False
                    distance_m = None
                    drive_duration_s = None
                    osrm_error = str(exc)
            elif len(coords_lat_lon) == 1:
                route_points = coords_lat_lon
                distance_m = 0.0
                drive_duration_s = 0.0
                straight_fallback_used = False
            else:
                route_points = []
                distance_m = None
                drive_duration_s = None
                straight_fallback_used = False

            stop_duration_s = stop_duration_minutes_total * 60.0
            total_route_duration_s = (
                drive_duration_s + stop_duration_s
                if drive_duration_s is not None
                else None
            )

            if distance_m is not None:
                total_distance_m += distance_m
                metric_routes_count += 1
            else:
                missing_metric_routes_count += 1
            if drive_duration_s is not None:
                total_drive_duration_s += drive_duration_s
            if total_route_duration_s is not None:
                total_duration_s += total_route_duration_s

            non_depot_stops = [stop for stop in stops if not bool(stop.get("is_depot"))]
            first_stop = non_depot_stops[0] if non_depot_stops else None
            last_stop = non_depot_stops[-1] if non_depot_stops else None

            route_specs.append(
                {
                    "driver": driver,
                    "color": color,
                    "stops_total": int(len(driver_df)),
                    "missing_stops": missing_stops,
                    "distance_text": _fmt_distance(distance_m),
                    "duration_text": _fmt_duration(drive_duration_s),
                    "drive_duration_text": _fmt_duration(drive_duration_s),
                    "total_duration_text": _fmt_duration(total_route_duration_s),
                    "stop_duration_minutes_total": round(stop_duration_minutes_total, 2),
                    "route_points": route_points,
                    "stops": stops,
                    "first_stop": first_stop,
                    "last_stop": last_stop,
                    "osrm_error": osrm_error,
                    "straight_fallback_used": straight_fallback_used,
                }
            )

            print(
                f"  {driver}: stops={len(driver_df)}, valid={len(driver_valid)}, "
                f"missing={missing_stops}, distance={_fmt_distance(distance_m)}, "
                f"drive={_fmt_duration(drive_duration_s)}, "
                f"stop={stop_duration_minutes_total:.1f} min, "
                f"total={_fmt_duration(total_route_duration_s)}"
            )
            if depot_inserted_start or depot_inserted_end:
                print(
                    "    Depot added to route boundaries: "
                    f"start={depot_inserted_start}, end={depot_inserted_end}"
                )
            if osrm_error:
                if straight_fallback_used:
                    print(f"    OSRM fallback to straight line: {osrm_error}")
                else:
                    print(f"    OSRM route unavailable: {osrm_error}")

        if metric_routes_count == 0:
            total_distance_text = "n/a"
            total_drive_duration_text = "n/a"
            total_duration_text = "n/a"
        elif missing_metric_routes_count > 0:
            total_distance_text = f"{_fmt_distance(total_distance_m)} (partial)"
            total_drive_duration_text = f"{_fmt_duration(total_drive_duration_s)} (partial)"
            total_duration_text = f"{_fmt_duration(total_duration_s)} (partial)"
        else:
            total_distance_text = _fmt_distance(total_distance_m)
            total_drive_duration_text = _fmt_duration(total_drive_duration_s)
            total_duration_text = _fmt_duration(total_duration_s)

        day_payloads.append(
            {
                "date": date,
                "routes": route_specs,
                "missing_total": missing_total,
                "total_distance_text": total_distance_text,
                "total_drive_duration_text": total_drive_duration_text,
                "total_duration_text": total_duration_text,
                "routes_count": len(drivers),
                "active_bins": active_bin_markers,
                "active_routine": active_routine_markers,
                "active_bins_count": len(active_bin_markers),
                "active_routine_count": len(active_routine_markers),
            }
        )

    html_doc = build_html(
        day_payloads=day_payloads,
        savers_points=savers_points,
        depot_point=depot_point,
        bins_points=bins_points,
        routine_points=routine_points,
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
