#!/usr/bin/env python3
"""Generate BIN and routine dashboard HTML from base linked tables."""

import argparse
import json
from pathlib import Path
from typing import Dict, List, Optional

try:
    import pandas as pd
except ImportError as exc:
    raise SystemExit(
        "This script requires pandas. Run with the project venv, for example: "
        "./.venv/bin/python visualizations/viz_bins.py"
    ) from exc

from create_bins_data import (
    BIN_NEARBY_THRESHOLD_METERS,
    CACHE_CANDIDATES,
    DATA_CANDIDATES,
    build_analysis_dataframe,
    first_existing,
    normalize_location_name,
)

SCRIPT_DIR = Path(__file__).resolve().parent

INPUT_DATA_CSV = first_existing(DATA_CANDIDATES)
INPUT_CACHE_CSV = first_existing(CACHE_CANDIDATES)
INPUT_BINS_CSV = SCRIPT_DIR / "data_bins.csv"
INPUT_ROUTINE_CSV = SCRIPT_DIR / "data_routine.csv"
INPUT_SAVERS_CSV = SCRIPT_DIR / "data_savers.csv"
OUTPUT_HTML = SCRIPT_DIR / "dash_bins.html"


def to_int(value: Optional[str], default: int = 0) -> int:
    if value is None:
        return default
    raw = str(value).strip()
    if raw == "":
        return default
    try:
        return int(float(raw))
    except ValueError:
        return default


def to_float(value: Optional[str], default: float = 0.0) -> float:
    if value is None:
        return default
    raw = str(value).strip()
    if raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def compute_center(points: List[Dict[str, float]]) -> List[float]:
    if not points:
        return [41.7, -71.5]
    lat = sum(point["lat"] for point in points) / len(points)
    lon = sum(point["lon"] for point in points) / len(points)
    return [lat, lon]


def parse_json_string_list(value: object) -> List[str]:
    raw = str(value or "").strip()
    if raw == "":
        return []
    try:
        parsed = json.loads(raw)
    except Exception:
        return []
    if not isinstance(parsed, list):
        return []
    return [str(item).strip() for item in parsed if str(item).strip() != ""]


def main() -> None:
    parser = argparse.ArgumentParser(description="Create BIN and routine dashboard HTML")
    parser.add_argument("--data", default=str(INPUT_DATA_CSV), help="Stop-level geocoded data CSV")
    parser.add_argument("--cache", default=str(INPUT_CACHE_CSV), help="Geocode cache CSV")
    parser.add_argument("--bins", default=str(INPUT_BINS_CSV), help="Bins dataframe CSV")
    parser.add_argument("--routine", default=str(INPUT_ROUTINE_CSV), help="Routine donors CSV")
    parser.add_argument("--savers", default=str(INPUT_SAVERS_CSV), help="Savers aggregates CSV")
    parser.add_argument("--output", default=str(OUTPUT_HTML), help="Output HTML path")
    args = parser.parse_args()

    data_path = Path(args.data).expanduser().resolve()
    cache_path = Path(args.cache).expanduser().resolve()
    bins_path = Path(args.bins).expanduser().resolve()
    routine_path = Path(args.routine).expanduser().resolve()
    savers_path = Path(args.savers).expanduser().resolve()
    output_path = Path(args.output).expanduser().resolve()

    required_paths = [
        data_path,
        cache_path,
        bins_path,
        routine_path,
    ]
    for required in required_paths:
        if not required.exists():
            raise SystemExit(f"Input CSV not found: {required}")

    analysis_df = build_analysis_dataframe(data_path, cache_path)
    analysis_df["location_norm"] = analysis_df["Location"].map(normalize_location_name)
    analysis_df["is_bin_labeled"] = analysis_df["Location"].str.strip().str.upper().str.startswith("BIN")

    bins_df = pd.read_csv(bins_path, dtype=str).fillna("")
    routine_df = pd.read_csv(routine_path, dtype=str).fillna("")
    if savers_path.exists():
        savers_df = pd.read_csv(savers_path, dtype=str).fillna("")
    else:
        savers_df = pd.DataFrame(columns=["savers_id", "associated_stop_count"])

    if "bin_cluster_id" not in bins_df.columns:
        bins_df["bin_cluster_id"] = bins_df.get("bin_id", "")
    bins_df["bin_cluster_id"] = bins_df["bin_cluster_id"].replace("", pd.NA).fillna(
        bins_df.get("bin_id", "")
    )

    location_to_bin_id: Dict[str, str] = {}
    seed_display_to_bin_id: Dict[str, str] = {}
    distance_display_to_bin_id: Dict[str, str] = {}
    seed_display_name_set = set()
    distance_display_name_set = set()
    for row in bins_df.itertuples(index=False):
        bin_id = str(getattr(row, "bin_cluster_id", "") or getattr(row, "bin_id", "")).strip()
        if bin_id == "":
            continue
        location_norm = str(getattr(row, "location_norm", "")).strip()
        if location_norm and location_norm not in location_to_bin_id:
            location_to_bin_id[location_norm] = bin_id

        seed_names = parse_json_string_list(getattr(row, "seed_display_names", ""))
        distance_names = parse_json_string_list(getattr(row, "distance_display_names", ""))
        for display_name in seed_names:
            seed_display_name_set.add(display_name)
            if display_name not in seed_display_to_bin_id:
                seed_display_to_bin_id[display_name] = bin_id
        for display_name in distance_names:
            distance_display_name_set.add(display_name)
            if display_name not in distance_display_to_bin_id:
                distance_display_to_bin_id[display_name] = bin_id

    display_to_bin_id = dict(seed_display_to_bin_id)
    for display_name, bin_id in distance_display_to_bin_id.items():
        display_to_bin_id.setdefault(display_name, bin_id)

    analysis_df["bin_cluster_id"] = analysis_df["display_name_final"].map(display_to_bin_id).fillna("")
    unassigned_labeled_mask = analysis_df["is_bin_labeled"] & (analysis_df["bin_cluster_id"] == "")
    analysis_df.loc[unassigned_labeled_mask, "bin_cluster_id"] = (
        analysis_df.loc[unassigned_labeled_mask, "location_norm"].map(location_to_bin_id).fillna("")
    )

    analysis_df["is_bin_seed_display"] = analysis_df["display_name_final"].isin(seed_display_name_set)
    analysis_df["is_bin_by_display_association"] = (
        analysis_df["is_bin_seed_display"] & ~analysis_df["is_bin_labeled"]
    )
    analysis_df["is_bin_by_distance_association"] = analysis_df["display_name_final"].isin(
        distance_display_name_set
    )
    analysis_df["is_bin_associated"] = (
        analysis_df["is_bin_labeled"]
        | analysis_df["is_bin_by_display_association"]
        | analysis_df["is_bin_by_distance_association"]
    )
    analysis_df["is_bin"] = analysis_df["is_bin_associated"]
    analysis_df["bin_id"] = analysis_df["bin_cluster_id"]

    routine_display_names = {
        str(name).strip()
        for name in routine_df.get("display_name_final", pd.Series([], dtype=str)).astype(str).tolist()
        if str(name).strip() != ""
    }
    analysis_df["is_routine"] = (
        ~analysis_df["is_bin_associated"] & analysis_df["display_name_final"].isin(routine_display_names)
    )

    analysis_df["actual_date_parsed"] = pd.to_datetime(analysis_df.get("Actual Date", ""), errors="coerce")
    analysis_df["planned_date_parsed"] = pd.to_datetime(analysis_df.get("Planned Date", ""), errors="coerce")
    analysis_df["visit_date"] = analysis_df["actual_date_parsed"].fillna(analysis_df["planned_date_parsed"])
    analysis_df["visit_month"] = analysis_df["visit_date"].dt.to_period("M").astype(str)
    analysis_df.loc[analysis_df["visit_date"].isna(), "visit_month"] = ""

    total_stops = int(len(analysis_df))
    bin_labeled_stops = int(analysis_df["is_bin_labeled"].sum())
    bin_assoc_stops = int(analysis_df["is_bin_associated"].sum())
    bin_assoc_only_stops = int((analysis_df["is_bin_associated"] & ~analysis_df["is_bin_labeled"]).sum())
    bin_geo_cluster_stops = int(analysis_df["is_bin_by_distance_association"].sum())
    non_bin_stops = total_stops - bin_assoc_stops
    unique_bins = int(len(bins_df))
    routine_total_stops = int(analysis_df["is_routine"].sum())
    routine_unique_customers = int(len(routine_display_names))
    bin_assoc_share_pct = (bin_assoc_stops / total_stops * 100.0) if total_stops else 0.0
    nearby_threshold_m = int(BIN_NEARBY_THRESHOLD_METERS)
    savers_df["associated_stop_count_num"] = pd.to_numeric(
        savers_df.get("associated_stop_count", 0), errors="coerce"
    ).fillna(0).astype(int)
    savers_unique_count = int(len(savers_df))
    savers_total_stops = int(savers_df["associated_stop_count_num"].sum()) if savers_unique_count else 0
    other_total_stops = max(
        total_stops - bin_assoc_stops - routine_total_stops - savers_total_stops,
        0,
    )

    location_scope_df = analysis_df.loc[analysis_df["display_name_final"] != ""].copy()
    total_locations = int(location_scope_df["display_name_final"].nunique())
    bin_location_count = int(
        location_scope_df.loc[location_scope_df["is_bin_associated"], "display_name_final"].nunique()
    )
    routine_location_count = int(
        location_scope_df.loc[location_scope_df["is_routine"], "display_name_final"].nunique()
    )
    other_location_count = int(
        location_scope_df.loc[
            ~location_scope_df["is_bin_associated"] & ~location_scope_df["is_routine"],
            "display_name_final",
        ].nunique()
    )

    grouped_counts_df = (
        analysis_df.loc[
            analysis_df["display_name_final"] != "",
            ["is_bin_associated", "display_name_final"],
        ]
        .groupby(["is_bin_associated", "display_name_final"], as_index=False)
        .size()
        .rename(columns={"size": "stop_count"})
    )
    grouped_counts_df["stop_count"] = pd.to_numeric(grouped_counts_df["stop_count"], errors="coerce").fillna(0).astype(int)
    bin_stop_counts = grouped_counts_df.loc[
        grouped_counts_df["is_bin_associated"], "stop_count"
    ].tolist()
    other_display_counts_df = (
        analysis_df.loc[
            (analysis_df["display_name_final"] != "")
            & (~analysis_df["is_bin_associated"])
            & (~analysis_df["display_name_final"].isin(routine_display_names)),
            ["display_name_final"],
        ]
        .groupby("display_name_final", as_index=False)
        .size()
        .rename(columns={"size": "stop_count"})
    )
    other_display_counts_df["stop_count"] = (
        pd.to_numeric(other_display_counts_df["stop_count"], errors="coerce")
        .fillna(0)
        .astype(int)
    )
    other_stop_counts = other_display_counts_df["stop_count"].tolist()

    routine_df["total_stop_count"] = pd.to_numeric(routine_df.get("total_stop_count", 0), errors="coerce").fillna(0).astype(int)
    routine_df["max_monthly_stop_count"] = pd.to_numeric(
        routine_df.get("max_monthly_stop_count", 0), errors="coerce"
    ).fillna(0).astype(int)
    routine_df["lat_num"] = pd.to_numeric(routine_df.get("lat", ""), errors="coerce")
    routine_df["lon_num"] = pd.to_numeric(routine_df.get("lon", ""), errors="coerce")
    routine_unique_donors = int(len(routine_df))
    routine_gt20_mask = routine_df["total_stop_count"] > 20
    routine_gt3_month_mask = routine_df["max_monthly_stop_count"] > 3
    routine_gt20_donors = int(routine_gt20_mask.sum())
    routine_gt3_month_donors = int(routine_gt3_month_mask.sum())
    routine_both_criteria_donors = int((routine_gt20_mask & routine_gt3_month_mask).sum())
    routine_only_gt20_donors = int((routine_gt20_mask & ~routine_gt3_month_mask).sum())
    routine_only_gt3_month_donors = int((routine_gt3_month_mask & ~routine_gt20_mask).sum())

    routine_stop_counts = routine_df.loc[routine_df["total_stop_count"] > 0, "total_stop_count"].tolist()
    routine_points: List[Dict[str, object]] = []
    for row in routine_df.itertuples(index=False):
        if pd.isna(getattr(row, "lat_num", None)) or pd.isna(getattr(row, "lon_num", None)):
            continue
        routine_points.append(
            {
                "display_name": str(getattr(row, "display_name_final", "")).strip(),
                "total_stop_count": int(getattr(row, "total_stop_count", 0)),
                "max_monthly_stop_count": int(getattr(row, "max_monthly_stop_count", 0)),
                "lat": float(getattr(row, "lat_num")),
                "lon": float(getattr(row, "lon_num")),
            }
        )

    bins_df["associated_stop_count_num"] = pd.to_numeric(
        bins_df.get("associated_stop_count", 0), errors="coerce"
    ).fillna(0).astype(int)
    bins_df["grouped_display_name_count_num"] = pd.to_numeric(
        bins_df.get("grouped_display_name_count", 0), errors="coerce"
    ).fillna(0).astype(int)
    bins_df["centroid_lat_num"] = pd.to_numeric(bins_df.get("centroid_lat", ""), errors="coerce")
    bins_df["centroid_lon_num"] = pd.to_numeric(bins_df.get("centroid_lon", ""), errors="coerce")
    bins_df["primary_lat_num"] = pd.to_numeric(bins_df.get("primary_lat", ""), errors="coerce")
    bins_df["primary_lon_num"] = pd.to_numeric(bins_df.get("primary_lon", ""), errors="coerce")
    bins_df["map_lat"] = bins_df["centroid_lat_num"].fillna(bins_df["primary_lat_num"])
    bins_df["map_lon"] = bins_df["centroid_lon_num"].fillna(bins_df["primary_lon_num"])

    bin_address_set = set()
    bin_label_set = set()
    for row in bins_df.itertuples(index=False):
        label = str(getattr(row, "location_name_primary", "")).strip()
        if label != "":
            bin_label_set.add(label)

        addresses = parse_json_string_list(getattr(row, "all_grouped_display_names", ""))
        if not addresses:
            primary_display_name = str(getattr(row, "primary_display_name", "")).strip()
            if primary_display_name != "":
                addresses = [primary_display_name]
        for address in addresses:
            bin_address_set.add(address)

    unique_bin_addresses = int(len(bin_address_set))
    unique_bin_labels = int(len(bin_label_set))
    unique_bin_clusters = int(len(bins_df))

    overall_bin_points: List[Dict[str, object]] = []
    for row in bins_df.itertuples(index=False):
        if pd.isna(getattr(row, "map_lat", None)) or pd.isna(getattr(row, "map_lon", None)):
            continue
        overall_bin_points.append(
            {
                "bin_id": str(getattr(row, "bin_cluster_id", "") or getattr(row, "bin_id", "")).strip(),
                "location_name": str(getattr(row, "location_name_primary", "")).strip(),
                "primary_display_name": str(getattr(row, "primary_display_name", "")).strip(),
                "display_name_count": int(getattr(row, "grouped_display_name_count_num", 0)),
                "stop_count": int(getattr(row, "associated_stop_count_num", 0)),
                "lat": float(getattr(row, "map_lat")),
                "lon": float(getattr(row, "map_lon")),
            }
        )

    bin_month_labels: List[str] = []
    month_unique_bins: List[int] = []
    bin_month_total_stops: List[int] = []
    monthly_bin_map_data: Dict[str, List[Dict[str, object]]] = {}
    monthly_max_visits = 1

    monthly_scope_df = analysis_df.loc[
        (analysis_df["bin_cluster_id"] != "") & analysis_df["visit_date"].notna()
    ].copy()
    if not monthly_scope_df.empty:
        month_start = monthly_scope_df["visit_date"].min().to_period("M")
        month_end = monthly_scope_df["visit_date"].max().to_period("M")
        bin_month_labels = [str(period) for period in pd.period_range(month_start, month_end, freq="M")]
        monthly_bin_map_data = {month: [] for month in bin_month_labels}

        month_unique_bins = (
            monthly_scope_df.groupby("visit_month")["bin_cluster_id"]
            .nunique()
            .reindex(bin_month_labels, fill_value=0)
            .astype(int)
            .tolist()
        )
        bin_month_total_stops = (
            monthly_scope_df.groupby("visit_month")
            .size()
            .reindex(bin_month_labels, fill_value=0)
            .astype(int)
            .tolist()
        )

        monthly_bin_visits_df = (
            monthly_scope_df.groupby(["visit_month", "bin_cluster_id"], as_index=False)
            .size()
            .rename(columns={"size": "visit_count"})
        )

        bin_geo_df = bins_df[
            [
                "bin_cluster_id",
                "location_name_primary",
                "primary_display_name",
                "map_lat",
                "map_lon",
            ]
        ].copy()
        monthly_bin_visits_df = monthly_bin_visits_df.merge(
            bin_geo_df,
            how="left",
            on="bin_cluster_id",
        )
        monthly_bin_visits_df = monthly_bin_visits_df.loc[
            monthly_bin_visits_df["map_lat"].notna() & monthly_bin_visits_df["map_lon"].notna()
        ].copy()
        monthly_bin_visits_df = monthly_bin_visits_df.sort_values(
            ["visit_month", "visit_count", "bin_cluster_id"],
            ascending=[True, False, True],
        )

        for row in monthly_bin_visits_df.itertuples(index=False):
            visit_count = int(getattr(row, "visit_count"))
            monthly_max_visits = max(monthly_max_visits, visit_count)
            month_key = str(getattr(row, "visit_month"))
            monthly_bin_map_data.setdefault(month_key, []).append(
                {
                    "bin_id": str(getattr(row, "bin_cluster_id", "")).strip(),
                    "location_name": str(getattr(row, "location_name_primary", "")).strip(),
                    "primary_display_name": str(getattr(row, "primary_display_name", "")).strip(),
                    "visit_count": visit_count,
                    "lat": float(getattr(row, "map_lat")),
                    "lon": float(getattr(row, "map_lon")),
                }
            )

    routine_month_labels: List[str] = []
    routine_month_unique_customers: List[int] = []
    routine_month_total_stops: List[int] = []
    monthly_routine_map_data: Dict[str, List[Dict[str, object]]] = {}
    routine_monthly_max_visits = 1

    routine_scope_df = analysis_df.loc[
        analysis_df["is_routine"] & analysis_df["visit_date"].notna() & (analysis_df["display_name_final"] != "")
    ].copy()
    if not routine_scope_df.empty:
        routine_month_start = routine_scope_df["visit_date"].min().to_period("M")
        routine_month_end = routine_scope_df["visit_date"].max().to_period("M")
        routine_month_labels = [
            str(period) for period in pd.period_range(routine_month_start, routine_month_end, freq="M")
        ]
        monthly_routine_map_data = {month: [] for month in routine_month_labels}

        routine_month_unique_customers = (
            routine_scope_df.groupby("visit_month")["display_name_final"]
            .nunique()
            .reindex(routine_month_labels, fill_value=0)
            .astype(int)
            .tolist()
        )
        routine_month_total_stops = (
            routine_scope_df.groupby("visit_month")
            .size()
            .reindex(routine_month_labels, fill_value=0)
            .astype(int)
            .tolist()
        )

        routine_monthly_visits_df = (
            routine_scope_df.groupby(["visit_month", "display_name_final"], as_index=False)
            .size()
            .rename(columns={"size": "visit_count"})
        )

        display_geo_df = (
            analysis_df.loc[analysis_df["display_name_final"] != "", ["display_name_final", "lat_final", "lon_final"]]
            .groupby("display_name_final", as_index=False)
            .agg(lat_geo=("lat_final", "median"), lon_geo=("lon_final", "median"))
        )
        routine_geo_df = routine_df[["display_name_final", "lat_num", "lon_num"]].copy()
        routine_geo_df = routine_geo_df.merge(display_geo_df, how="left", on="display_name_final")
        routine_geo_df["lat_map"] = routine_geo_df["lat_num"].fillna(routine_geo_df["lat_geo"])
        routine_geo_df["lon_map"] = routine_geo_df["lon_num"].fillna(routine_geo_df["lon_geo"])
        routine_geo_df = routine_geo_df[["display_name_final", "lat_map", "lon_map"]]

        routine_monthly_visits_df = routine_monthly_visits_df.merge(
            routine_geo_df,
            how="left",
            on="display_name_final",
        )
        routine_monthly_visits_df = routine_monthly_visits_df.loc[
            routine_monthly_visits_df["lat_map"].notna() & routine_monthly_visits_df["lon_map"].notna()
        ].copy()
        routine_monthly_visits_df = routine_monthly_visits_df.sort_values(
            ["visit_month", "visit_count", "display_name_final"],
            ascending=[True, False, True],
        )

        for row in routine_monthly_visits_df.itertuples(index=False):
            visit_count = int(getattr(row, "visit_count"))
            routine_monthly_max_visits = max(routine_monthly_max_visits, visit_count)
            month_key = str(getattr(row, "visit_month"))
            monthly_routine_map_data.setdefault(month_key, []).append(
                {
                    "display_name": str(getattr(row, "display_name_final", "")).strip(),
                    "visit_count": visit_count,
                    "lat": float(getattr(row, "lat_map")),
                    "lon": float(getattr(row, "lon_map")),
                }
            )

    map_center = compute_center(
        [{"lat": float(point["lat"]), "lon": float(point["lon"])} for point in overall_bin_points]
    )
    routine_map_center = compute_center(
        [{"lat": float(point["lat"]), "lon": float(point["lon"])} for point in routine_points]
    )

    html = """<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\" />
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />
  <title>BIN and Routine Dashboard</title>
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
      --accent: #5ad2f4;
      --good: #8bd17c;
      --warn: #ffcf6e;
      --bad: #ff8e72;
      --routine: #f4b460;
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
      height: 380px;
      border-radius: 10px;
      overflow: hidden;
    }
    .section-divider {
      grid-column: 1 / -1;
      display: flex;
      align-items: center;
      gap: 10px;
      margin: 2px 0;
      color: var(--muted);
      font-size: 11px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      font-weight: 700;
    }
    .section-divider::before,
    .section-divider::after {
      content: "";
      flex: 1;
      height: 1px;
      background: var(--panel-border);
    }
    @media (max-width: 1100px) {
      .card-wide { grid-column: span 1; }
    }
  </style>
</head>
<body>
  <header>
    <h1>BIN and Routine Dashboard</h1>
    <p class=\"subtitle\">Visualization source: __DATA_SOURCE__, __CACHE_SOURCE__, __BINS_SOURCE__, __ROUTINE_SOURCE__, __SAVERS_SOURCE__</p>
    <p class=\"subtitle\">Seed bins: <code>Location</code> starts with <code>\"BIN\"</code>; routine: non-bin display names with <code>total stops &gt; 20</code> or <code>max monthly stops &gt; 3</code>.</p>
  </header>

  <section class=\"stats\">
    <div class=\"stat-card\">
      <div class=\"stat-label\">Unique BINs</div>
      <div class=\"stat-value\" style=\"color:var(--accent)\">__UNIQUE_BIN_CLUSTERS__</div>
      <div class=\"stat-label\" style=\"margin-top:8px\">Addresses: __UNIQUE_BIN_ADDRESSES__</div>
      <div class=\"stat-label\" style=\"margin-top:6px\">Labels: __UNIQUE_BIN_LABELS__</div>
      <div class=\"stat-label\" style=\"margin-top:6px\">Clusters: __UNIQUE_BIN_CLUSTERS__</div>
    </div>
    <div class=\"stat-card\">
      <div class=\"stat-label\">Routine Donors</div>
      <div class=\"stat-value\" style=\"color:var(--routine)\">__ROUTINE_UNIQUE_DONORS__</div>
      <div class=\"stat-label\" style=\"margin-top:8px\">Only &gt; 20 Stops: __ROUTINE_ONLY_GT20_DONORS__</div>
      <div class=\"stat-label\" style=\"margin-top:6px\">Only &gt; 3 Visits in a Month: __ROUTINE_ONLY_GT3_MONTH_DONORS__</div>
      <div class=\"stat-label\" style=\"margin-top:6px\">Meets Both Criteria: __ROUTINE_BOTH_CRITERIA_DONORS__</div>
    </div>
    <div class=\"stat-card\">
      <div class=\"stat-label\">Savers</div>
      <div class=\"stat-value\" style=\"color:var(--warn)\">__SAVERS_UNIQUE_COUNT__</div>
      <div class=\"stat-label\" style=\"margin-top:8px\">Savers Stops: __SAVERS_TOTAL_STOPS__</div>
    </div>
    <div class=\"stat-card\">
      <div class=\"stat-label\">Total Stops</div>
      <div class=\"stat-value\">__TOTAL_STOPS__</div>
      <div class=\"stat-label\" style=\"margin-top:8px\">BINs: __BIN_TOTAL_STOPS__</div>
      <div class=\"stat-label\" style=\"margin-top:6px\">Routine Donors: __ROUTINE_TOTAL_STOPS__</div>
      <div class=\"stat-label\" style=\"margin-top:6px\">Savers: __SAVERS_TOTAL_STOPS__</div>
      <div class=\"stat-label\" style=\"margin-top:6px\">Other (non-Savers): __OTHER_TOTAL_STOPS__</div>
    </div>
    <div class=\"stat-card\">
      <div class=\"stat-label\">Locations</div>
      <div class=\"stat-value\">__TOTAL_LOCATIONS__</div>
      <div class=\"stat-label\" style=\"margin-top:8px\">BINs: __BIN_LOCATION_COUNT__</div>
      <div class=\"stat-label\" style=\"margin-top:6px\">Routine Donors: __ROUTINE_LOCATION_COUNT__</div>
      <div class=\"stat-label\" style=\"margin-top:6px\">Other: __OTHER_LOCATION_COUNT__</div>
    </div>
  </section>

  <section class=\"grid\">
    <div class=\"section-divider\"><span>Histogram</span></div>
    <div class=\"card card-wide\">
      <div class=\"card-toolbar\">
        <button class=\"chart-title-btn\">Histogram: Stops per Display Name (BIN, Routine, Other)</button>
        <button class=\"chart-expand-btn\" aria-label=\"Expand chart\">⤢</button>
      </div>
      <div id=\"hist_stop_counts\" class=\"chart-content\" style=\"height:320px;\"></div>
    </div>

    <div class=\"section-divider\"><span>BIN Plots</span></div>
    <div class=\"card\">
      <div class=\"card-toolbar\">
        <button class=\"chart-title-btn\">Trend: Unique Bins Visited per Month</button>
        <button class=\"chart-expand-btn\" aria-label=\"Expand chart\">⤢</button>
      </div>
      <div id=\"line_unique_bins_monthly\" class=\"chart-content\" style=\"height:320px;\"></div>
    </div>
    <div class=\"card\">
      <div class=\"card-toolbar\">
        <button class=\"chart-title-btn\">Trend: Total BIN Stops per Month</button>
        <button class=\"chart-expand-btn\" aria-label=\"Expand chart\">⤢</button>
      </div>
      <div id=\"line_bin_stops_monthly\" class=\"chart-content\" style=\"height:320px;\"></div>
    </div>

    <div class=\"card card-wide\">
      <div class=\"card-toolbar\">
        <button class=\"chart-title-btn\">Map: BIN Centroids Sized by Overall Total Visits</button>
        <button class=\"chart-expand-btn\" aria-label=\"Expand chart\">⤢</button>
      </div>
      <div id=\"bin_map_total\" class=\"chart-content map\"></div>
    </div>
    <div class=\"card card-wide\">
      <div class=\"card-toolbar\">
        <button class=\"chart-title-btn\">Map: BIN Centroids Sized by Monthly Visit Count (Slider)</button>
        <button class=\"chart-expand-btn\" aria-label=\"Expand chart\">⤢</button>
      </div>
      <div style=\"padding: 2px 8px 8px 8px;\">
        <div id=\"bin_month_readout\" style=\"font-size:13px;color:#a4b1c4;margin-bottom:6px;\"></div>
        <input id=\"bin_month_slider\" type=\"range\" min=\"0\" max=\"0\" step=\"1\" value=\"0\" style=\"width:100%;\" />
        <div id=\"bin_month_tick_row\" style=\"position:relative;height:20px;margin-top:4px;\"></div>
      </div>
      <div id=\"bin_map_monthly\" class=\"chart-content map\"></div>
    </div>

    <div class=\"section-divider\"><span>Routine Donor Plots</span></div>
    <div class=\"card\">
      <div class=\"card-toolbar\">
        <button class=\"chart-title-btn\">Trend: Unique Routine Donors Visited per Month</button>
        <button class=\"chart-expand-btn\" aria-label=\"Expand chart\">⤢</button>
      </div>
      <div id=\"line_unique_routine_monthly\" class=\"chart-content\" style=\"height:320px;\"></div>
    </div>
    <div class=\"card\">
      <div class=\"card-toolbar\">
        <button class=\"chart-title-btn\">Trend: Total Routine Stops per Month</button>
        <button class=\"chart-expand-btn\" aria-label=\"Expand chart\">⤢</button>
      </div>
      <div id=\"line_routine_stops_monthly\" class=\"chart-content\" style=\"height:320px;\"></div>
    </div>

    <div class=\"card card-wide\">
      <div class=\"card-toolbar\">
        <button class=\"chart-title-btn\">Map: Routine Donors Sized by Overall Total Visits</button>
        <button class=\"chart-expand-btn\" aria-label=\"Expand chart\">⤢</button>
      </div>
      <div id=\"routine_map_total\" class=\"chart-content map\"></div>
    </div>
    <div class=\"card card-wide\">
      <div class=\"card-toolbar\">
        <button class=\"chart-title-btn\">Map: Routine Donors Sized by Monthly Visits (Slider)</button>
        <button class=\"chart-expand-btn\" aria-label=\"Expand chart\">⤢</button>
      </div>
      <div style=\"padding: 2px 8px 8px 8px;\">
        <div id=\"routine_month_readout\" style=\"font-size:13px;color:#a4b1c4;margin-bottom:6px;\"></div>
        <input id=\"routine_month_slider\" type=\"range\" min=\"0\" max=\"0\" step=\"1\" value=\"0\" style=\"width:100%;\" />
        <div id=\"routine_month_tick_row\" style=\"position:relative;height:20px;margin-top:4px;\"></div>
      </div>
      <div id=\"routine_map_monthly\" class=\"chart-content map\"></div>
    </div>
  </section>

  <script>
    const binStopCounts = __BIN_STOP_COUNTS__;
    const routineStopCounts = __ROUTINE_STOP_COUNTS__;
    const otherStopCounts = __OTHER_STOP_COUNTS__;

    const binMapCenter = __BIN_MAP_CENTER__;
    const overallBinPoints = __OVERALL_BIN_POINTS__;
    const binMonthLabels = __BIN_MONTH_LABELS__;
    const monthUniqueBins = __MONTH_UNIQUE_BINS__;
    const binMonthTotalStops = __BIN_MONTH_TOTAL_STOPS__;
    const monthlyBinMapData = __MONTHLY_BIN_MAP_DATA__;
    const monthlyBinMaxVisits = __MONTHLY_BIN_MAX_VISITS__;

    const routineMapCenter = __ROUTINE_MAP_CENTER__;
    const routinePoints = __ROUTINE_POINTS__;
    const routineMonthLabels = __ROUTINE_MONTH_LABELS__;
    const routineMonthUniqueCustomers = __ROUTINE_MONTH_UNIQUE_CUSTOMERS__;
    const routineMonthTotalStops = __ROUTINE_MONTH_TOTAL_STOPS__;
    const monthlyRoutineMapData = __MONTHLY_ROUTINE_MAP_DATA__;
    const monthlyRoutineMaxVisits = __MONTHLY_ROUTINE_MAX_VISITS__;

    const baseLayout = {
      margin: {t: 10, r: 10, b: 45, l: 45},
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      font: {color: '#e6edf3'}
    };

    Plotly.newPlot('hist_stop_counts', [
      {
        x: binStopCounts,
        type: 'histogram',
        opacity: 0.52,
        name: 'BIN (associated)',
        marker: {color: '#5ad2f4'},
        hovertemplate: 'BIN stops per display_name: %{x}<br>Display names: %{y}<extra></extra>'
      },
      {
        x: routineStopCounts,
        type: 'histogram',
        opacity: 0.60,
        name: 'Routine',
        marker: {color: '#f4b460'},
        hovertemplate: 'Routine stops per display_name: %{x}<br>Display names: %{y}<extra></extra>'
      },
      {
        x: otherStopCounts,
        type: 'histogram',
        opacity: 0.50,
        name: 'Other',
        marker: {color: '#ff8e72'},
        hovertemplate: 'Other stops per display_name: %{x}<br>Display names: %{y}<extra></extra>'
      }
    ], {
      ...baseLayout,
      barmode: 'overlay',
      xaxis: {title: 'Stops per display_name'},
      yaxis: {title: 'Display names (log scale)', type: 'log'},
      legend: {orientation: 'h', x: 0, y: 1.12}
    }, {displayModeBar: false});

    Plotly.newPlot('line_unique_bins_monthly', [{
      x: binMonthLabels,
      y: monthUniqueBins,
      type: 'scatter',
      mode: 'lines+markers',
      marker: {color: '#2ec4ff', size: 7},
      line: {color: '#2ec4ff', width: 2},
      hovertemplate: 'Month: %{x}<br>Unique bins visited: %{y}<extra></extra>'
    }], {
      ...baseLayout,
      xaxis: {title: 'Date (Month)', tickangle: -35},
      yaxis: {title: 'Unique bins visited', rangemode: 'tozero'}
    }, {displayModeBar: false});

    Plotly.newPlot('line_bin_stops_monthly', [{
      x: binMonthLabels,
      y: binMonthTotalStops,
      type: 'bar',
      marker: {color: '#2ec4ff'},
      hovertemplate: 'Month: %{x}<br>BIN stops: %{y}<extra></extra>'
    }], {
      ...baseLayout,
      xaxis: {title: 'Date (Month)', tickangle: -35},
      yaxis: {title: 'BIN stops', rangemode: 'tozero'}
    }, {displayModeBar: false});

    Plotly.newPlot('line_unique_routine_monthly', [{
      x: routineMonthLabels,
      y: routineMonthUniqueCustomers,
      type: 'scatter',
      mode: 'lines+markers',
      marker: {color: '#f4b460', size: 7},
      line: {color: '#f4b460', width: 2},
      hovertemplate: 'Month: %{x}<br>Unique routine donors: %{y}<extra></extra>'
    }], {
      ...baseLayout,
      xaxis: {title: 'Date (Month)', tickangle: -35},
      yaxis: {title: 'Unique routine donors', rangemode: 'tozero'}
    }, {displayModeBar: false});

    Plotly.newPlot('line_routine_stops_monthly', [{
      x: routineMonthLabels,
      y: routineMonthTotalStops,
      type: 'bar',
      marker: {color: '#d49a45'},
      hovertemplate: 'Month: %{x}<br>Routine stops: %{y}<extra></extra>'
    }], {
      ...baseLayout,
      xaxis: {title: 'Date (Month)', tickangle: -35},
      yaxis: {title: 'Routine stops', rangemode: 'tozero'}
    }, {displayModeBar: false});

    const tileUrl = 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png';
    const tileAttr = '&copy; OpenStreetMap contributors';

    function addTotalPointsToMap(map, points, maxValue, popupHtmlBuilder, color, fillColor) {
      points.forEach((p) => {
        const scaled = Math.sqrt((p.total_stop_count || p.stop_count || 0) / Math.max(1, maxValue));
        const radius = 4 + (16 * scaled);
        L.circleMarker([p.lat, p.lon], {
          radius,
          color,
          weight: 1,
          fillColor,
          fillOpacity: 0.68
        })
        .bindPopup(popupHtmlBuilder(p))
        .addTo(map);
      });
    }

    const mapTotal = L.map('bin_map_total', {zoomControl: true}).setView(binMapCenter, 10);
    L.tileLayer(tileUrl, {attribution: tileAttr, maxZoom: 19}).addTo(mapTotal);
    const maxOverallBin = overallBinPoints.reduce((mx, p) => Math.max(mx, p.stop_count || 0), 1);
    addTotalPointsToMap(
      mapTotal,
      overallBinPoints,
      maxOverallBin,
      (p) => `<b>${p.bin_id}</b><br/>${p.location_name}<br/>Primary display: ${p.primary_display_name}<br/>Total visits: ${p.stop_count}<br/>Grouped display names: ${p.display_name_count}`,
      '#6fe3ff',
      '#2ec4ff'
    );

    const routineMapTotal = L.map('routine_map_total', {zoomControl: true}).setView(routineMapCenter, 10);
    L.tileLayer(tileUrl, {attribution: tileAttr, maxZoom: 19}).addTo(routineMapTotal);
    const maxOverallRoutine = routinePoints.reduce((mx, p) => Math.max(mx, p.total_stop_count || 0), 1);
    addTotalPointsToMap(
      routineMapTotal,
      routinePoints,
      maxOverallRoutine,
      (p) => `<b>Routine</b><br/>${p.display_name}<br/>Total visits: ${p.total_stop_count}<br/>Max monthly: ${p.max_monthly_stop_count}`,
      '#ffd38a',
      '#f4b460'
    );

    function prettyMonth(monthKey) {
      if (!monthKey || monthKey.length !== 7) return monthKey || 'N/A';
      const year = monthKey.slice(0, 4);
      const monthNum = Number(monthKey.slice(5, 7));
      const names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
      const label = monthNum >= 1 && monthNum <= 12 ? names[monthNum - 1] : monthKey.slice(5, 7);
      return `${label} ${year}`;
    }

    function renderYearTicks(tickRow, labels) {
      tickRow.innerHTML = '';
      if (!labels || labels.length <= 1) return;
      labels.forEach((monthKey, idx) => {
        const pct = (idx / (labels.length - 1)) * 100;
        const tick = document.createElement('div');
        tick.style.position = 'absolute';
        tick.style.left = `${pct}%`;
        tick.style.top = '0';
        tick.style.width = '1px';
        tick.style.height = '8px';
        tick.style.background = '#5f6b7d';
        tickRow.appendChild(tick);

        if (monthKey.endsWith('-01') || idx === 0 || idx === labels.length - 1) {
          const yearLabel = document.createElement('div');
          yearLabel.textContent = monthKey.slice(0, 4);
          yearLabel.style.position = 'absolute';
          yearLabel.style.left = `${pct}%`;
          yearLabel.style.top = '8px';
          yearLabel.style.transform = 'translateX(-50%)';
          yearLabel.style.fontSize = '11px';
          yearLabel.style.color = '#a4b1c4';
          tickRow.appendChild(yearLabel);
        }
      });
    }

    function initMonthlyMap(opts) {
      const map = opts.map;
      const layer = opts.layer;
      const labels = opts.labels;
      const slider = opts.slider;
      const readout = opts.readout;
      const tickRow = opts.tickRow;
      const dataByMonth = opts.dataByMonth;
      const maxVisits = Math.max(1, opts.maxVisits || 1);
      const color = opts.color;
      const fillColor = opts.fillColor;
      const popupBuilder = opts.popupBuilder;

      function renderMonth(monthKey) {
        layer.clearLayers();
        const points = (dataByMonth && dataByMonth[monthKey]) ? dataByMonth[monthKey] : [];
        points.forEach((p) => {
          const scaled = Math.sqrt((p.visit_count || 0) / maxVisits);
          const radius = 4 + (16 * scaled);
          L.circleMarker([p.lat, p.lon], {
            radius,
            color,
            weight: 1,
            fillColor,
            fillOpacity: 0.68
          })
          .bindPopup(popupBuilder(p))
          .addTo(layer);
        });
        readout.textContent = `${opts.labelPrefix}: ${prettyMonth(monthKey)} | Points with visits: ${points.length}`;
      }

      if (!labels || labels.length === 0) {
        slider.disabled = true;
        readout.textContent = `No monthly data available for ${opts.labelPrefix.toLowerCase()}.`;
        renderYearTicks(tickRow, []);
        return;
      }

      slider.min = '0';
      slider.max = String(labels.length - 1);
      slider.step = '1';
      slider.value = String(labels.length - 1);
      renderYearTicks(tickRow, labels);

      const initialMonth = labels[Number(slider.value)];
      renderMonth(initialMonth);

      slider.addEventListener('input', () => {
        const monthKey = labels[Number(slider.value)];
        renderMonth(monthKey);
      });
    }

    const mapBinMonthly = L.map('bin_map_monthly', {zoomControl: true}).setView(binMapCenter, 10);
    L.tileLayer(tileUrl, {attribution: tileAttr, maxZoom: 19}).addTo(mapBinMonthly);
    const binMonthLayer = L.layerGroup().addTo(mapBinMonthly);

    initMonthlyMap({
      map: mapBinMonthly,
      layer: binMonthLayer,
      labels: binMonthLabels,
      slider: document.getElementById('bin_month_slider'),
      readout: document.getElementById('bin_month_readout'),
      tickRow: document.getElementById('bin_month_tick_row'),
      dataByMonth: monthlyBinMapData,
      maxVisits: monthlyBinMaxVisits,
      color: '#6fe3ff',
      fillColor: '#2ec4ff',
      labelPrefix: 'Month',
      popupBuilder: (p) => `<b>${p.bin_id}</b><br/>${p.location_name}<br/>Primary display: ${p.primary_display_name}<br/>Visits in month: ${p.visit_count}`,
    });

    const mapRoutineMonthly = L.map('routine_map_monthly', {zoomControl: true}).setView(routineMapCenter, 10);
    L.tileLayer(tileUrl, {attribution: tileAttr, maxZoom: 19}).addTo(mapRoutineMonthly);
    const routineMonthLayer = L.layerGroup().addTo(mapRoutineMonthly);

    initMonthlyMap({
      map: mapRoutineMonthly,
      layer: routineMonthLayer,
      labels: routineMonthLabels,
      slider: document.getElementById('routine_month_slider'),
      readout: document.getElementById('routine_month_readout'),
      tickRow: document.getElementById('routine_month_tick_row'),
      dataByMonth: monthlyRoutineMapData,
      maxVisits: monthlyRoutineMaxVisits,
      color: '#ffd38a',
      fillColor: '#f4b460',
      labelPrefix: 'Month',
      popupBuilder: (p) => `<b>Routine</b><br/>${p.display_name}<br/>Visits in month: ${p.visit_count}`,
    });

    const grid = document.querySelector('.grid');
    const cards = Array.from(document.querySelectorAll('.card'));
    const leafletMaps = {
      bin_map_total: mapTotal,
      bin_map_monthly: mapBinMonthly,
      routine_map_total: routineMapTotal,
      routine_map_monthly: mapRoutineMonthly,
    };
    let expandedCard = null;

    function resizeCardContent(card) {
      const content = card ? card.querySelector('.chart-content') : null;
      if (!content) return;
      if (content.classList.contains('map')) {
        const mapInst = leafletMaps[content.id];
        if (mapInst) mapInst.invalidateSize();
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
      content.dataset.defaultHeight = content.style.height || (content.classList.contains('map') ? '380px' : '320px');
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

    html = html.replace("__DATA_SOURCE__", data_path.name)
    html = html.replace("__CACHE_SOURCE__", cache_path.name)
    html = html.replace("__BINS_SOURCE__", bins_path.name)
    html = html.replace("__ROUTINE_SOURCE__", routine_path.name)
    html = html.replace("__SAVERS_SOURCE__", savers_path.name if savers_path.exists() else "data_savers.csv (missing)")

    html = html.replace("__TOTAL_STOPS__", str(total_stops))
    html = html.replace("__UNIQUE_BIN_ADDRESSES__", str(unique_bin_addresses))
    html = html.replace("__UNIQUE_BIN_LABELS__", str(unique_bin_labels))
    html = html.replace("__UNIQUE_BIN_CLUSTERS__", str(unique_bin_clusters))
    html = html.replace("__ROUTINE_UNIQUE_DONORS__", str(routine_unique_donors))
    html = html.replace("__ROUTINE_ONLY_GT20_DONORS__", str(routine_only_gt20_donors))
    html = html.replace("__ROUTINE_ONLY_GT3_MONTH_DONORS__", str(routine_only_gt3_month_donors))
    html = html.replace("__ROUTINE_BOTH_CRITERIA_DONORS__", str(routine_both_criteria_donors))
    html = html.replace("__SAVERS_UNIQUE_COUNT__", str(savers_unique_count))
    html = html.replace("__SAVERS_TOTAL_STOPS__", str(savers_total_stops))
    html = html.replace("__BIN_TOTAL_STOPS__", str(bin_assoc_stops))
    html = html.replace("__ROUTINE_TOTAL_STOPS__", str(routine_total_stops))
    html = html.replace("__OTHER_TOTAL_STOPS__", str(other_total_stops))
    html = html.replace("__TOTAL_LOCATIONS__", str(total_locations))
    html = html.replace("__BIN_LOCATION_COUNT__", str(bin_location_count))
    html = html.replace("__ROUTINE_LOCATION_COUNT__", str(routine_location_count))
    html = html.replace("__OTHER_LOCATION_COUNT__", str(other_location_count))

    html = html.replace("__BIN_STOP_COUNTS__", json.dumps(bin_stop_counts))
    html = html.replace("__ROUTINE_STOP_COUNTS__", json.dumps(routine_stop_counts))
    html = html.replace("__OTHER_STOP_COUNTS__", json.dumps(other_stop_counts))

    html = html.replace("__BIN_MAP_CENTER__", json.dumps(map_center))
    html = html.replace("__OVERALL_BIN_POINTS__", json.dumps(overall_bin_points))
    html = html.replace("__BIN_MONTH_LABELS__", json.dumps(bin_month_labels))
    html = html.replace("__MONTH_UNIQUE_BINS__", json.dumps(month_unique_bins))
    html = html.replace("__BIN_MONTH_TOTAL_STOPS__", json.dumps(bin_month_total_stops))
    html = html.replace("__MONTHLY_BIN_MAP_DATA__", json.dumps(monthly_bin_map_data))
    html = html.replace("__MONTHLY_BIN_MAX_VISITS__", json.dumps(monthly_max_visits))

    html = html.replace("__ROUTINE_MAP_CENTER__", json.dumps(routine_map_center))
    html = html.replace("__ROUTINE_POINTS__", json.dumps(routine_points))
    html = html.replace("__ROUTINE_MONTH_LABELS__", json.dumps(routine_month_labels))
    html = html.replace("__ROUTINE_MONTH_UNIQUE_CUSTOMERS__", json.dumps(routine_month_unique_customers))
    html = html.replace("__ROUTINE_MONTH_TOTAL_STOPS__", json.dumps(routine_month_total_stops))
    html = html.replace("__MONTHLY_ROUTINE_MAP_DATA__", json.dumps(monthly_routine_map_data))
    html = html.replace("__MONTHLY_ROUTINE_MAX_VISITS__", json.dumps(routine_monthly_max_visits))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        f.write(html)

    print(f"Wrote BIN dashboard: {output_path}")


if __name__ == "__main__":
    main()
