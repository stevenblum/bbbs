#!/usr/bin/env python3
"""Create BIN and routine donor tables from stop-level geocode data."""

import argparse
import json
import math
import re
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

try:
    import pandas as pd
except ImportError as exc:
    raise SystemExit(
        "This script requires pandas. Run with the project venv, for example: "
        "./.venv/bin/python visualizations/create_bins_data.py"
    ) from exc

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

DATA_CANDIDATES: List[Path] = [
    PROJECT_ROOT / "data_geocode" / "latest" / "data_geocoded.csv",
    PROJECT_ROOT / "data_geocode" / "latest" / "data_geocode.csv",
]
CACHE_CANDIDATES: List[Path] = [
    PROJECT_ROOT / "data_geocode" / "latest" / "geocoded_address_cache.csv",
    PROJECT_ROOT / "data_geocode" / "latest" / "geocode_address_cache.csv",
]

DEFAULT_BINS_CSV = SCRIPT_DIR / "data_bins.csv"
DEFAULT_ROUTINE_CSV = SCRIPT_DIR / "data_routine.csv"

EARTH_RADIUS_M = 6371000.0
BIN_NEARBY_THRESHOLD_METERS = 100.0


def first_existing(paths: Sequence[Path]) -> Path:
    for path in paths:
        if path.exists():
            return path
    raise FileNotFoundError("None of the expected files exist: " + ", ".join(str(p) for p in paths))


def normalize_location_name(value: str) -> str:
    raw = str(value or "").strip()
    if raw == "":
        return ""
    cleaned = re.sub(r"^\s*BIN[\s:_-]*", "", raw, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r"\s+", " ", cleaned).strip().upper()
    return cleaned if cleaned else raw.upper()


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    lat1_rad = math.radians(lat1)
    lon1_rad = math.radians(lon1)
    lat2_rad = math.radians(lat2)
    lon2_rad = math.radians(lon2)
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad
    a = (
        math.sin(dlat / 2.0) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2.0) ** 2
    )
    return 2.0 * EARTH_RADIUS_M * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))


def mean_lat_lon(points: List[Tuple[float, float]]) -> Tuple[Optional[float], Optional[float]]:
    if not points:
        return None, None
    return (
        sum(point[0] for point in points) / len(points),
        sum(point[1] for point in points) / len(points),
    )


def build_analysis_dataframe(data_path: Path, cache_path: Path) -> pd.DataFrame:
    data_df = pd.read_csv(data_path, dtype=str)
    for col in [
        "Location",
        "Address",
        "display_name",
        "latitude",
        "longitude",
        "Planned Date",
        "Actual Date",
    ]:
        if col not in data_df.columns:
            data_df[col] = ""

    cache_df = pd.read_csv(
        cache_path,
        usecols=["address_raw", "address_nominatim", "latitude", "longitude"],
        dtype=str,
    )

    data_df = data_df.fillna("")
    cache_df = cache_df.fillna("")

    cache_df = cache_df.rename(
        columns={
            "address_raw": "cache_address_raw",
            "address_nominatim": "cache_display_name",
            "latitude": "cache_latitude",
            "longitude": "cache_longitude",
        }
    )
    cache_df = cache_df.drop_duplicates(subset=["cache_address_raw"], keep="first")

    analysis_df = data_df.merge(
        cache_df,
        how="left",
        left_on="Address",
        right_on="cache_address_raw",
    )

    for col in [
        "Location",
        "display_name",
        "cache_display_name",
        "latitude",
        "longitude",
        "cache_latitude",
        "cache_longitude",
    ]:
        if col not in analysis_df.columns:
            analysis_df[col] = ""
        analysis_df[col] = analysis_df[col].fillna("").astype(str)

    analysis_df["display_name"] = analysis_df["display_name"].str.strip()
    analysis_df["cache_display_name"] = analysis_df["cache_display_name"].str.strip()
    analysis_df["display_name_final"] = analysis_df["display_name"]
    empty_display_mask = analysis_df["display_name_final"] == ""
    analysis_df.loc[empty_display_mask, "display_name_final"] = analysis_df.loc[
        empty_display_mask, "cache_display_name"
    ]

    analysis_df["latitude_num"] = pd.to_numeric(analysis_df["latitude"], errors="coerce")
    analysis_df["longitude_num"] = pd.to_numeric(analysis_df["longitude"], errors="coerce")
    analysis_df["cache_latitude_num"] = pd.to_numeric(analysis_df["cache_latitude"], errors="coerce")
    analysis_df["cache_longitude_num"] = pd.to_numeric(analysis_df["cache_longitude"], errors="coerce")

    analysis_df["lat_final"] = analysis_df["latitude_num"].fillna(analysis_df["cache_latitude_num"])
    analysis_df["lon_final"] = analysis_df["longitude_num"].fillna(analysis_df["cache_longitude_num"])

    return analysis_df


def create_bins_data(
    data_path: Path,
    cache_path: Path,
    bins_output_path: Path,
    routine_output_path: Path,
) -> Tuple[Dict[str, float], pd.DataFrame]:
    analysis_df = build_analysis_dataframe(data_path, cache_path)

    analysis_df["location_norm"] = analysis_df["Location"].map(normalize_location_name)
    analysis_df["is_bin_labeled"] = analysis_df["Location"].str.strip().str.upper().str.startswith("BIN")

    display_points_df = (
        analysis_df.loc[analysis_df["display_name_final"] != ""]
        .groupby("display_name_final", as_index=False)
        .agg(
            display_total_stops=("display_name_final", "size"),
            display_labeled_stops=("is_bin_labeled", "sum"),
            lat=("lat_final", "median"),
            lon=("lon_final", "median"),
        )
    )
    display_points_df["lat"] = pd.to_numeric(display_points_df["lat"], errors="coerce")
    display_points_df["lon"] = pd.to_numeric(display_points_df["lon"], errors="coerce")

    display_total_stop_lookup = dict(
        zip(display_points_df["display_name_final"], display_points_df["display_total_stops"])
    )
    display_labeled_stop_lookup = dict(
        zip(display_points_df["display_name_final"], display_points_df["display_labeled_stops"])
    )
    display_lat_lookup = dict(zip(display_points_df["display_name_final"], display_points_df["lat"]))
    display_lon_lookup = dict(zip(display_points_df["display_name_final"], display_points_df["lon"]))

    seed_rows = analysis_df.loc[analysis_df["is_bin_labeled"]].copy()
    seed_rows_with_display = seed_rows.loc[seed_rows["display_name_final"] != ""].copy()

    seed_display_to_location: Dict[str, str] = {}
    if not seed_rows_with_display.empty:
        seed_display_location_counts = (
            seed_rows_with_display.groupby(["display_name_final", "location_norm"], as_index=False)
            .size()
            .rename(columns={"size": "seed_count"})
        )
        seed_display_location_counts = seed_display_location_counts.sort_values(
            ["display_name_final", "seed_count", "location_norm"],
            ascending=[True, False, True],
        )
        dominant_seed_locations = seed_display_location_counts.drop_duplicates(
            subset=["display_name_final"], keep="first"
        )
        seed_display_to_location = dict(
            zip(
                dominant_seed_locations["display_name_final"],
                dominant_seed_locations["location_norm"],
            )
        )

    location_to_seed_displays: Dict[str, List[str]] = {}
    for display_name, location_key in seed_display_to_location.items():
        location_to_seed_displays.setdefault(location_key, []).append(display_name)

    seed_location_keys = sorted(set(seed_rows["location_norm"].dropna().tolist()))
    seed_location_keys = [key for key in seed_location_keys if str(key).strip() != ""]

    bins_meta: List[Dict[str, object]] = []
    for location_key in seed_location_keys:
        location_rows = seed_rows.loc[seed_rows["location_norm"] == location_key]
        if location_rows.empty:
            continue

        location_counts = (
            location_rows["Location"].astype(str).str.strip().replace("", pd.NA).dropna().value_counts()
        )
        location_name_primary = (
            str(location_counts.index[0]) if not location_counts.empty else str(location_key)
        )

        seed_display_names = sorted(location_to_seed_displays.get(location_key, []))
        if seed_display_names:
            primary_display_name = max(
                seed_display_names,
                key=lambda name: (
                    int(display_labeled_stop_lookup.get(name, 0)),
                    int(display_total_stop_lookup.get(name, 0)),
                    name,
                ),
            )
        else:
            primary_display_name = ""

        seed_coords: List[Tuple[float, float]] = []
        for display_name in seed_display_names:
            lat_val = display_lat_lookup.get(display_name)
            lon_val = display_lon_lookup.get(display_name)
            if pd.notna(lat_val) and pd.notna(lon_val):
                seed_coords.append((float(lat_val), float(lon_val)))

        if seed_coords:
            centroid_lat, centroid_lon = mean_lat_lon(seed_coords)
        else:
            geo_rows = location_rows.loc[
                location_rows["lat_final"].notna() & location_rows["lon_final"].notna()
            ]
            centroid_lat = float(geo_rows["lat_final"].median()) if not geo_rows.empty else None
            centroid_lon = float(geo_rows["lon_final"].median()) if not geo_rows.empty else None

        bins_meta.append(
            {
                "location_norm": str(location_key),
                "location_name_primary": location_name_primary,
                "primary_display_name": primary_display_name,
                "seed_display_names": set(seed_display_names),
                "distance_display_names": set(),
                "centroid_lat": centroid_lat,
                "centroid_lon": centroid_lon,
            }
        )

    bins_meta = sorted(
        bins_meta,
        key=lambda item: (
            str(item["location_name_primary"]).upper(),
            str(item["location_norm"]).upper(),
        ),
    )

    location_to_bin_id: Dict[str, str] = {}
    bin_lookup: Dict[str, Dict[str, object]] = {}
    seed_display_to_bin_id: Dict[str, str] = {}
    for idx, bin_meta in enumerate(bins_meta, start=1):
        bin_id = f"BIN{idx:03d}"
        bin_meta["bin_id"] = bin_id
        location_to_bin_id[str(bin_meta["location_norm"])] = bin_id
        bin_lookup[bin_id] = bin_meta
        for display_name in bin_meta["seed_display_names"]:
            seed_display_to_bin_id[str(display_name)] = bin_id

    candidate_points_df = display_points_df.loc[
        (display_points_df["display_labeled_stops"] == 0)
        & (~display_points_df["display_name_final"].isin(seed_display_to_bin_id.keys()))
        & display_points_df["lat"].notna()
        & display_points_df["lon"].notna()
    ].copy()

    bin_centers: List[Tuple[str, float, float]] = []
    for bin_id, bin_meta in bin_lookup.items():
        lat_val = bin_meta.get("centroid_lat")
        lon_val = bin_meta.get("centroid_lon")
        if lat_val is None or lon_val is None:
            continue
        bin_centers.append((bin_id, float(lat_val), float(lon_val)))

    distance_display_to_bin_id: Dict[str, str] = {}
    distance_display_to_meters: Dict[str, float] = {}
    for _, row in candidate_points_df.iterrows():
        if not bin_centers:
            break

        display_name = str(row["display_name_final"])
        lat_val = float(row["lat"])
        lon_val = float(row["lon"])

        nearest_bin_id = ""
        nearest_distance = float("inf")
        for bin_id, bin_lat, bin_lon in bin_centers:
            distance_m = haversine_m(lat_val, lon_val, bin_lat, bin_lon)
            if distance_m < nearest_distance:
                nearest_distance = distance_m
                nearest_bin_id = bin_id

        if nearest_bin_id and nearest_distance <= BIN_NEARBY_THRESHOLD_METERS:
            distance_display_to_bin_id[display_name] = nearest_bin_id
            distance_display_to_meters[display_name] = nearest_distance
            bin_lookup[nearest_bin_id]["distance_display_names"].add(display_name)

    display_to_bin_id: Dict[str, str] = dict(seed_display_to_bin_id)
    display_to_bin_id.update(distance_display_to_bin_id)

    analysis_df["bin_cluster_id"] = analysis_df["display_name_final"].map(display_to_bin_id).fillna("")
    unassigned_labeled_mask = analysis_df["is_bin_labeled"] & (analysis_df["bin_cluster_id"] == "")
    analysis_df.loc[unassigned_labeled_mask, "bin_cluster_id"] = (
        analysis_df.loc[unassigned_labeled_mask, "location_norm"].map(location_to_bin_id).fillna("")
    )

    analysis_df["is_bin_seed_display"] = analysis_df["display_name_final"].isin(
        seed_display_to_bin_id.keys()
    )
    analysis_df["is_bin_by_display_association"] = (
        analysis_df["is_bin_seed_display"] & ~analysis_df["is_bin_labeled"]
    )
    analysis_df["is_bin_by_distance_association"] = analysis_df["display_name_final"].isin(
        distance_display_to_bin_id.keys()
    )
    analysis_df["is_bin_associated"] = (
        analysis_df["is_bin_labeled"]
        | analysis_df["is_bin_by_display_association"]
        | analysis_df["is_bin_by_distance_association"]
    )
    analysis_df["is_bin"] = analysis_df["is_bin_associated"]
    analysis_df["bin_id"] = analysis_df["bin_cluster_id"]

    bin_records: List[Dict[str, object]] = []
    for bin_id, bin_meta in sorted(bin_lookup.items(), key=lambda item: item[0]):
        seed_display_names = sorted(str(v) for v in bin_meta["seed_display_names"])
        distance_display_names = sorted(str(v) for v in bin_meta["distance_display_names"])
        all_display_names = sorted(set(seed_display_names) | set(distance_display_names))

        primary_display_name = str(bin_meta["primary_display_name"])
        if primary_display_name == "" and all_display_names:
            primary_display_name = max(
                all_display_names,
                key=lambda name: (
                    int(display_labeled_stop_lookup.get(name, 0)),
                    int(display_total_stop_lookup.get(name, 0)),
                    name,
                ),
            )
        other_display_names = [name for name in all_display_names if name != primary_display_name]

        bin_rows_df = analysis_df.loc[analysis_df["bin_cluster_id"] == bin_id].copy()
        associated_stop_count = int(len(bin_rows_df))
        labeled_stop_count = int(bin_rows_df["is_bin_labeled"].sum()) if associated_stop_count else 0
        association_only_stop_count = associated_stop_count - labeled_stop_count
        display_association_stop_count = (
            int(bin_rows_df["is_bin_by_display_association"].sum()) if associated_stop_count else 0
        )
        distance_association_stop_count = (
            int(bin_rows_df["is_bin_by_distance_association"].sum()) if associated_stop_count else 0
        )

        geo_rows = bin_rows_df.loc[bin_rows_df["lat_final"].notna() & bin_rows_df["lon_final"].notna()]
        if not geo_rows.empty:
            centroid_lat = float(geo_rows["lat_final"].median())
            centroid_lon = float(geo_rows["lon_final"].median())
        else:
            centroid_lat = float(bin_meta["centroid_lat"]) if bin_meta["centroid_lat"] is not None else None
            centroid_lon = float(bin_meta["centroid_lon"]) if bin_meta["centroid_lon"] is not None else None

        primary_lat = display_lat_lookup.get(primary_display_name)
        primary_lon = display_lon_lookup.get(primary_display_name)
        primary_lat = float(primary_lat) if pd.notna(primary_lat) else None
        primary_lon = float(primary_lon) if pd.notna(primary_lon) else None

        max_distance_m = max(
            [distance_display_to_meters.get(name, 0.0) for name in distance_display_names],
            default=0.0,
        )

        bin_records.append(
            {
                "bin_cluster_id": bin_id,
                "bin_id": bin_id,
                "location_name_primary": str(bin_meta["location_name_primary"]),
                "location_norm": str(bin_meta["location_norm"]),
                "primary_display_name": primary_display_name,
                "other_display_names": json.dumps(other_display_names, ensure_ascii=False),
                "seed_display_names": json.dumps(seed_display_names, ensure_ascii=False),
                "distance_display_names": json.dumps(distance_display_names, ensure_ascii=False),
                "all_grouped_display_names": json.dumps(all_display_names, ensure_ascii=False),
                "grouped_display_name_count": int(len(all_display_names)),
                "labeled_stop_count": labeled_stop_count,
                "display_association_stop_count": display_association_stop_count,
                "distance_association_stop_count": distance_association_stop_count,
                "association_only_stop_count": association_only_stop_count,
                "associated_stop_count": associated_stop_count,
                "primary_lat": primary_lat,
                "primary_lon": primary_lon,
                "centroid_lat": centroid_lat,
                "centroid_lon": centroid_lon,
                "max_distance_association_m": round(float(max_distance_m), 2),
            }
        )

    bins_df = pd.DataFrame(bin_records)
    if not bins_df.empty:
        bins_df = bins_df.loc[bins_df["associated_stop_count"] > 0].copy()
        bins_df = bins_df.sort_values(
            ["associated_stop_count", "labeled_stop_count", "bin_cluster_id"],
            ascending=[False, False, True],
        )

    analysis_df["actual_date_parsed"] = pd.to_datetime(analysis_df.get("Actual Date", ""), errors="coerce")
    analysis_df["planned_date_parsed"] = pd.to_datetime(analysis_df.get("Planned Date", ""), errors="coerce")
    analysis_df["visit_date"] = analysis_df["actual_date_parsed"].fillna(analysis_df["planned_date_parsed"])
    analysis_df["visit_month"] = analysis_df["visit_date"].dt.to_period("M").astype(str)
    analysis_df.loc[analysis_df["visit_date"].isna(), "visit_month"] = ""

    non_bin_scope_df = analysis_df.loc[
        (~analysis_df["is_bin_associated"]) & (analysis_df["display_name_final"] != "")
    ].copy()
    non_bin_display_counts_df = (
        non_bin_scope_df.groupby("display_name_final", as_index=False)
        .size()
        .rename(columns={"size": "stop_count"})
        .sort_values(["stop_count", "display_name_final"], ascending=[False, True])
    )

    routine_df = pd.DataFrame(
        columns=[
            "display_name_final",
            "total_stop_count",
            "max_monthly_stop_count",
            "is_routine_total_gt20",
            "is_routine_month_gt3",
            "is_routine",
            "lat",
            "lon",
        ]
    )

    if not non_bin_scope_df.empty:
        non_bin_total_counts_df = (
            non_bin_scope_df.groupby("display_name_final", as_index=False)
            .size()
            .rename(columns={"size": "total_stop_count"})
        )

        non_bin_monthly_counts_df = (
            non_bin_scope_df.loc[non_bin_scope_df["visit_month"] != ""]
            .groupby(["display_name_final", "visit_month"], as_index=False)
            .size()
            .rename(columns={"size": "visit_count"})
        )

        max_monthly_df = (
            non_bin_monthly_counts_df.groupby("display_name_final", as_index=False)["visit_count"]
            .max()
            .rename(columns={"visit_count": "max_monthly_stop_count"})
        )

        routine_candidates_df = non_bin_total_counts_df.merge(
            max_monthly_df,
            how="left",
            on="display_name_final",
        )
        routine_candidates_df["max_monthly_stop_count"] = (
            pd.to_numeric(routine_candidates_df["max_monthly_stop_count"], errors="coerce")
            .fillna(0)
            .astype(int)
        )
        routine_candidates_df["is_routine_total_gt20"] = routine_candidates_df["total_stop_count"] > 20
        routine_candidates_df["is_routine_month_gt3"] = routine_candidates_df["max_monthly_stop_count"] > 3
        routine_candidates_df["is_routine"] = (
            routine_candidates_df["is_routine_total_gt20"]
            | routine_candidates_df["is_routine_month_gt3"]
        )

        routine_df = routine_candidates_df.loc[routine_candidates_df["is_routine"]].copy()
        routine_df = routine_df.merge(
            display_points_df[["display_name_final", "lat", "lon"]],
            how="left",
            on="display_name_final",
        )
        routine_df = routine_df.sort_values(
            ["total_stop_count", "max_monthly_stop_count", "display_name_final"],
            ascending=[False, False, True],
        )

    bins_output_path.parent.mkdir(parents=True, exist_ok=True)
    routine_output_path.parent.mkdir(parents=True, exist_ok=True)
    bins_df.to_csv(bins_output_path, index=False)
    routine_df.to_csv(routine_output_path, index=False)

    metrics = {
        "total_stops": float(len(analysis_df)),
        "bin_associated_stops": float(int(analysis_df["is_bin_associated"].sum())),
        "routine_total_stops": float(int(routine_df["total_stop_count"].sum() if not routine_df.empty else 0)),
        "unique_bins": float(len(bins_df)),
        "routine_unique_donors": float(len(routine_df)),
    }
    return metrics, non_bin_display_counts_df


def main() -> None:
    parser = argparse.ArgumentParser(description="Create BIN and routine donor tables")
    parser.add_argument("--data", default=str(first_existing(DATA_CANDIDATES)), help="Stop-level geocoded data CSV")
    parser.add_argument("--cache", default=str(first_existing(CACHE_CANDIDATES)), help="Geocode cache CSV")
    parser.add_argument("--bins-output", default=str(DEFAULT_BINS_CSV), help="Output CSV path for bins dataframe")
    parser.add_argument("--routine-output", default=str(DEFAULT_ROUTINE_CSV), help="Output CSV path for routine donor aggregates")
    args = parser.parse_args()

    data_path = Path(args.data).expanduser().resolve()
    cache_path = Path(args.cache).expanduser().resolve()
    bins_output_path = Path(args.bins_output).expanduser().resolve()
    routine_output_path = Path(args.routine_output).expanduser().resolve()

    if not data_path.exists():
        raise SystemExit(f"Data CSV not found: {data_path}")
    if not cache_path.exists():
        raise SystemExit(f"Cache CSV not found: {cache_path}")

    metrics, non_bin_display_counts_df = create_bins_data(
        data_path=data_path,
        cache_path=cache_path,
        bins_output_path=bins_output_path,
        routine_output_path=routine_output_path,
    )

    print(f"Wrote bins CSV: {bins_output_path}")
    print(f"Wrote routine CSV: {routine_output_path}")
    print(
        "Summary: "
        f"stops={int(metrics['total_stops'])}, "
        f"bins={int(metrics['unique_bins'])}, "
        f"routine_donors={int(metrics['routine_unique_donors'])}"
    )

    print("\nNon-bin display names sorted by total visits (stop_count descending):")
    for rank, row in enumerate(non_bin_display_counts_df.itertuples(index=False), start=1):
        print(f"{rank:6d}. {int(row.stop_count):6d} | {row.display_name_final}")


if __name__ == "__main__":
    main()

