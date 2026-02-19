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
DEFAULT_SAVERS_SEED_CSV = SCRIPT_DIR / "persistent_savers_addresses.csv"
DEFAULT_SAVERS_CSV = SCRIPT_DIR / "data_savers.csv"

EARTH_RADIUS_M = 6371000.0
BIN_NEARBY_THRESHOLD_METERS = 100.0
SAVERS_NEARBY_THRESHOLD_METERS = 91.44  # 100 yards


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


def normalize_text_key(value: str) -> str:
    raw = str(value or "").strip()
    if raw == "":
        return ""
    return re.sub(r"\s+", " ", raw).strip().upper()


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


def identify_and_extract_savers(
    analysis_df: pd.DataFrame,
    savers_seed_path: Path,
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict[str, float]]:
    savers_output_columns = [
        "savers_cluster_id",
        "savers_id",
        "savers_seed_key",
        "location_name_primary",
        "location_norm",
        "primary_display_name",
        "other_display_names",
        "seed_display_names",
        "distance_display_names",
        "all_grouped_display_names",
        "grouped_display_name_count",
        "seed_match_stop_count",
        "location_label_match_stop_count",
        "distance_association_stop_count",
        "association_only_stop_count",
        "associated_stop_count",
        "primary_lat",
        "primary_lon",
        "centroid_lat",
        "centroid_lon",
        "max_distance_association_m",
    ]

    seed_df = pd.read_csv(savers_seed_path, dtype=str).fillna("")
    for col in ["address_raw", "latitude_raw", "longitude_raw", "display_name"]:
        if col not in seed_df.columns:
            seed_df[col] = ""

    seed_df = seed_df.copy()
    seed_df["seed_id"] = [f"SAVERS_SEED_{idx:03d}" for idx in range(1, len(seed_df) + 1)]
    seed_df["seed_display_name"] = seed_df["display_name"].astype(str).str.strip()
    seed_df["seed_display_norm"] = seed_df["seed_display_name"].map(normalize_text_key)
    seed_df["seed_address_norm"] = seed_df["address_raw"].map(normalize_text_key)
    seed_df["seed_lat"] = pd.to_numeric(seed_df["latitude_raw"], errors="coerce")
    seed_df["seed_lon"] = pd.to_numeric(seed_df["longitude_raw"], errors="coerce")

    display_to_seed_id: Dict[str, str] = {}
    address_to_seed_id: Dict[str, str] = {}
    seed_display_lookup: Dict[str, set[str]] = {}
    for row in seed_df.itertuples(index=False):
        seed_id = str(row.seed_id)
        display_name = str(row.seed_display_name).strip()
        display_norm = str(row.seed_display_norm).strip()
        address_norm = str(row.seed_address_norm).strip()

        if display_norm and display_norm not in display_to_seed_id:
            display_to_seed_id[display_norm] = seed_id
        if address_norm and address_norm not in address_to_seed_id:
            address_to_seed_id[address_norm] = seed_id
        if display_name:
            seed_display_lookup.setdefault(seed_id, set()).add(display_name)

    working_df = analysis_df.copy()
    working_df["display_norm"] = working_df["display_name_final"].map(normalize_text_key)
    working_df["address_norm"] = working_df["Address"].map(normalize_text_key)
    working_df["location_norm_savers"] = working_df["Location"].map(normalize_text_key)
    working_df["is_savers_location_label"] = working_df["location_norm_savers"].str.startswith(
        "SAVERS"
    )

    working_df["savers_cluster_key"] = working_df["display_norm"].map(display_to_seed_id).fillna("")
    missing_cluster_mask = working_df["savers_cluster_key"] == ""
    working_df.loc[missing_cluster_mask, "savers_cluster_key"] = (
        working_df.loc[missing_cluster_mask, "address_norm"].map(address_to_seed_id).fillna("")
    )
    working_df["is_savers_seed_match"] = working_df["savers_cluster_key"] != ""

    location_fallback_mask = working_df["is_savers_location_label"] & (
        working_df["savers_cluster_key"] == ""
    )
    working_df.loc[location_fallback_mask, "savers_cluster_key"] = (
        "SAVERS_LOC::" + working_df.loc[location_fallback_mask, "location_norm_savers"]
    )

    expansion_points: List[Tuple[str, float, float]] = []
    for row in seed_df.itertuples(index=False):
        if pd.notna(row.seed_lat) and pd.notna(row.seed_lon):
            expansion_points.append((str(row.seed_id), float(row.seed_lat), float(row.seed_lon)))

    initial_seed_rows = working_df.loc[
        (working_df["savers_cluster_key"] != "")
        & working_df["lat_final"].notna()
        & working_df["lon_final"].notna(),
        ["savers_cluster_key", "lat_final", "lon_final"],
    ]
    for row in initial_seed_rows.itertuples(index=False):
        expansion_points.append(
            (
                str(row.savers_cluster_key),
                float(row.lat_final),
                float(row.lon_final),
            )
        )

    working_df["savers_distance_m"] = pd.NA
    working_df["is_savers_by_distance_association"] = False
    distance_candidate_idx = working_df.index[
        (working_df["savers_cluster_key"] == "")
        & working_df["lat_final"].notna()
        & working_df["lon_final"].notna()
    ]
    if expansion_points:
        for idx in distance_candidate_idx:
            lat_val = float(working_df.at[idx, "lat_final"])
            lon_val = float(working_df.at[idx, "lon_final"])

            nearest_cluster_key = ""
            nearest_distance_m = float("inf")
            for cluster_key, seed_lat, seed_lon in expansion_points:
                distance_m = haversine_m(lat_val, lon_val, seed_lat, seed_lon)
                if distance_m < nearest_distance_m:
                    nearest_distance_m = distance_m
                    nearest_cluster_key = cluster_key

            if nearest_cluster_key and nearest_distance_m <= SAVERS_NEARBY_THRESHOLD_METERS:
                working_df.at[idx, "savers_cluster_key"] = nearest_cluster_key
                working_df.at[idx, "is_savers_by_distance_association"] = True
                working_df.at[idx, "savers_distance_m"] = float(nearest_distance_m)

    working_df["is_savers_associated"] = working_df["savers_cluster_key"] != ""
    savers_scope_df = working_df.loc[working_df["is_savers_associated"]].copy()

    savers_records: List[Dict[str, object]] = []
    if not savers_scope_df.empty:
        grouped = savers_scope_df.groupby("savers_cluster_key", sort=False)
        for cluster_key, cluster_df in grouped:
            display_counts = (
                cluster_df["display_name_final"]
                .astype(str)
                .str.strip()
                .replace("", pd.NA)
                .dropna()
                .value_counts()
            )
            all_display_names = sorted(display_counts.index.tolist())

            seed_display_names = set(seed_display_lookup.get(str(cluster_key), set()))
            seed_display_names.update(
                cluster_df.loc[cluster_df["is_savers_seed_match"], "display_name_final"]
                .astype(str)
                .str.strip()
                .replace("", pd.NA)
                .dropna()
                .tolist()
            )
            seed_display_names_sorted = sorted(seed_display_names)

            distance_display_names = sorted(
                set(
                    cluster_df.loc[
                        cluster_df["is_savers_by_distance_association"], "display_name_final"
                    ]
                    .astype(str)
                    .str.strip()
                    .replace("", pd.NA)
                    .dropna()
                    .tolist()
                )
            )

            if seed_display_names_sorted:
                primary_display_name = max(
                    seed_display_names_sorted,
                    key=lambda name: (int(display_counts.get(name, 0)), name),
                )
            elif all_display_names:
                primary_display_name = max(
                    all_display_names,
                    key=lambda name: (int(display_counts.get(name, 0)), name),
                )
            else:
                primary_display_name = ""
            other_display_names = [
                name for name in all_display_names if name != primary_display_name
            ]

            location_counts = (
                cluster_df["Location"]
                .astype(str)
                .str.strip()
                .replace("", pd.NA)
                .dropna()
                .value_counts()
            )
            location_name_primary = (
                str(location_counts.index[0]) if not location_counts.empty else str(cluster_key)
            )
            location_norm = normalize_text_key(location_name_primary)

            geo_rows = cluster_df.loc[
                cluster_df["lat_final"].notna() & cluster_df["lon_final"].notna()
            ]
            centroid_lat = float(geo_rows["lat_final"].median()) if not geo_rows.empty else None
            centroid_lon = float(geo_rows["lon_final"].median()) if not geo_rows.empty else None

            if primary_display_name:
                primary_geo_rows = cluster_df.loc[
                    (cluster_df["display_name_final"] == primary_display_name)
                    & cluster_df["lat_final"].notna()
                    & cluster_df["lon_final"].notna()
                ]
                primary_lat = (
                    float(primary_geo_rows["lat_final"].median())
                    if not primary_geo_rows.empty
                    else centroid_lat
                )
                primary_lon = (
                    float(primary_geo_rows["lon_final"].median())
                    if not primary_geo_rows.empty
                    else centroid_lon
                )
            else:
                primary_lat = centroid_lat
                primary_lon = centroid_lon

            max_distance_m_val = pd.to_numeric(
                cluster_df["savers_distance_m"], errors="coerce"
            ).max(skipna=True)
            max_distance_m = float(max_distance_m_val) if pd.notna(max_distance_m_val) else 0.0

            seed_match_stop_count = int(cluster_df["is_savers_seed_match"].sum())
            location_label_match_stop_count = int(cluster_df["is_savers_location_label"].sum())
            distance_association_stop_count = int(
                cluster_df["is_savers_by_distance_association"].sum()
            )
            association_only_stop_count = int(
                (
                    (~cluster_df["is_savers_seed_match"])
                    & (~cluster_df["is_savers_location_label"])
                ).sum()
            )
            associated_stop_count = int(len(cluster_df))

            savers_records.append(
                {
                    "savers_seed_key": str(cluster_key),
                    "location_name_primary": location_name_primary,
                    "location_norm": location_norm,
                    "primary_display_name": primary_display_name,
                    "other_display_names": json.dumps(other_display_names, ensure_ascii=False),
                    "seed_display_names": json.dumps(seed_display_names_sorted, ensure_ascii=False),
                    "distance_display_names": json.dumps(
                        distance_display_names, ensure_ascii=False
                    ),
                    "all_grouped_display_names": json.dumps(
                        all_display_names, ensure_ascii=False
                    ),
                    "grouped_display_name_count": int(len(all_display_names)),
                    "seed_match_stop_count": seed_match_stop_count,
                    "location_label_match_stop_count": location_label_match_stop_count,
                    "distance_association_stop_count": distance_association_stop_count,
                    "association_only_stop_count": association_only_stop_count,
                    "associated_stop_count": associated_stop_count,
                    "primary_lat": primary_lat,
                    "primary_lon": primary_lon,
                    "centroid_lat": centroid_lat,
                    "centroid_lon": centroid_lon,
                    "max_distance_association_m": round(max_distance_m, 2),
                }
            )

    savers_df = pd.DataFrame(savers_records)
    if savers_df.empty:
        savers_df = pd.DataFrame(columns=savers_output_columns)
    else:
        savers_df = savers_df.sort_values(
            ["associated_stop_count", "seed_match_stop_count", "savers_seed_key"],
            ascending=[False, False, True],
        ).reset_index(drop=True)
        savers_df["savers_id"] = [
            f"SAVERS{idx:03d}" for idx in range(1, len(savers_df) + 1)
        ]
        savers_df["savers_cluster_id"] = savers_df["savers_id"]
        savers_df = savers_df[savers_output_columns]

    filtered_analysis_df = analysis_df.loc[~working_df["is_savers_associated"]].copy()
    savers_metrics = {
        "savers_associated_stops": float(int(working_df["is_savers_associated"].sum())),
        "savers_seed_match_stops": float(int(working_df["is_savers_seed_match"].sum())),
        "savers_distance_associated_stops": float(
            int(working_df["is_savers_by_distance_association"].sum())
        ),
        "savers_clusters": float(len(savers_df)),
    }
    return filtered_analysis_df, savers_df, savers_metrics


def create_bins_data(
    data_path: Path,
    cache_path: Path,
    bins_output_path: Path,
    routine_output_path: Path,
    savers_seed_path: Path = DEFAULT_SAVERS_SEED_CSV,
    savers_output_path: Path = DEFAULT_SAVERS_CSV,
) -> Tuple[Dict[str, float], pd.DataFrame]:
    analysis_df = build_analysis_dataframe(data_path, cache_path)
    total_stops_all = float(len(analysis_df))
    analysis_df, savers_df, savers_metrics = identify_and_extract_savers(
        analysis_df=analysis_df,
        savers_seed_path=savers_seed_path,
    )

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
    savers_output_path.parent.mkdir(parents=True, exist_ok=True)
    savers_df.to_csv(savers_output_path, index=False)
    bins_df.to_csv(bins_output_path, index=False)
    routine_df.to_csv(routine_output_path, index=False)

    metrics = {
        "total_stops": total_stops_all,
        "bin_associated_stops": float(int(analysis_df["is_bin_associated"].sum())),
        "routine_total_stops": float(int(routine_df["total_stop_count"].sum() if not routine_df.empty else 0)),
        "unique_bins": float(len(bins_df)),
        "routine_unique_donors": float(len(routine_df)),
        "savers_associated_stops": savers_metrics["savers_associated_stops"],
        "savers_seed_match_stops": savers_metrics["savers_seed_match_stops"],
        "savers_distance_associated_stops": savers_metrics["savers_distance_associated_stops"],
        "savers_clusters": savers_metrics["savers_clusters"],
    }
    return metrics, non_bin_display_counts_df


def main() -> None:
    parser = argparse.ArgumentParser(description="Create BIN and routine donor tables")
    parser.add_argument("--data", default=str(first_existing(DATA_CANDIDATES)), help="Stop-level geocoded data CSV")
    parser.add_argument("--cache", default=str(first_existing(CACHE_CANDIDATES)), help="Geocode cache CSV")
    parser.add_argument(
        "--savers-seed",
        default=str(DEFAULT_SAVERS_SEED_CSV),
        help="Persistent Savers seed CSV (address/display/lat/lon)",
    )
    parser.add_argument(
        "--savers-output",
        default=str(DEFAULT_SAVERS_CSV),
        help="Output CSV path for Savers aggregates",
    )
    parser.add_argument("--bins-output", default=str(DEFAULT_BINS_CSV), help="Output CSV path for bins dataframe")
    parser.add_argument("--routine-output", default=str(DEFAULT_ROUTINE_CSV), help="Output CSV path for routine donor aggregates")
    args = parser.parse_args()

    data_path = Path(args.data).expanduser().resolve()
    cache_path = Path(args.cache).expanduser().resolve()
    savers_seed_path = Path(args.savers_seed).expanduser().resolve()
    savers_output_path = Path(args.savers_output).expanduser().resolve()
    bins_output_path = Path(args.bins_output).expanduser().resolve()
    routine_output_path = Path(args.routine_output).expanduser().resolve()

    if not data_path.exists():
        raise SystemExit(f"Data CSV not found: {data_path}")
    if not cache_path.exists():
        raise SystemExit(f"Cache CSV not found: {cache_path}")
    if not savers_seed_path.exists():
        raise SystemExit(f"Savers seed CSV not found: {savers_seed_path}")

    metrics, non_bin_display_counts_df = create_bins_data(
        data_path=data_path,
        cache_path=cache_path,
        savers_seed_path=savers_seed_path,
        savers_output_path=savers_output_path,
        bins_output_path=bins_output_path,
        routine_output_path=routine_output_path,
    )

    print(f"Wrote savers CSV: {savers_output_path}")
    print(f"Wrote bins CSV: {bins_output_path}")
    print(f"Wrote routine CSV: {routine_output_path}")
    print(
        "Summary: "
        f"stops={int(metrics['total_stops'])}, "
        f"savers_stops={int(metrics['savers_associated_stops'])}, "
        f"savers_clusters={int(metrics['savers_clusters'])}, "
        f"bins={int(metrics['unique_bins'])}, "
        f"routine_donors={int(metrics['routine_unique_donors'])}"
    )

    print("\nNon-bin display names sorted by total visits (stop_count descending):")
    for rank, row in enumerate(non_bin_display_counts_df.itertuples(index=False), start=1):
        print(f"{rank:6d}. {int(row.stop_count):6d} | {row.display_name_final}")


if __name__ == "__main__":
    main()
