#!/usr/bin/env python3
"""Build optimizer-ready problem instances from geocoded stop-level data.

Step-by-step:
1. Load geocoded stop rows (`data_geocode.csv`) and parse dates/coordinates.
2. Load BIN and routine reference tables and build alias-to-primary name maps.
3. Canonicalize each stop to a primary display name.
4. Filter stops to the target date range and compute:
   - `visits_in_range` per canonical stop.
   - `mean_stop_duration` (minutes) per canonical stop from all in-range stop rows.
5. Compute lookback features (`stops_in_previous_7`, `stops_in_previous_30`).
6. Add active BIN/routine stops with recent activity even if not in range
   (`visits_in_range` becomes 0 for those).
7. Freeze stop order and query OSRM table API for pairwise travel matrices.
8. Write a JSON payload to `problem_instance_YYYY_MM_DD.csv`.

OSRM unit notes:
- Duration values are in seconds.
- Distance values are in meters.

Example terminal call:
  ./.venv/bin/python optimize/create_problem_instances.py \
    --start-date 2024-11-01 \
    --end-date 2024-11-01 \
    --include-all-active false \
    --output optimize/problem_instance_2024_11_01.csv
"""

from __future__ import annotations

import argparse
import ast
import json
import math
from datetime import timedelta
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

try:
    import pandas as pd
except ImportError as exc:
    raise SystemExit(
        "This script requires pandas. Run with the project venv, for example: "
        "./.venv/bin/python optimize/create_problem_instances.py"
    ) from exc


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

START_DATE = "2024-11-01"
END_DATE = "2024-11-01"

DEFAULT_GEOCODE_CSV = PROJECT_ROOT / "data_geocode" / "latest" / "data_geocode.csv"
DEFAULT_BINS_CSV = PROJECT_ROOT / "visualizations" / "data_bins.csv"
DEFAULT_ROUTINE_CSV = PROJECT_ROOT / "visualizations" / "data_routine.csv"
DEFAULT_OSRM_BASE_URL = "http://localhost:5000"
DEFAULT_REQUEST_TIMEOUT_SECONDS = 30
DEFAULT_TABLE_CHUNK_SIZE = 40

LOG_PREFIX = "[create_problem_instances]"
DEPOT_DISPLAY_NAME = "DEPOT, OLD PLAINFEILD PIKE AND TUNK HILL ROAD"
DEPOT_LAT = 41.766160818147014
DEPOT_LON = -71.63312226854363


def log_debug(message: str, verbose: bool = True) -> None:
    if verbose:
        print(f"{LOG_PREFIX} {message}")


def normalize_display_name(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    return text.casefold() if text else ""


def parse_bool(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "t", "yes", "y"}


def parse_list_field(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, float) and pd.isna(value):
        return []

    text = str(value).strip()
    if text == "" or text == "[]":
        return []

    parsed: Any
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        try:
            parsed = ast.literal_eval(text)
        except (SyntaxError, ValueError):
            return []

    if not isinstance(parsed, list):
        return []

    output: list[str] = []
    for item in parsed:
        item_text = str(item).strip()
        if item_text:
            output.append(item_text)
    return output


def parse_date_arg(value: str, arg_name: str) -> pd.Timestamp:
    parsed = pd.to_datetime(value, format="%Y-%m-%d", errors="coerce")
    if pd.isna(parsed):
        raise SystemExit(f"Invalid {arg_name}: {value!r}. Use YYYY-MM-DD.")
    return parsed.normalize()


def to_float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number):
        return None
    return number


def parse_duration_minutes(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None

    if ":" in text:
        parts = text.split(":")
        try:
            if len(parts) == 3:
                hours = float(parts[0])
                minutes = float(parts[1])
                seconds = float(parts[2])
                total = (hours * 60.0) + minutes + (seconds / 60.0)
                return None if math.isnan(total) else total
            if len(parts) == 2:
                minutes = float(parts[0])
                seconds = float(parts[1])
                total = minutes + (seconds / 60.0)
                return None if math.isnan(total) else total
        except ValueError:
            return None

    try:
        number = float(text)
    except ValueError:
        return None
    if math.isnan(number):
        return None
    return number


def add_alias(alias_map: dict[str, str], alias: str, primary: str) -> None:
    alias_key = normalize_display_name(alias)
    if not alias_key:
        return
    if alias_key not in alias_map:
        alias_map[alias_key] = primary


def load_geocode_dataframe(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str).fillna("")

    for col in ["display_name", "Planned Date", "Actual Date", "latitude", "longitude"]:
        if col not in df.columns:
            df[col] = ""

    df["display_name_final"] = df["display_name"].astype(str).str.strip()
    df["display_name_key"] = df["display_name_final"].map(normalize_display_name)

    df["actual_date_parsed"] = pd.to_datetime(df["Actual Date"], errors="coerce")
    df["planned_date_parsed"] = pd.to_datetime(df["Planned Date"], errors="coerce")
    df["visit_date"] = df["actual_date_parsed"].fillna(df["planned_date_parsed"])
    df["visit_date"] = df["visit_date"].dt.normalize()

    df["planned_stop_number_num"] = pd.to_numeric(df.get("Planned Stop Number", ""), errors="coerce")
    df["actual_stop_number_num"] = pd.to_numeric(df.get("Actual Stop Number", ""), errors="coerce")
    df["latitude_num"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude_num"] = pd.to_numeric(df["longitude"], errors="coerce")

    return df


def load_bins_metadata(path: Path) -> tuple[pd.DataFrame, dict[str, str], list[str], dict[str, dict[str, Any]]]:
    bins_df = pd.read_csv(path, dtype=str).fillna("")
    alias_to_primary: dict[str, str] = {}
    primary_order: list[str] = []
    meta_by_primary: dict[str, dict[str, Any]] = {}

    for _, row in bins_df.iterrows():
        primary = str(row.get("primary_display_name", "")).strip()
        if not primary:
            continue

        if primary not in meta_by_primary:
            meta_by_primary[primary] = {
                "bin_id": str(row.get("bin_id", "")).strip(),
                "bin_cluster_id": str(row.get("bin_cluster_id", "")).strip(),
                "location_name_primary": str(row.get("location_name_primary", "")).strip(),
                "location_norm": str(row.get("location_norm", "")).strip(),
                "primary_lat": to_float_or_none(row.get("primary_lat")),
                "primary_lon": to_float_or_none(row.get("primary_lon")),
                "associated_stop_count": to_float_or_none(row.get("associated_stop_count")),
            }
            primary_order.append(primary)

        add_alias(alias_to_primary, primary, primary)
        for list_col in (
            "all_grouped_display_names",
            "seed_display_names",
            "distance_display_names",
            "other_display_names",
        ):
            for alias in parse_list_field(row.get(list_col)):
                add_alias(alias_to_primary, alias, primary)

    return bins_df, alias_to_primary, primary_order, meta_by_primary


def load_routine_metadata(
    path: Path,
) -> tuple[pd.DataFrame, dict[str, str], list[str], dict[str, dict[str, Any]]]:
    routine_df = pd.read_csv(path, dtype=str).fillna("")
    alias_to_primary: dict[str, str] = {}
    primary_order: list[str] = []
    meta_by_primary: dict[str, dict[str, Any]] = {}

    for _, row in routine_df.iterrows():
        is_routine = parse_bool(row.get("is_routine", True))
        if not is_routine:
            continue

        primary = str(row.get("display_name_final", "")).strip()
        if not primary:
            primary = str(row.get("primary_display_name", "")).strip()
        if not primary:
            continue

        if primary not in meta_by_primary:
            meta_by_primary[primary] = {
                "total_stop_count": to_float_or_none(row.get("total_stop_count")),
                "max_monthly_stop_count": to_float_or_none(row.get("max_monthly_stop_count")),
                "lat": to_float_or_none(row.get("lat")),
                "lon": to_float_or_none(row.get("lon")),
            }
            primary_order.append(primary)

        add_alias(alias_to_primary, primary, primary)
        for list_col in ("all_grouped_display_names", "other_display_names"):
            if list_col in row.index:
                for alias in parse_list_field(row.get(list_col)):
                    add_alias(alias_to_primary, alias, primary)

    return routine_df, alias_to_primary, primary_order, meta_by_primary


def canonicalize_display_name(display_name: str, alias_to_primary: dict[str, str]) -> str:
    text = str(display_name or "").strip()
    if not text:
        return ""
    key = normalize_display_name(text)
    return alias_to_primary.get(key, text)


def iter_chunks(indices: list[int], chunk_size: int) -> list[list[int]]:
    return [indices[i : i + chunk_size] for i in range(0, len(indices), chunk_size)]


def fetch_osrm_submatrix(
    source_coords: list[tuple[float, float]],
    dest_coords: list[tuple[float, float]],
    osrm_base_url: str,
    request_timeout_seconds: int,
) -> tuple[list[list[float | None]], list[list[float | None]]]:
    all_coords = source_coords + dest_coords
    coord_segment = ";".join(f"{lon},{lat}" for lat, lon in all_coords)
    source_indices = ";".join(str(i) for i in range(len(source_coords)))
    dest_indices = ";".join(str(len(source_coords) + i) for i in range(len(dest_coords)))

    query = urlencode(
        {
            "annotations": "duration,distance",
            "sources": source_indices,
            "destinations": dest_indices,
        }
    )
    url = f"{osrm_base_url.rstrip('/')}/table/v1/driving/{coord_segment}?{query}"

    try:
        with urlopen(url, timeout=request_timeout_seconds) as response:
            payload = json.load(response)
    except HTTPError as exc:
        raise RuntimeError(f"OSRM table request failed with status {exc.code}") from exc
    except URLError as exc:
        raise RuntimeError(
            f"Could not connect to OSRM at {osrm_base_url}. "
            "Make sure the OSRM Docker container is running."
        ) from exc

    if payload.get("code") != "Ok":
        raise RuntimeError(f"OSRM table response error: {payload}")

    durations_raw = payload.get("durations", [])
    distances_raw = payload.get("distances", [])

    durations: list[list[float | None]] = []
    for row in durations_raw:
        durations.append([to_float_or_none(value) for value in row])

    distances: list[list[float | None]] = []
    for row in distances_raw:
        distances.append([to_float_or_none(value) for value in row])

    return durations, distances


def build_osrm_matrices(
    coords: list[tuple[float, float] | None],
    osrm_base_url: str,
    request_timeout_seconds: int,
    table_chunk_size: int,
    verbose: bool = True,
) -> tuple[list[list[float | None]], list[list[float | None]]]:
    stop_count = len(coords)
    duration_matrix: list[list[float | None]] = [
        [None for _ in range(stop_count)] for _ in range(stop_count)
    ]
    distance_matrix: list[list[float | None]] = [
        [None for _ in range(stop_count)] for _ in range(stop_count)
    ]

    valid_indices = [idx for idx, coord in enumerate(coords) if coord is not None]
    if not valid_indices:
        log_debug("No stops with coordinates; OSRM matrices are all null.", verbose=verbose)
        return duration_matrix, distance_matrix

    compact_coords = [coords[idx] for idx in valid_indices if coords[idx] is not None]
    compact_chunks = iter_chunks(list(range(len(compact_coords))), max(1, table_chunk_size))
    total_blocks = len(compact_chunks) * len(compact_chunks)
    log_debug(
        "Requesting OSRM table matrix "
        f"for {len(compact_coords)} stops with coordinates "
        f"({len(compact_chunks)}x{len(compact_chunks)} = {total_blocks} block requests).",
        verbose=verbose,
    )
    log_debug(
        "OSRM duration units are seconds; distance units are meters.",
        verbose=verbose,
    )

    compact_duration: list[list[float | None]] = [
        [None for _ in range(len(compact_coords))] for _ in range(len(compact_coords))
    ]
    compact_distance: list[list[float | None]] = [
        [None for _ in range(len(compact_coords))] for _ in range(len(compact_coords))
    ]

    block_number = 0
    for source_chunk in compact_chunks:
        source_coords = [compact_coords[idx] for idx in source_chunk]
        for dest_chunk in compact_chunks:
            block_number += 1
            log_debug(
                f"OSRM block {block_number}/{total_blocks} "
                f"(sources={len(source_chunk)}, destinations={len(dest_chunk)})",
                verbose=verbose,
            )
            dest_coords = [compact_coords[idx] for idx in dest_chunk]
            sub_durations, sub_distances = fetch_osrm_submatrix(
                source_coords=source_coords,
                dest_coords=dest_coords,
                osrm_base_url=osrm_base_url,
                request_timeout_seconds=request_timeout_seconds,
            )

            for i_local, i_compact in enumerate(source_chunk):
                for j_local, j_compact in enumerate(dest_chunk):
                    compact_duration[i_compact][j_compact] = sub_durations[i_local][j_local]
                    compact_distance[i_compact][j_compact] = sub_distances[i_local][j_local]

    for i_compact, i_full in enumerate(valid_indices):
        for j_compact, j_full in enumerate(valid_indices):
            duration_matrix[i_full][j_full] = compact_duration[i_compact][j_compact]
            distance_matrix[i_full][j_full] = compact_distance[i_compact][j_compact]

    log_debug("Finished OSRM table matrix assembly.", verbose=verbose)
    return duration_matrix, distance_matrix


def create_problem_instance(
    geocode_csv_path: Path,
    bins_csv_path: Path,
    routine_csv_path: Path,
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
    output_path: Path,
    osrm_base_url: str,
    request_timeout_seconds: int,
    table_chunk_size: int,
    skip_osrm: bool,
    include_all_active: bool,
    verbose: bool = True,
) -> dict[str, Any]:
    log_debug(f"Loading geocoded stops: {geocode_csv_path}", verbose=verbose)
    geocode_df = load_geocode_dataframe(geocode_csv_path)
    log_debug(f"Loaded {len(geocode_df)} geocoded rows.", verbose=verbose)

    log_debug(f"Loading BIN metadata: {bins_csv_path}", verbose=verbose)
    _, bin_alias_map, bin_primary_order, bin_meta = load_bins_metadata(bins_csv_path)
    log_debug(
        f"Loaded {len(bin_primary_order)} BIN primary locations and {len(bin_alias_map)} BIN aliases.",
        verbose=verbose,
    )

    log_debug(f"Loading routine metadata: {routine_csv_path}", verbose=verbose)
    _, routine_alias_map, routine_primary_order, routine_meta = load_routine_metadata(routine_csv_path)
    log_debug(
        "Loaded "
        f"{len(routine_primary_order)} routine primary locations and "
        f"{len(routine_alias_map)} routine aliases.",
        verbose=verbose,
    )

    alias_to_primary: dict[str, str] = {}
    for alias_key, primary in bin_alias_map.items():
        if alias_key not in alias_to_primary:
            alias_to_primary[alias_key] = primary
    for alias_key, primary in routine_alias_map.items():
        if alias_key not in alias_to_primary:
            alias_to_primary[alias_key] = primary
    log_debug(f"Combined alias map contains {len(alias_to_primary)} canonical mappings.", verbose=verbose)

    log_debug("Canonicalizing display names and preparing date-filtered data.", verbose=verbose)
    geocode_df["display_canonical"] = geocode_df["display_name_final"].map(
        lambda value: canonicalize_display_name(value, alias_to_primary)
    )
    geocode_df["display_canonical_key"] = geocode_df["display_canonical"].map(normalize_display_name)

    usable_df = geocode_df.loc[
        geocode_df["visit_date"].notna() & (geocode_df["display_canonical"] != "")
    ].copy()

    range_df = usable_df.loc[
        (usable_df["visit_date"] >= start_date) & (usable_df["visit_date"] <= end_date)
    ].copy()
    range_df = range_df.sort_values(
        ["visit_date", "Driver", "planned_stop_number_num", "actual_stop_number_num"],
        ascending=[True, True, True, True],
        na_position="last",
    )

    range_order = list(dict.fromkeys(range_df["display_canonical"].tolist()))
    range_counts = range_df["display_canonical"].value_counts().to_dict()
    if "Actual Duration" in range_df.columns:
        range_df["actual_duration_minutes"] = range_df["Actual Duration"].map(parse_duration_minutes)
    else:
        range_df["actual_duration_minutes"] = None
    mean_stop_duration_by_name = (
        range_df.dropna(subset=["actual_duration_minutes"])
        .groupby("display_canonical")["actual_duration_minutes"]
        .mean()
        .to_dict()
    )
    log_debug(
        f"Date-range rows: {len(range_df)} | unique canonical stops in range: {len(range_order)}",
        verbose=verbose,
    )
    log_debug(
        f"Computed mean_stop_duration for {len(mean_stop_duration_by_name)} stops "
        "from in-range Actual Duration values (minutes).",
        verbose=verbose,
    )

    previous_7_start = start_date - timedelta(days=7)
    previous_30_start = start_date - timedelta(days=30)

    prev_7_df = usable_df.loc[
        (usable_df["visit_date"] < start_date) & (usable_df["visit_date"] >= previous_7_start)
    ]
    prev_30_df = usable_df.loc[
        (usable_df["visit_date"] < start_date) & (usable_df["visit_date"] >= previous_30_start)
    ]

    prev_7_counts = prev_7_df["display_canonical"].value_counts().to_dict()
    prev_30_counts = prev_30_df["display_canonical"].value_counts().to_dict()
    log_debug(
        "Computed lookback features from "
        f"{previous_30_start.strftime('%Y-%m-%d')} to {(start_date - timedelta(days=1)).strftime('%Y-%m-%d')}.",
        verbose=verbose,
    )

    coords_by_name: dict[str, tuple[float, float]] = {}
    coords_df = usable_df.loc[
        usable_df["latitude_num"].notna() & usable_df["longitude_num"].notna(),
        ["display_canonical", "latitude_num", "longitude_num"],
    ].copy()
    if not coords_df.empty:
        grouped = (
            coords_df.groupby("display_canonical", as_index=False)[["latitude_num", "longitude_num"]]
            .median()
            .rename(columns={"latitude_num": "lat", "longitude_num": "lon"})
        )
        for _, row in grouped.iterrows():
            coords_by_name[str(row["display_canonical"])] = (
                float(row["lat"]),
                float(row["lon"]),
            )
    log_debug(
        f"Resolved coordinate centroids for {len(coords_by_name)} canonical stops.",
        verbose=verbose,
    )

    depot_key = normalize_display_name(DEPOT_DISPLAY_NAME)
    range_order_without_depot = [
        name for name in range_order if normalize_display_name(name) != depot_key
    ]
    final_stop_order = [DEPOT_DISPLAY_NAME] + range_order_without_depot
    seen = set(final_stop_order)

    def is_active_recent(display_name: str) -> bool:
        return int(prev_7_counts.get(display_name, 0)) > 0 or int(prev_30_counts.get(display_name, 0)) > 0

    if include_all_active:
        for display_name in bin_primary_order:
            if display_name in seen:
                continue
            if is_active_recent(display_name):
                final_stop_order.append(display_name)
                seen.add(display_name)

        for display_name in routine_primary_order:
            if display_name in seen:
                continue
            if is_active_recent(display_name):
                final_stop_order.append(display_name)
                seen.add(display_name)
        added_active_only = len(final_stop_order) - len(range_order_without_depot) - 1
        log_debug(
            "Inserted depot at index 0 and added "
            f"{added_active_only} active BIN/routine stops not directly visited in range.",
            verbose=verbose,
        )
    else:
        log_debug(
            "Inserted depot at index 0 and skipped adding active-only BIN/routine stops "
            "(include_all_active=false).",
            verbose=verbose,
        )

    stops: list[dict[str, Any]] = []
    coord_list: list[tuple[float, float] | None] = []

    for stop_index, display_name in enumerate(final_stop_order):
        is_depot = normalize_display_name(display_name) == depot_key
        bin_row = bin_meta.get(display_name, {})
        routine_row = routine_meta.get(display_name, {})

        if is_depot:
            lat = DEPOT_LAT
            lon = DEPOT_LON
        else:
            lat = to_float_or_none(bin_row.get("primary_lat"))
            lon = to_float_or_none(bin_row.get("primary_lon"))
            if lat is None or lon is None:
                lat = to_float_or_none(routine_row.get("lat"))
                lon = to_float_or_none(routine_row.get("lon"))
            if (lat is None or lon is None) and display_name in coords_by_name:
                lat, lon = coords_by_name[display_name]

        mean_stop_duration_value = (
            float("nan")
            if is_depot
            else (
                float(mean_stop_duration_by_name[display_name])
                if display_name in mean_stop_duration_by_name
                else float("nan")
            )
        )

        stop_payload = {
            "stop_index": stop_index,
            "display_name": display_name,
            "visits_in_range": 0 if is_depot else int(range_counts.get(display_name, 0)),
            "mean_stop_duration": mean_stop_duration_value,
            "stops_in_previous_7": 0 if is_depot else int(prev_7_counts.get(display_name, 0)),
            "stops_in_previous_30": 0 if is_depot else int(prev_30_counts.get(display_name, 0)),
            "is_bin": False if is_depot else bool(bin_row),
            "is_routine": False if is_depot else bool(routine_row),
            "bin_id": "" if is_depot else (str(bin_row.get("bin_id", "")) if bin_row else ""),
            "bin_cluster_id": "" if is_depot else (str(bin_row.get("bin_cluster_id", "")) if bin_row else ""),
            "bin_location_name_primary": (
                "" if is_depot else (str(bin_row.get("location_name_primary", "")) if bin_row else "")
            ),
            "routine_total_stop_count": (
                int(routine_row["total_stop_count"])
                if (not is_depot) and routine_row and routine_row.get("total_stop_count") is not None
                else None
            ),
            "routine_max_monthly_stop_count": (
                int(routine_row["max_monthly_stop_count"])
                if (not is_depot) and routine_row and routine_row.get("max_monthly_stop_count") is not None
                else None
            ),
            "latitude": lat,
            "longitude": lon,
        }
        stops.append(stop_payload)
        coord_list.append((lat, lon) if lat is not None and lon is not None else None)
    missing_coords = sum(1 for coord in coord_list if coord is None)
    log_debug(
        f"Built ordered stop list: {len(stops)} stops total, {missing_coords} without coordinates.",
        verbose=verbose,
    )

    if skip_osrm:
        log_debug("Skipping OSRM matrix calls due to --skip-osrm flag.", verbose=verbose)
        log_debug(
            "Travel time units would be seconds and distance units would be meters.",
            verbose=verbose,
        )
        travel_time_matrix_seconds = [[None for _ in stops] for _ in stops]
        travel_distance_matrix_meters = [[None for _ in stops] for _ in stops]
    else:
        travel_time_matrix_seconds, travel_distance_matrix_meters = build_osrm_matrices(
            coords=coord_list,
            osrm_base_url=osrm_base_url,
            request_timeout_seconds=request_timeout_seconds,
            table_chunk_size=table_chunk_size,
            verbose=verbose,
        )

    payload = {
        "start_date": start_date.strftime("%Y-%m-%d"),
        "end_date": end_date.strftime("%Y-%m-%d"),
        "stop_count": len(stops),
        "travel_time_unit": "seconds",
        "travel_distance_unit": "meters",
        "stops": stops,
        "travel_time_matrix_seconds": travel_time_matrix_seconds,
        "travel_distance_matrix_meters": travel_distance_matrix_meters,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    log_debug(f"Wrote output JSON payload: {output_path}", verbose=verbose)

    return payload


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create optimizer problem-instance JSON payloads from geocoded stop data."
    )
    parser.add_argument(
        "--start-date",
        default=START_DATE,
        help="Start date for the instance (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--end-date",
        default=END_DATE,
        help="End date for the instance (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--geocode-csv",
        default=str(DEFAULT_GEOCODE_CSV),
        help="Path to data_geocode.csv.",
    )
    parser.add_argument(
        "--bins-csv",
        default=str(DEFAULT_BINS_CSV),
        help="Path to data_bins.csv.",
    )
    parser.add_argument(
        "--routine-csv",
        default=str(DEFAULT_ROUTINE_CSV),
        help="Path to data_routine.csv.",
    )
    parser.add_argument(
        "--output",
        default="",
        help=(
            "Output file path. Defaults to optimize/problem_instance_YYYY_MM_DD.csv "
            "(JSON content in file)."
        ),
    )
    parser.add_argument(
        "--osrm-base-url",
        default=DEFAULT_OSRM_BASE_URL,
        help="OSRM base URL (default: http://localhost:5000).",
    )
    parser.add_argument(
        "--request-timeout-seconds",
        type=int,
        default=DEFAULT_REQUEST_TIMEOUT_SECONDS,
        help="HTTP timeout for OSRM requests in seconds.",
    )
    parser.add_argument(
        "--table-chunk-size",
        type=int,
        default=DEFAULT_TABLE_CHUNK_SIZE,
        help="Chunk size used for OSRM table API block requests.",
    )
    parser.add_argument(
        "--skip-osrm",
        action="store_true",
        help="Skip OSRM matrix calls and output null matrices (for data-shape testing).",
    )
    parser.add_argument(
        "--include-all-active",
        "--include_all_active",
        dest="include_all_active",
        type=parse_bool,
        default=True,
        help=(
            "Whether to include active BIN/routine stops that were not visited in the selected "
            "date range. Accepts true/false. Default: true."
        ),
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress step-by-step debug logs.",
    )

    args = parser.parse_args()
    verbose = not args.quiet

    start_date = parse_date_arg(args.start_date, "--start-date")
    end_date = parse_date_arg(args.end_date, "--end-date")
    if end_date < start_date:
        raise SystemExit(
            f"Invalid range: end_date ({end_date.date()}) is before start_date ({start_date.date()})."
        )

    geocode_csv_path = Path(args.geocode_csv).expanduser().resolve()
    bins_csv_path = Path(args.bins_csv).expanduser().resolve()
    routine_csv_path = Path(args.routine_csv).expanduser().resolve()

    if args.output.strip():
        output_path = Path(args.output).expanduser().resolve()
    else:
        output_name = f"problem_instance_{start_date.strftime('%Y_%m_%d')}.csv"
        output_path = (SCRIPT_DIR / output_name).resolve()

    log_debug(
        "Starting problem instance build for "
        f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}.",
        verbose=verbose,
    )
    log_debug(f"OSRM base URL: {args.osrm_base_url}", verbose=verbose)
    log_debug(
        f"OSRM travel-time unit is seconds; travel-distance unit is meters.",
        verbose=verbose,
    )
    log_debug(
        f"include_all_active={bool(args.include_all_active)}",
        verbose=verbose,
    )

    for label, path in (
        ("geocode CSV", geocode_csv_path),
        ("bins CSV", bins_csv_path),
        ("routine CSV", routine_csv_path),
    ):
        if not path.exists():
            raise SystemExit(f"{label} not found: {path}")

    payload = create_problem_instance(
        geocode_csv_path=geocode_csv_path,
        bins_csv_path=bins_csv_path,
        routine_csv_path=routine_csv_path,
        start_date=start_date,
        end_date=end_date,
        output_path=output_path,
        osrm_base_url=args.osrm_base_url,
        request_timeout_seconds=max(1, int(args.request_timeout_seconds)),
        table_chunk_size=max(1, int(args.table_chunk_size)),
        skip_osrm=bool(args.skip_osrm),
        include_all_active=bool(args.include_all_active),
        verbose=verbose,
    )

    missing_coord_count = sum(
        1 for stop in payload["stops"] if stop.get("latitude") is None or stop.get("longitude") is None
    )
    print(f"Wrote problem instance: {output_path}")
    print(
        f"Range: {payload['start_date']} to {payload['end_date']} | "
        f"stops={payload['stop_count']} | missing_coords={missing_coord_count}"
    )
    print(
        f"OSRM units: travel_time={payload['travel_time_unit']}, "
        f"travel_distance={payload['travel_distance_unit']}"
    )


if __name__ == "__main__":
    main()
