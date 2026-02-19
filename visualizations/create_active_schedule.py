#!/usr/bin/env python3
"""Create daily active-schedule tables for BINs, routine donors, and Savers."""

from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
from typing import Any, Sequence

import numpy as np

try:
    import pandas as pd
except ImportError as exc:
    raise SystemExit(
        "This script requires pandas. Run with the project venv, for example: "
        "./.venv/bin/python visualizations/create_active_schedule.py"
    ) from exc


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

DATA_CANDIDATES: list[Path] = [
    PROJECT_ROOT / "data_geocode" / "latest" / "data_geocoded.csv",
    PROJECT_ROOT / "data_geocode" / "latest" / "data_geocode.csv",
]
CACHE_CANDIDATES: list[Path] = [
    PROJECT_ROOT / "data_geocode" / "latest" / "geocoded_address_cache.csv",
    PROJECT_ROOT / "data_geocode" / "latest" / "geocode_address_cache.csv",
]

DEFAULT_BINS_CSV = SCRIPT_DIR / "data_bins.csv"
DEFAULT_ROUTINE_CSV = SCRIPT_DIR / "data_routine.csv"
DEFAULT_SAVERS_CSV = SCRIPT_DIR / "data_savers.csv"
DEFAULT_ACTIVE_BINS_CSV = SCRIPT_DIR / "data_active_bins.csv"
DEFAULT_ACTIVE_ROUTINE_CSV = SCRIPT_DIR / "data_active_routine.csv"
DEFAULT_ACTIVE_SAVERS_CSV = SCRIPT_DIR / "data_active_savers.csv"

WINDOW_DAYS = (7, 14, 28, 56)
FUTURE_WINDOW_DAYS = (7, 28)
NAN_TOKEN = "NaN"


def first_existing(paths: Sequence[Path]) -> Path:
    for path in paths:
        if path.exists():
            return path
    raise FileNotFoundError("None of the expected files exist: " + ", ".join(str(p) for p in paths))


def normalize_display_name(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    text = str(value).strip()
    return text.casefold() if text else ""


def parse_bool(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"1", "true", "t", "yes", "y"}


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


def build_analysis_dataframe(data_path: Path, cache_path: Path) -> pd.DataFrame:
    data_df = pd.read_csv(data_path, dtype=str).fillna("")
    cache_df = pd.read_csv(cache_path, dtype=str).fillna("")

    for col in [
        "Location",
        "Address",
        "display_name",
        "Planned Date",
        "Actual Date",
    ]:
        if col not in data_df.columns:
            data_df[col] = ""

    for col in ["address_raw", "address_nominatim"]:
        if col not in cache_df.columns:
            cache_df[col] = ""

    cache_df = cache_df.rename(
        columns={
            "address_raw": "cache_address_raw",
            "address_nominatim": "cache_display_name",
        }
    )
    cache_df = cache_df.drop_duplicates(subset=["cache_address_raw"], keep="first")

    analysis_df = data_df.merge(
        cache_df[["cache_address_raw", "cache_display_name"]],
        how="left",
        left_on="Address",
        right_on="cache_address_raw",
    )

    analysis_df["display_name"] = analysis_df["display_name"].fillna("").astype(str).str.strip()
    analysis_df["cache_display_name"] = (
        analysis_df["cache_display_name"].fillna("").astype(str).str.strip()
    )
    analysis_df["display_name_final"] = analysis_df["display_name"]
    missing_display_mask = analysis_df["display_name_final"] == ""
    analysis_df.loc[missing_display_mask, "display_name_final"] = analysis_df.loc[
        missing_display_mask, "cache_display_name"
    ]
    analysis_df["display_name_final"] = analysis_df["display_name_final"].fillna("").astype(str).str.strip()

    analysis_df["actual_date_parsed"] = pd.to_datetime(
        analysis_df["Actual Date"], errors="coerce"
    )
    analysis_df["planned_date_parsed"] = pd.to_datetime(
        analysis_df["Planned Date"], errors="coerce"
    )
    analysis_df["visit_date"] = analysis_df["actual_date_parsed"].fillna(
        analysis_df["planned_date_parsed"]
    )
    analysis_df["visit_date"] = analysis_df["visit_date"].dt.normalize()
    analysis_df["display_name_key"] = analysis_df["display_name_final"].map(normalize_display_name)

    return analysis_df


def build_bin_display_to_id_map(bins_df: pd.DataFrame) -> tuple[dict[str, str], list[str]]:
    bins_df = bins_df.copy()
    if "associated_stop_count" in bins_df.columns:
        bins_df["associated_stop_count_num"] = pd.to_numeric(
            bins_df["associated_stop_count"], errors="coerce"
        ).fillna(0)
    else:
        bins_df["associated_stop_count_num"] = 0

    sort_cols = ["associated_stop_count_num"]
    sort_ascending = [False]
    if "bin_id" in bins_df.columns:
        sort_cols.append("bin_id")
        sort_ascending.append(True)
    bins_df = bins_df.sort_values(sort_cols, ascending=sort_ascending)

    display_to_bin_id: dict[str, str] = {}
    ordered_bin_ids: list[str] = []

    for _, row in bins_df.iterrows():
        bin_id = str(row.get("bin_id", "")).strip()
        if not bin_id:
            continue
        if bin_id not in ordered_bin_ids:
            ordered_bin_ids.append(bin_id)

        display_names: list[str] = []
        for col in (
            "all_grouped_display_names",
            "seed_display_names",
            "distance_display_names",
            "other_display_names",
        ):
            display_names.extend(parse_list_field(row.get(col)))
        primary_display_name = str(row.get("primary_display_name", "")).strip()
        if primary_display_name:
            display_names.append(primary_display_name)

        for display_name in display_names:
            key = normalize_display_name(display_name)
            if key and key not in display_to_bin_id:
                display_to_bin_id[key] = bin_id

    return display_to_bin_id, ordered_bin_ids


def build_active_routine_names(routine_df: pd.DataFrame) -> list[str]:
    routine_df = routine_df.copy()
    if "display_name_final" not in routine_df.columns:
        return []

    routine_df["display_name_final"] = routine_df["display_name_final"].fillna("").astype(str).str.strip()
    routine_df = routine_df.loc[routine_df["display_name_final"] != ""].copy()

    if "is_routine" in routine_df.columns:
        routine_df["is_routine_bool"] = routine_df["is_routine"].map(parse_bool)
        routine_df = routine_df.loc[routine_df["is_routine_bool"]].copy()

    if routine_df.empty:
        return []

    if "total_stop_count" in routine_df.columns:
        routine_df["total_stop_count_num"] = pd.to_numeric(
            routine_df["total_stop_count"], errors="coerce"
        ).fillna(0)
        routine_df = routine_df.sort_values(
            ["total_stop_count_num", "display_name_final"],
            ascending=[False, True],
        )
    else:
        routine_df = routine_df.sort_values(["display_name_final"], ascending=[True])

    return list(dict.fromkeys(routine_df["display_name_final"].tolist()))


def build_active_savers_names_and_map(savers_df: pd.DataFrame) -> tuple[dict[str, str], list[str]]:
    savers_df = savers_df.copy()
    if savers_df.empty:
        return {}, []

    for col in [
        "savers_id",
        "primary_display_name",
        "all_grouped_display_names",
        "seed_display_names",
        "distance_display_names",
        "other_display_names",
    ]:
        if col not in savers_df.columns:
            savers_df[col] = ""

    if "associated_stop_count" in savers_df.columns:
        savers_df["associated_stop_count_num"] = pd.to_numeric(
            savers_df["associated_stop_count"], errors="coerce"
        ).fillna(0)
    else:
        savers_df["associated_stop_count_num"] = 0

    savers_df = savers_df.sort_values(
        ["associated_stop_count_num", "savers_id", "primary_display_name"],
        ascending=[False, True, True],
    )

    display_to_savers_name: dict[str, str] = {}
    ordered_savers_names: list[str] = []
    used_savers_names: set[str] = set()

    for _, row in savers_df.iterrows():
        savers_id = str(row.get("savers_id", "")).strip()
        primary_display_name = str(row.get("primary_display_name", "")).strip()
        savers_name = primary_display_name if primary_display_name else savers_id
        if not savers_name:
            continue

        if savers_name in used_savers_names:
            if savers_id and savers_id not in used_savers_names:
                savers_name = savers_id
            else:
                suffix = 2
                while f"{savers_name} ({suffix})" in used_savers_names:
                    suffix += 1
                savers_name = f"{savers_name} ({suffix})"

        used_savers_names.add(savers_name)
        ordered_savers_names.append(savers_name)

        display_names: list[str] = []
        for col in (
            "all_grouped_display_names",
            "seed_display_names",
            "distance_display_names",
            "other_display_names",
        ):
            display_names.extend(parse_list_field(row.get(col)))
        if primary_display_name:
            display_names.append(primary_display_name)

        for display_name in display_names:
            key = normalize_display_name(display_name)
            if key and key not in display_to_savers_name:
                display_to_savers_name[key] = savers_name

    return display_to_savers_name, ordered_savers_names


def build_daily_count_matrix(
    analysis_df: pd.DataFrame,
    date_index: pd.DatetimeIndex,
    display_to_entity: dict[str, str],
    entity_columns: list[str],
) -> pd.DataFrame:
    if not entity_columns:
        return pd.DataFrame(index=date_index)

    entity_df = analysis_df.copy()
    entity_df = entity_df.loc[entity_df["visit_date"].notna()].copy()
    entity_df["entity_id"] = entity_df["display_name_key"].map(display_to_entity)
    entity_df = entity_df.loc[entity_df["entity_id"].notna()].copy()

    if entity_df.empty:
        return pd.DataFrame(0, index=date_index, columns=entity_columns, dtype=int)

    counts_df = (
        entity_df.groupby(["visit_date", "entity_id"], as_index=False)
        .size()
        .pivot(index="visit_date", columns="entity_id", values="size")
        .fillna(0)
    )
    counts_df = counts_df.reindex(index=date_index, fill_value=0)
    counts_df = counts_df.reindex(columns=entity_columns, fill_value=0)
    counts_df = counts_df.astype(int)
    return counts_df


def _format_date_or_nan(day_value: np.datetime64) -> str:
    return NAN_TOKEN if np.isnat(day_value) else str(day_value)


def build_schedule_json_series(
    counts: pd.Series,
    date_index: pd.DatetimeIndex,
) -> pd.Series:
    count_values = counts.to_numpy(dtype=np.int64)
    all_days = date_index.to_numpy(dtype="datetime64[D]")
    visit_days = all_days[count_values > 0]

    total_days = len(all_days)
    prev_dates = np.full(total_days, np.datetime64("NaT"), dtype="datetime64[D]")
    next_dates = np.full(total_days, np.datetime64("NaT"), dtype="datetime64[D]")

    if len(visit_days) > 0:
        left_positions = np.searchsorted(visit_days, all_days, side="left")
        prev_mask = left_positions > 0
        prev_dates[prev_mask] = visit_days[left_positions[prev_mask] - 1]

        right_positions = np.searchsorted(visit_days, all_days, side="right")
        next_mask = right_positions < len(visit_days)
        next_dates[next_mask] = visit_days[right_positions[next_mask]]

    prev_days_since = np.full(total_days, np.nan, dtype=float)
    next_days_to = np.full(total_days, np.nan, dtype=float)

    valid_prev = ~np.isnat(prev_dates)
    valid_next = ~np.isnat(next_dates)
    prev_days_since[valid_prev] = (
        (all_days[valid_prev] - prev_dates[valid_prev]).astype("timedelta64[D]").astype(np.int64)
    )
    next_days_to[valid_next] = (
        (next_dates[valid_next] - all_days[valid_next]).astype("timedelta64[D]").astype(np.int64)
    )

    rolling_data = {
        days: counts.rolling(window=days, min_periods=1).sum().astype(int).to_numpy()
        for days in WINDOW_DAYS
    }

    # Future windows exclude "today": next_7 is [day+1, day+7], next_28 is [day+1, day+28].
    reversed_counts = counts.iloc[::-1]
    future_rolling_data = {
        days: (
            reversed_counts.shift(1)
            .rolling(window=days, min_periods=1)
            .sum()
            .iloc[::-1]
            .fillna(0)
            .astype(int)
            .to_numpy()
        )
        for days in FUTURE_WINDOW_DAYS
    }

    recent_or_frequent_rule = (
        ((rolling_data[14] > 0) | (rolling_data[56] > 3))
        & (next_days_to < 56)
    )
    future_density_rule = (
        (future_rolling_data[7] > 0)
        & (future_rolling_data[28] > 1)
    )
    active_values = recent_or_frequent_rule | future_density_rule

    payloads: list[str] = []
    for idx in range(total_days):
        payload = {
            "previous_date": _format_date_or_nan(prev_dates[idx]),
            "previous_days_since": (
                int(prev_days_since[idx]) if not np.isnan(prev_days_since[idx]) else NAN_TOKEN
            ),
            "next_date": _format_date_or_nan(next_dates[idx]),
            "next_days_to": int(next_days_to[idx]) if not np.isnan(next_days_to[idx]) else NAN_TOKEN,
            "stops_in_previous_7": int(rolling_data[7][idx]),
            "stops_in_previous_14": int(rolling_data[14][idx]),
            "stops_in_previous_28": int(rolling_data[28][idx]),
            "stops_in_previous_56": int(rolling_data[56][idx]),
            "stops_in_next_7": int(future_rolling_data[7][idx]),
            "stops_in_next_28": int(future_rolling_data[28][idx]),
            "active": bool(active_values[idx]),
        }
        payloads.append(json.dumps(payload, separators=(",", ":"), ensure_ascii=False))

    return pd.Series(payloads, index=date_index, dtype="string")


def build_schedule_output(
    counts_df: pd.DataFrame,
    date_index: pd.DatetimeIndex,
) -> pd.DataFrame:
    output_df = pd.DataFrame({"date": date_index.strftime("%Y-%m-%d")}, index=date_index)
    for entity_col in counts_df.columns:
        output_df[entity_col] = build_schedule_json_series(counts_df[entity_col], date_index)
    output_df = output_df.reset_index(drop=True)
    return output_df


def create_active_schedule_data(
    data_path: Path,
    cache_path: Path,
    bins_path: Path,
    routine_path: Path,
    active_bins_output_path: Path,
    active_routine_output_path: Path,
    savers_path: Path = DEFAULT_SAVERS_CSV,
    active_savers_output_path: Path = DEFAULT_ACTIVE_SAVERS_CSV,
) -> dict[str, int]:
    analysis_df = build_analysis_dataframe(data_path, cache_path)

    all_dates = analysis_df["visit_date"].dropna()
    if all_dates.empty:
        raise SystemExit("No valid visit dates found in data (Actual Date / Planned Date).")
    first_day = all_dates.min()
    last_day = all_dates.max()
    date_index = pd.date_range(start=first_day, end=last_day, freq="D")

    bins_df = pd.read_csv(bins_path, dtype=str).fillna("")
    routine_df = pd.read_csv(routine_path, dtype=str).fillna("")
    savers_df = pd.read_csv(savers_path, dtype=str).fillna("")

    bin_display_to_id, ordered_bin_ids = build_bin_display_to_id_map(bins_df)
    routine_names = build_active_routine_names(routine_df)
    savers_display_to_name, savers_names = build_active_savers_names_and_map(savers_df)
    routine_display_to_name = {
        normalize_display_name(name): name for name in routine_names if normalize_display_name(name)
    }

    bins_counts_df = build_daily_count_matrix(
        analysis_df=analysis_df,
        date_index=date_index,
        display_to_entity=bin_display_to_id,
        entity_columns=ordered_bin_ids,
    )
    routine_counts_df = build_daily_count_matrix(
        analysis_df=analysis_df,
        date_index=date_index,
        display_to_entity=routine_display_to_name,
        entity_columns=routine_names,
    )
    savers_counts_df = build_daily_count_matrix(
        analysis_df=analysis_df,
        date_index=date_index,
        display_to_entity=savers_display_to_name,
        entity_columns=savers_names,
    )

    active_bins_df = build_schedule_output(bins_counts_df, date_index)
    active_routine_df = build_schedule_output(routine_counts_df, date_index)
    active_savers_df = build_schedule_output(savers_counts_df, date_index)

    active_bins_output_path.parent.mkdir(parents=True, exist_ok=True)
    active_routine_output_path.parent.mkdir(parents=True, exist_ok=True)
    active_savers_output_path.parent.mkdir(parents=True, exist_ok=True)
    active_bins_df.to_csv(active_bins_output_path, index=False)
    active_routine_df.to_csv(active_routine_output_path, index=False)
    active_savers_df.to_csv(active_savers_output_path, index=False)

    return {
        "days": len(date_index),
        "bins": len(ordered_bin_ids),
        "routine_donors": len(routine_names),
        "savers": len(savers_names),
        "bin_rows_with_visits": int((bins_counts_df.sum(axis=1) > 0).sum()) if not bins_counts_df.empty else 0,
        "routine_rows_with_visits": int((routine_counts_df.sum(axis=1) > 0).sum())
        if not routine_counts_df.empty
        else 0,
        "savers_rows_with_visits": int((savers_counts_df.sum(axis=1) > 0).sum())
        if not savers_counts_df.empty
        else 0,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create daily active schedule CSVs for BINs, routine donors, and Savers."
    )
    parser.add_argument("--data", default=str(first_existing(DATA_CANDIDATES)), help="Stop-level geocoded data CSV")
    parser.add_argument("--cache", default=str(first_existing(CACHE_CANDIDATES)), help="Geocode cache CSV")
    parser.add_argument("--bins", default=str(DEFAULT_BINS_CSV), help="Input BIN definition CSV")
    parser.add_argument("--routine", default=str(DEFAULT_ROUTINE_CSV), help="Input routine donor CSV")
    parser.add_argument("--savers", default=str(DEFAULT_SAVERS_CSV), help="Input Savers CSV")
    parser.add_argument(
        "--bins-output",
        default=str(DEFAULT_ACTIVE_BINS_CSV),
        help="Output CSV path for daily BIN active schedule",
    )
    parser.add_argument(
        "--routine-output",
        default=str(DEFAULT_ACTIVE_ROUTINE_CSV),
        help="Output CSV path for daily routine donor active schedule",
    )
    parser.add_argument(
        "--savers-output",
        default=str(DEFAULT_ACTIVE_SAVERS_CSV),
        help="Output CSV path for daily Savers active schedule",
    )
    args = parser.parse_args()

    data_path = Path(args.data).expanduser().resolve()
    cache_path = Path(args.cache).expanduser().resolve()
    bins_path = Path(args.bins).expanduser().resolve()
    routine_path = Path(args.routine).expanduser().resolve()
    savers_path = Path(args.savers).expanduser().resolve()
    bins_output_path = Path(args.bins_output).expanduser().resolve()
    routine_output_path = Path(args.routine_output).expanduser().resolve()
    savers_output_path = Path(args.savers_output).expanduser().resolve()

    if not data_path.exists():
        raise SystemExit(f"Data CSV not found: {data_path}")
    if not cache_path.exists():
        raise SystemExit(f"Cache CSV not found: {cache_path}")
    if not bins_path.exists():
        raise SystemExit(f"BIN CSV not found: {bins_path}")
    if not routine_path.exists():
        raise SystemExit(f"Routine CSV not found: {routine_path}")
    if not savers_path.exists():
        raise SystemExit(f"Savers CSV not found: {savers_path}")

    metrics = create_active_schedule_data(
        data_path=data_path,
        cache_path=cache_path,
        bins_path=bins_path,
        routine_path=routine_path,
        active_bins_output_path=bins_output_path,
        active_routine_output_path=routine_output_path,
        savers_path=savers_path,
        active_savers_output_path=savers_output_path,
    )

    print(f"Wrote active BIN schedule CSV: {bins_output_path}")
    print(f"Wrote active routine schedule CSV: {routine_output_path}")
    print(f"Wrote active Savers schedule CSV: {savers_output_path}")
    print(
        "Summary: "
        f"days={metrics['days']}, "
        f"bins={metrics['bins']}, "
        f"routine_donors={metrics['routine_donors']}, "
        f"savers={metrics['savers']}, "
        f"days_with_bin_visits={metrics['bin_rows_with_visits']}, "
        f"days_with_routine_visits={metrics['routine_rows_with_visits']}, "
        f"days_with_savers_visits={metrics['savers_rows_with_visits']}"
    )


if __name__ == "__main__":
    main()
