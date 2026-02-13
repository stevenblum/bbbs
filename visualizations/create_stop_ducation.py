#!/usr/bin/env python3
"""Create a modeling dataset for stop-duration prediction."""

from __future__ import annotations

import argparse
import ast
import json
from pathlib import Path
from typing import Any, Sequence

try:
    import numpy as np
    import pandas as pd
except ImportError as exc:
    raise SystemExit(
        "This script requires pandas and numpy. Run with the project venv, for example: "
        "./.venv/bin/python visualizations/create_stop_ducation.py"
    ) from exc


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

DEFAULT_DATA_CSV = PROJECT_ROOT / "data_geocode" / "latest" / "data_geocode.csv"
DEFAULT_ACTIVE_BINS_CSV = SCRIPT_DIR / "data_active_bins.csv"
DEFAULT_ACTIVE_ROUTINE_CSV = SCRIPT_DIR / "data_active_routine.csv"
DEFAULT_BINS_CSV = SCRIPT_DIR / "data_bins.csv"
DEFAULT_OUTPUT_CSV = SCRIPT_DIR / "data_stop_duration.csv"

NUMERIC_JSON_FIELDS = {
    "previous_days_since",
    "next_days_to",
    "stops_in_previous_7",
    "stops_in_previous_14",
    "stops_in_previous_28",
    "stops_in_previous_56",
    "stops_in_next_7",
    "stops_in_next_28",
}
BOOLEAN_JSON_FIELDS = {"active"}


def normalize_display_name(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    text = str(value).strip()
    return text.casefold() if text else ""


def parse_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text == "":
        return None
    if text in {"1", "true", "t", "yes", "y"}:
        return True
    if text in {"0", "false", "f", "no", "n"}:
        return False
    return None


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


def first_existing(paths: Sequence[Path]) -> Path:
    for path in paths:
        if path.exists():
            return path
    raise FileNotFoundError("None of the expected files exist: " + ", ".join(str(p) for p in paths))


def resolve_column(df: pd.DataFrame, candidates: Sequence[str], field_name: str) -> str:
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    raise SystemExit(
        f"Missing required column for {field_name}. "
        f"Tried columns: {', '.join(candidates)}"
    )


def build_bin_display_to_id_map(bins_df: pd.DataFrame) -> dict[str, str]:
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

    for _, row in bins_df.iterrows():
        bin_id = str(row.get("bin_id", "")).strip()
        if not bin_id:
            continue

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

    return display_to_bin_id


def _clean_payload_value(field: str, value: Any) -> Any:
    if value is None:
        return None

    if isinstance(value, str):
        text = value.strip()
        if text == "" or text.lower() == "nan":
            return None
        value = text

    if field in BOOLEAN_JSON_FIELDS:
        return parse_bool(value)

    if field in NUMERIC_JSON_FIELDS:
        return pd.to_numeric(value, errors="coerce")

    return value


def parse_payload(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, float) and pd.isna(value):
        return {}
    text = str(value).strip()
    if text == "" or text.lower() == "nan":
        return {}

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return {}
    if not isinstance(parsed, dict):
        return {}

    cleaned: dict[str, Any] = {}
    for key, raw_val in parsed.items():
        cleaned[key] = _clean_payload_value(str(key), raw_val)
    return cleaned


def parse_payload_series(payload_series: pd.Series) -> pd.Series:
    payload_series = payload_series.fillna("")
    unique_payloads = pd.unique(payload_series)
    parsed_cache: dict[str, dict[str, Any]] = {}
    for payload in unique_payloads:
        parsed_cache[payload] = parse_payload(payload)
    return payload_series.map(parsed_cache)


def _time_bucket(hour: Any) -> str | None:
    if pd.isna(hour):
        return None
    hour_int = int(hour)
    if 6 <= hour_int < 12:
        return "morning"
    if 12 <= hour_int < 18:
        return "afternoon"
    if 18 <= hour_int < 24:
        return "evening"
    return "night"


def expand_payload_dict_column(
    output_df: pd.DataFrame,
    payload_col: str,
) -> list[str]:
    all_keys: set[str] = set()
    for payload in output_df[payload_col]:
        if isinstance(payload, dict):
            all_keys.update(payload.keys())

    schedule_cols: list[str] = []
    for key in sorted(all_keys):
        output_df[key] = output_df[payload_col].map(
            lambda item: item.get(key) if isinstance(item, dict) else None
        )
        schedule_cols.append(key)

    return schedule_cols


def create_stop_duration_dataset(
    data_csv: Path,
    active_bins_csv: Path,
    active_routine_csv: Path,
    bins_csv: Path,
    output_csv: Path,
) -> dict[str, int]:
    stops_raw = pd.read_csv(data_csv, dtype=str).fillna("")

    col_display_name = resolve_column(stops_raw, ["display_name"], "display_name")
    col_actual_date = resolve_column(stops_raw, ["Actual Date", "Actual_Date"], "Actual Date")
    col_actual_time = resolve_column(stops_raw, ["Actual Time", "Actual_Time"], "Actual Time")
    col_driver = resolve_column(stops_raw, ["Driver"], "Driver")
    col_planned_duration = resolve_column(
        stops_raw,
        ["Planned Duration", "Planned Duraation", "Planned_Duration", "Planned_Duraation"],
        "Planned Duration",
    )
    col_actual_duration = resolve_column(
        stops_raw, ["Actual Duration", "Actual_Duration"], "Actual Duration"
    )

    output_df = stops_raw[
        [
            col_display_name,
            col_actual_date,
            col_actual_time,
            col_driver,
            col_planned_duration,
            col_actual_duration,
        ]
    ].copy()
    output_df = output_df.rename(
        columns={
            col_display_name: "display_name",
            col_actual_date: "actual_date",
            col_actual_time: "actual_time",
            col_driver: "driver",
            col_planned_duration: "planned_duration",
            col_actual_duration: "actual_duration",
        }
    )

    for col in [
        "display_name",
        "actual_date",
        "actual_time",
        "driver",
        "planned_duration",
        "actual_duration",
    ]:
        output_df[col] = output_df[col].astype(str).str.strip()

    output_df["display_name_key"] = output_df["display_name"].map(normalize_display_name)
    output_df["stop_date"] = pd.to_datetime(output_df["actual_date"], errors="coerce").dt.normalize()
    output_df["stop_date_str"] = output_df["stop_date"].dt.strftime("%Y-%m-%d")

    bins_df = pd.read_csv(bins_csv, dtype=str).fillna("")
    display_to_bin_id = build_bin_display_to_id_map(bins_df)
    output_df["bin_id"] = output_df["display_name_key"].map(display_to_bin_id)
    output_df["is_bin"] = output_df["bin_id"].notna()
    output_df["is_location"] = ~output_df["is_bin"]

    active_bins_df = pd.read_csv(active_bins_csv, dtype=str).fillna("")
    if "date" not in active_bins_df.columns:
        raise SystemExit(f"Missing required 'date' column in {active_bins_csv}")
    active_bins_long = active_bins_df.melt(
        id_vars=["date"], var_name="bin_id", value_name="bin_json"
    )
    active_bins_long["bin_payload"] = parse_payload_series(active_bins_long["bin_json"])
    output_df = output_df.merge(
        active_bins_long[["date", "bin_id", "bin_payload"]],
        how="left",
        left_on=["stop_date_str", "bin_id"],
        right_on=["date", "bin_id"],
    ).drop(columns=["date"], errors="ignore")

    active_routine_df = pd.read_csv(active_routine_csv, dtype=str).fillna("")
    if "date" not in active_routine_df.columns:
        raise SystemExit(f"Missing required 'date' column in {active_routine_csv}")
    active_routine_long = active_routine_df.melt(
        id_vars=["date"],
        var_name="routine_display_name",
        value_name="routine_json",
    )
    active_routine_long["display_name_key"] = active_routine_long["routine_display_name"].map(
        normalize_display_name
    )
    active_routine_long["routine_payload"] = parse_payload_series(active_routine_long["routine_json"])
    output_df = output_df.merge(
        active_routine_long[["date", "display_name_key", "routine_payload"]],
        how="left",
        left_on=["stop_date_str", "display_name_key"],
        right_on=["date", "display_name_key"],
    ).drop(columns=["date"], errors="ignore")

    output_df["is_routine_location"] = output_df["routine_payload"].map(
        lambda item: isinstance(item, dict) and len(item) > 0
    )
    output_df["stop_type"] = np.where(output_df["is_bin"], "bin", "location")
    output_df["schedule_source"] = np.where(
        output_df["is_bin"], "bins", np.where(output_df["is_routine_location"], "routine", "none")
    )
    output_df["schedule_payload"] = output_df["bin_payload"]
    location_mask = ~output_df["is_bin"]
    output_df.loc[location_mask, "schedule_payload"] = output_df.loc[location_mask, "routine_payload"]
    schedule_cols = expand_payload_dict_column(output_df, "schedule_payload")

    actual_time_parsed = pd.to_datetime(output_df["actual_time"], format="%H:%M:%S", errors="coerce")
    missing_time_mask = actual_time_parsed.isna()
    if missing_time_mask.any():
        actual_time_parsed = actual_time_parsed.copy()
        actual_time_parsed.loc[missing_time_mask] = pd.to_datetime(
            output_df.loc[missing_time_mask, "actual_time"], errors="coerce"
        )

    output_df["hour_of_day"] = actual_time_parsed.dt.hour.astype("Int64")
    output_df["day_of_week"] = output_df["stop_date"].dt.dayofweek.astype("Int64")
    output_df["week_of_year"] = (
        output_df["stop_date"].dt.isocalendar().week.astype("Int64").clip(lower=1, upper=52)
    )
    output_df["month_of_year"] = output_df["stop_date"].dt.month.astype("Int64")
    output_df["time_of_day"] = output_df["hour_of_day"].map(_time_bucket)

    output_df["planned_duration_minutes"] = pd.to_numeric(
        output_df["planned_duration"], errors="coerce"
    )
    output_df["actual_duration_minutes"] = pd.to_numeric(
        output_df["actual_duration"], errors="coerce"
    )

    leading_cols = [
        "display_name",
        "driver",
        "actual_date",
        "actual_time",
        "planned_duration",
        "actual_duration",
        "planned_duration_minutes",
        "actual_duration_minutes",
        "stop_date_str",
        "day_of_week",
        "week_of_year",
        "month_of_year",
        "hour_of_day",
        "time_of_day",
        "stop_type",
        "is_bin",
        "is_location",
        "is_routine_location",
        "bin_id",
        "schedule_source",
    ]
    trailing_cols = sorted(schedule_cols) + ["display_name_key", "stop_date"]
    ordered_cols = [col for col in leading_cols + trailing_cols if col in output_df.columns]
    output_df = output_df[ordered_cols]

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    output_df.to_csv(output_csv, index=False)

    return {
        "rows": len(output_df),
        "bin_rows": int(output_df["is_bin"].sum()),
        "location_rows": int(output_df["is_location"].sum()),
        "routine_rows": int(output_df["is_routine_location"].sum()),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create stop-duration modeling dataset from stop data + active bin/routine schedules."
    )
    parser.add_argument("--data", default=str(DEFAULT_DATA_CSV), help="Input geocoded stop CSV")
    parser.add_argument(
        "--active-bins", default=str(DEFAULT_ACTIVE_BINS_CSV), help="Input active bins CSV"
    )
    parser.add_argument(
        "--active-routine", default=str(DEFAULT_ACTIVE_ROUTINE_CSV), help="Input active routine CSV"
    )
    parser.add_argument("--bins", default=str(DEFAULT_BINS_CSV), help="Input bins definition CSV")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT_CSV), help="Output dataset CSV")
    args = parser.parse_args()

    data_csv = Path(args.data).expanduser().resolve()
    active_bins_csv = Path(args.active_bins).expanduser().resolve()
    active_routine_csv = Path(args.active_routine).expanduser().resolve()
    bins_csv = Path(args.bins).expanduser().resolve()
    output_csv = Path(args.output).expanduser().resolve()

    for path, label in [
        (data_csv, "data CSV"),
        (active_bins_csv, "active bins CSV"),
        (active_routine_csv, "active routine CSV"),
        (bins_csv, "bins CSV"),
    ]:
        if not path.exists():
            raise SystemExit(f"Missing {label}: {path}")

    metrics = create_stop_duration_dataset(
        data_csv=data_csv,
        active_bins_csv=active_bins_csv,
        active_routine_csv=active_routine_csv,
        bins_csv=bins_csv,
        output_csv=output_csv,
    )

    print(f"Wrote stop-duration dataset: {output_csv}")
    print(
        "Summary: "
        f"rows={metrics['rows']}, "
        f"bin_rows={metrics['bin_rows']}, "
        f"location_rows={metrics['location_rows']}, "
        f"routine_rows={metrics['routine_rows']}"
    )


if __name__ == "__main__":
    main()
