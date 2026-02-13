#!/usr/bin/env python3
"""Generate stop-duration analysis dashboard using linear regression."""

from __future__ import annotations

import argparse
import html as html_lib
import json
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent

INPUT_CANDIDATES: list[Path] = [
    SCRIPT_DIR / "data_stop_duration.csv",
    SCRIPT_DIR / "stop_duration_data.csv",
]
OUTPUT_HTML = SCRIPT_DIR / "dash_stop_duration.html"
DEFAULT_COEFFICIENTS_CSV = SCRIPT_DIR / "data_stop_duration_coefficients.csv"
DEFAULT_ONEHOT_MAPPING_JSON = SCRIPT_DIR / "data_stop_duration_onehot_mapping.json"

TRUE_TOKENS = {"1", "true", "t", "yes", "y"}
FALSE_TOKENS = {"0", "false", "f", "no", "n"}
DAY_OF_WEEK_NAME_BY_INDEX = {
    0: "Monday",
    1: "Tuesday",
    2: "Wednesday",
    3: "Thursday",
    4: "Friday",
    5: "Saturday",
    6: "Sunday",
}
MONTH_NAME_BY_INDEX = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December",
}
OUTLIER_DIFF_THRESHOLD_PCT = 80.0


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
        f"Missing required column for {field_name}. Tried: {', '.join(candidates)}"
    )


def resolve_optional_column(df: pd.DataFrame, candidates: Sequence[str]) -> str | None:
    for candidate in candidates:
        if candidate in df.columns:
            return candidate
    return None


def to_bool_series(values: pd.Series) -> pd.Series:
    text = values.fillna("").astype(str).str.strip().str.lower()
    out = pd.Series(pd.NA, index=values.index, dtype="boolean")
    out[text.isin(TRUE_TOKENS)] = True
    out[text.isin(FALSE_TOKENS)] = False
    return out


def normalize_text(values: pd.Series) -> pd.Series:
    text = values.fillna("").astype(str).str.strip()
    text = text.replace("", pd.NA)
    return text


def to_numbered_category_label(value: Any, names_by_index: dict[int, str]) -> str:
    raw = str(value).strip()
    try:
        numeric_value = float(raw)
    except ValueError:
        return raw

    if not np.isfinite(numeric_value) or not numeric_value.is_integer():
        return raw

    idx = int(numeric_value)
    name = names_by_index.get(idx)
    if name is None:
        return raw
    return f"{idx} - {name}"


def build_missing_table_html(missing_counts: pd.Series, total_rows: int) -> str:
    rows_html: list[str] = []
    for name, count in missing_counts.items():
        pct = (float(count) / float(total_rows) * 100.0) if total_rows else 0.0
        rows_html.append(
            "<tr>"
            f"<td>{name}</td>"
            f"<td>{int(count):,}</td>"
            f"<td>{pct:.2f}%</td>"
            "</tr>"
        )
    return "\n".join(rows_html)


def build_named_coefficient_table_html(
    df: pd.DataFrame,
    name_column: str,
    empty_message: str,
) -> str:
    if df.empty:
        return f"<tr><td colspan=\"3\">{html_lib.escape(empty_message)}</td></tr>"

    rows_html: list[str] = []
    for row in df.itertuples(index=False):
        name = html_lib.escape(str(getattr(row, name_column)))
        coef = float(getattr(row, "coefficient"))
        abs_coef = abs(coef)
        rows_html.append(
            "<tr>"
            f"<td>{name}</td>"
            f"<td class=\"num-col\">{coef:.6f}</td>"
            f"<td class=\"num-col\">{abs_coef:.6f}</td>"
            "</tr>"
        )
    return "\n".join(rows_html)


def choose_eligible_mask(df: pd.DataFrame) -> pd.Series:
    bin_col = resolve_optional_column(df, ["is_bin", "if_bin"])
    routine_col = resolve_optional_column(df, ["is_routine_location", "is_routine"])
    source_col = resolve_optional_column(df, ["schedule_source"])

    if bin_col is not None and routine_col is not None:
        is_bin = to_bool_series(df[bin_col]).fillna(False)
        is_routine = to_bool_series(df[routine_col]).fillna(False)
        return is_bin | is_routine

    if source_col is not None:
        source_text = df[source_col].fillna("").astype(str).str.strip().str.lower()
        return source_text.isin({"bins", "routine"})

    raise SystemExit(
        "Could not determine bin/routine subset. Expected columns like "
        "`is_bin` + `is_routine_location` or `schedule_source`."
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Create stop-duration analysis dashboard HTML")
    parser.add_argument(
        "--input",
        default=str(first_existing(INPUT_CANDIDATES)),
        help="Input stop-duration CSV",
    )
    parser.add_argument("--output", default=str(OUTPUT_HTML), help="Output dashboard HTML path")
    parser.add_argument(
        "--coefficients-output",
        default=str(DEFAULT_COEFFICIENTS_CSV),
        help="Output CSV for model coefficients",
    )
    parser.add_argument(
        "--onehot-mapping-output",
        default=str(DEFAULT_ONEHOT_MAPPING_JSON),
        help="Output JSON for one-hot mapping",
    )
    args = parser.parse_args()

    try:
        from sklearn.compose import ColumnTransformer
        from sklearn.linear_model import LinearRegression
        from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
        from sklearn.preprocessing import OneHotEncoder
    except ImportError as exc:
        raise SystemExit(
            "This script requires scikit-learn. Install it in the project venv, then run: "
            "./.venv/bin/python visualizations/viz_stop_duration.py"
        ) from exc

    input_csv = Path(args.input).expanduser().resolve()
    output_html = Path(args.output).expanduser().resolve()
    coefficients_output = Path(args.coefficients_output).expanduser().resolve()
    onehot_mapping_output = Path(args.onehot_mapping_output).expanduser().resolve()

    if not input_csv.exists():
        raise SystemExit(f"Input CSV not found: {input_csv}")

    raw_df = pd.read_csv(input_csv, dtype=str)
    eligible_mask = choose_eligible_mask(raw_df)
    eligible_df = raw_df.loc[eligible_mask].copy()

    col_target = resolve_column(
        eligible_df,
        ["actual_duration_minutes", "actual_duraction_minutes", "actual_duration", "Actual Duration"],
        "actual duration target",
    )
    col_display_name = resolve_column(eligible_df, ["display_name"], "display_name")
    col_driver = resolve_column(eligible_df, ["driver", "Driver"], "driver")
    col_day_of_week = resolve_column(eligible_df, ["day_of_week"], "day_of_week")
    col_month_of_year = resolve_column(eligible_df, ["month_of_year"], "month_of_year")
    col_if_bin = resolve_column(eligible_df, ["is_bin", "if_bin"], "if_bin")
    col_prev_28 = resolve_column(
        eligible_df,
        ["stops_in_previous_28", "stop_in_previous_28"],
        "stops_in_previous_28",
    )
    col_prev_14 = resolve_column(eligible_df, ["stops_in_previous_14"], "stops_in_previous_14")
    col_prev_7 = resolve_column(eligible_df, ["stops_in_previous_7"], "stops_in_previous_7")
    col_prev_days_since = resolve_column(
        eligible_df, ["previous_days_since"], "previous_days_since"
    )
    col_actual_date_hover = resolve_optional_column(
        eligible_df, ["actual_date", "Actual Date", "stop_date_str", "Actual_Date"]
    )
    col_planned_minutes_hover = resolve_optional_column(
        eligible_df,
        [
            "planned_duration_minutes",
            "planned_duration",
            "Planned Duration",
            "Planned Duraation",
            "Planned_Duration",
            "Planned_Duraation",
        ],
    )

    model_df = pd.DataFrame(
        {
            "actual_duration_minutes": pd.to_numeric(eligible_df[col_target], errors="coerce"),
            "display_name": normalize_text(eligible_df[col_display_name]),
            "driver": normalize_text(eligible_df[col_driver]),
            "day_of_week": pd.to_numeric(eligible_df[col_day_of_week], errors="coerce"),
            "month_of_year": pd.to_numeric(eligible_df[col_month_of_year], errors="coerce"),
            "if_bin": to_bool_series(eligible_df[col_if_bin]).astype("Float64"),
            "stops_in_previous_28": pd.to_numeric(eligible_df[col_prev_28], errors="coerce"),
            "stops_in_previous_14": pd.to_numeric(eligible_df[col_prev_14], errors="coerce"),
            "stops_in_previous_7": pd.to_numeric(eligible_df[col_prev_7], errors="coerce"),
            "previous_days_since": pd.to_numeric(eligible_df[col_prev_days_since], errors="coerce"),
        }
    )
    model_df["if_bin"] = model_df["if_bin"].astype("Float64")

    missing_counts = model_df.isna().sum()
    total_eligible_rows = len(model_df)

    clean_df = model_df.dropna().copy()
    dropped_rows = total_eligible_rows - len(clean_df)
    rows_with_any_nan = int(model_df.isna().any(axis=1).sum())

    if clean_df.empty:
        raise SystemExit("No rows available after removing missing values in modeling columns.")

    clean_df["day_of_week"] = clean_df["day_of_week"].astype(int)
    clean_df["month_of_year"] = clean_df["month_of_year"].astype(int)

    hover_df = pd.DataFrame(index=clean_df.index)
    if col_actual_date_hover is None:
        hover_df["actual_date"] = ""
    else:
        hover_df["actual_date"] = (
            eligible_df.loc[clean_df.index, col_actual_date_hover].fillna("").astype(str).str.strip()
        )
    hover_df["driver"] = clean_df["driver"].fillna("").astype(str)
    hover_df["display_name"] = clean_df["display_name"].fillna("").astype(str)
    if col_planned_minutes_hover is None:
        hover_df["planned_stop_duration_minutes"] = np.nan
    else:
        hover_df["planned_stop_duration_minutes"] = pd.to_numeric(
            eligible_df.loc[clean_df.index, col_planned_minutes_hover], errors="coerce"
        )

    y = clean_df["actual_duration_minutes"].astype(float).to_numpy()
    numeric_features = [
        "if_bin",
        "stops_in_previous_28",
        "stops_in_previous_14",
        "stops_in_previous_7",
        "previous_days_since",
    ]
    categorical_features = ["display_name", "driver", "day_of_week", "month_of_year"]
    X = clean_df[numeric_features + categorical_features].copy()

    preprocessor = ColumnTransformer(
        transformers=[
            ("num", "passthrough", numeric_features),
            ("cat", OneHotEncoder(handle_unknown="ignore"), categorical_features),
        ],
        remainder="drop",
        sparse_threshold=0.3,
    )

    X_transformed = preprocessor.fit_transform(X)
    model = LinearRegression()
    model.fit(X_transformed, y)
    y_pred = model.predict(X_transformed)

    # Log-log scatter requires strictly positive values on both axes.
    scatter_mask = (y > 0) & (y_pred > 0)
    scatter_actual = y[scatter_mask]
    scatter_predicted = y_pred[scatter_mask]
    if len(scatter_actual) == 0:
        raise SystemExit(
            "No positive actual/predicted values available for log-log scatter plotting."
        )
    scatter_dates = hover_df["actual_date"].to_numpy()[scatter_mask]
    scatter_drivers = hover_df["driver"].to_numpy()[scatter_mask]
    scatter_display_names = hover_df["display_name"].to_numpy()[scatter_mask]
    scatter_planned_raw = hover_df["planned_stop_duration_minutes"].to_numpy(dtype=float)[scatter_mask]
    scatter_planned_strings = [
        "" if np.isnan(value) else f"{float(value):.3f}" for value in scatter_planned_raw
    ]
    scatter_meta = [
        [date, driver, display_name, planned]
        for date, driver, display_name, planned in zip(
            scatter_dates,
            scatter_drivers,
            scatter_display_names,
            scatter_planned_strings,
        )
    ]
    planned_all = hover_df["planned_stop_duration_minutes"].to_numpy(dtype=float)
    scatter_actual_planned_mask = (y > 0) & (planned_all > 0)
    scatter_actual_planned_actual = y[scatter_actual_planned_mask]
    scatter_actual_planned_planned = planned_all[scatter_actual_planned_mask]
    scatter_actual_planned_dates = hover_df["actual_date"].to_numpy()[scatter_actual_planned_mask]
    scatter_actual_planned_drivers = hover_df["driver"].to_numpy()[scatter_actual_planned_mask]
    scatter_actual_planned_display_names = hover_df["display_name"].to_numpy()[scatter_actual_planned_mask]
    scatter_actual_planned_diff_pct = (
        np.abs(scatter_actual_planned_planned - scatter_actual_planned_actual)
        / scatter_actual_planned_planned
        * 100.0
    )
    scatter_actual_planned_outlier_mask = (
        scatter_actual_planned_diff_pct > OUTLIER_DIFF_THRESHOLD_PCT
    )
    scatter_actual_planned_inlier_mask = ~scatter_actual_planned_outlier_mask
    scatter_actual_planned_meta_all = [
        [date, driver, display_name, f"{diff_pct:.2f}"]
        for date, driver, display_name, diff_pct in zip(
            scatter_actual_planned_dates,
            scatter_actual_planned_drivers,
            scatter_actual_planned_display_names,
            scatter_actual_planned_diff_pct,
        )
    ]
    scatter_actual_planned_inlier_actual = scatter_actual_planned_actual[
        scatter_actual_planned_inlier_mask
    ]
    scatter_actual_planned_inlier_planned = scatter_actual_planned_planned[
        scatter_actual_planned_inlier_mask
    ]
    scatter_actual_planned_inlier_meta = [
        meta
        for meta, is_outlier in zip(
            scatter_actual_planned_meta_all,
            scatter_actual_planned_outlier_mask,
        )
        if not is_outlier
    ]
    scatter_actual_planned_outlier_actual = scatter_actual_planned_actual[
        scatter_actual_planned_outlier_mask
    ]
    scatter_actual_planned_outlier_planned = scatter_actual_planned_planned[
        scatter_actual_planned_outlier_mask
    ]
    scatter_actual_planned_outlier_meta = [
        meta
        for meta, is_outlier in zip(
            scatter_actual_planned_meta_all,
            scatter_actual_planned_outlier_mask,
        )
        if is_outlier
    ]
    actual_planned_rule_points = int(len(scatter_actual_planned_actual))
    actual_planned_outlier_count = int(np.sum(scatter_actual_planned_outlier_mask))
    actual_planned_outlier_pct = (
        float(actual_planned_outlier_count) / float(actual_planned_rule_points) * 100.0
        if actual_planned_rule_points
        else 0.0
    )
    planned_duration_hist_values = planned_all[np.isfinite(planned_all)]
    planned_duration_hist_values = planned_duration_hist_values[planned_duration_hist_values >= 0]
    actual_duration_hist_values = y[np.isfinite(y)]
    actual_duration_hist_values = actual_duration_hist_values[actual_duration_hist_values >= 0]

    mae = float(mean_absolute_error(y, y_pred))
    mse = float(mean_squared_error(y, y_pred))
    r2 = float(r2_score(y, y_pred))

    feature_names = preprocessor.get_feature_names_out()
    coefficients = model.coef_
    coef_df = pd.DataFrame(
        {
            "feature": feature_names,
            "coefficient": coefficients,
            "abs_coefficient": np.abs(coefficients),
        }
    )
    coef_df = coef_df.sort_values("abs_coefficient", ascending=False).reset_index(drop=True)
    coefficients_output.parent.mkdir(parents=True, exist_ok=True)
    coef_df.to_csv(coefficients_output, index=False)

    coef_lookup = {name: float(coef) for name, coef in zip(feature_names, coefficients)}
    cat_encoder = preprocessor.named_transformers_["cat"]
    num_feature_count = len(numeric_features)
    cat_feature_names = list(feature_names[num_feature_count:])
    cat_coefficients = np.array(coefficients[num_feature_count:], dtype=float)

    cat_group_coefs: dict[str, np.ndarray] = {}
    cat_group_feature_names: dict[str, list[str]] = {}
    offset = 0
    for idx, cat_name in enumerate(categorical_features):
        group_count = len(cat_encoder.categories_[idx])
        group_names = cat_feature_names[offset : offset + group_count]
        group_coefs = np.array(cat_coefficients[offset : offset + group_count], dtype=float)
        cat_group_feature_names[cat_name] = group_names
        cat_group_coefs[cat_name] = group_coefs
        offset += group_count
    if offset != len(cat_coefficients):
        raise SystemExit(
            "Unexpected categorical coefficient count from OneHotEncoder. "
            f"Expected {offset}, got {len(cat_coefficients)}."
        )

    location_coefs = cat_group_coefs.get("display_name", np.array([], dtype=float))
    driver_coefs = cat_group_coefs.get("driver", np.array([], dtype=float))
    day_of_week_coefs = cat_group_coefs.get("day_of_week", np.array([], dtype=float))
    month_of_year_coefs = cat_group_coefs.get("month_of_year", np.array([], dtype=float))
    day_of_week_categories = [
        to_numbered_category_label(value, DAY_OF_WEEK_NAME_BY_INDEX)
        for value in cat_encoder.categories_[categorical_features.index("day_of_week")]
    ]
    month_of_year_categories = [
        to_numbered_category_label(value, MONTH_NAME_BY_INDEX)
        for value in cat_encoder.categories_[categorical_features.index("month_of_year")]
    ]
    driver_table_rows = [
        {"driver": str(category), "coefficient": float(coef)}
        for category, coef in zip(
            cat_encoder.categories_[categorical_features.index("driver")],
            driver_coefs,
        )
    ]
    driver_coef_table_df = pd.DataFrame(driver_table_rows).sort_values(
        "coefficient", ascending=False
    )
    location_table_rows = [
        {"display_name": str(category), "coefficient": float(coef)}
        for category, coef in zip(
            cat_encoder.categories_[categorical_features.index("display_name")],
            location_coefs,
        )
    ]
    location_coef_table_df = pd.DataFrame(location_table_rows).sort_values(
        "coefficient", ascending=False
    )
    location_top10_df = location_coef_table_df.head(10).copy()

    importance_rows: list[dict[str, Any]] = []
    for feature in numeric_features:
        coef = float(coef_lookup.get(f"num__{feature}", 0.0))
        importance_rows.append(
            {
                "feature": feature,
                "signed_coefficient": coef,
                "importance_abs": abs(coef),
                "group": "numeric",
            }
        )
    for cat_name in categorical_features:
        group_coefs = cat_group_coefs.get(cat_name, np.array([], dtype=float))
        mean_abs = float(np.mean(np.abs(group_coefs))) if len(group_coefs) else 0.0
        importance_rows.append(
            {
                "feature": f"{cat_name} (mean |coef|)",
                "signed_coefficient": mean_abs,
                "importance_abs": mean_abs,
                "group": "categorical",
            }
        )

    importance_df = pd.DataFrame(importance_rows).sort_values(
        "importance_abs", ascending=False
    ).reset_index(drop=True)

    onehot_mapping: dict[str, list[dict[str, Any]]] = {}
    for idx, cat_name in enumerate(categorical_features):
        entries: list[dict[str, Any]] = []
        group_coefs = cat_group_coefs.get(cat_name, np.array([], dtype=float))
        group_feature_names = cat_group_feature_names.get(cat_name, [])
        for category, model_feature, coef in zip(
            cat_encoder.categories_[idx],
            group_feature_names,
            group_coefs,
        ):
            entries.append(
                {
                    "value": str(category),
                    "one_hot_feature": str(model_feature),
                    "coefficient": float(coef),
                }
            )
        onehot_mapping[cat_name] = entries
    onehot_mapping_output.parent.mkdir(parents=True, exist_ok=True)
    onehot_mapping_output.write_text(
        json.dumps(onehot_mapping, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    ref_min = float(min(np.min(scatter_actual), np.min(scatter_predicted)))
    ref_max = float(max(np.max(scatter_actual), np.max(scatter_predicted)))
    if len(scatter_actual_planned_actual) > 0:
        ref_min_ap = float(
            min(np.min(scatter_actual_planned_actual), np.min(scatter_actual_planned_planned))
        )
        ref_max_ap = float(
            max(np.max(scatter_actual_planned_actual), np.max(scatter_actual_planned_planned))
        )
    else:
        ref_min_ap = 1.0
        ref_max_ap = 10.0
    missing_table_html = build_missing_table_html(missing_counts, total_eligible_rows)
    driver_coeff_table_html = build_named_coefficient_table_html(
        driver_coef_table_df,
        name_column="driver",
        empty_message="No driver coefficients available.",
    )
    location_top10_table_html = build_named_coefficient_table_html(
        location_top10_df,
        name_column="display_name",
        empty_message="No location coefficients available.",
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Stop Duration Dashboard</title>
  <script src="https://cdn.plot.ly/plotly-2.30.0.min.js"></script>
  <style>
    :root {{
      --bg: #0e1117;
      --panel: #171c24;
      --panel-border: #2a3240;
      --text: #e6edf3;
      --muted: #a4b1c4;
      --accent: #5ad2f4;
      --good: #8bd17c;
      --warn: #ffcf6e;
      --bad: #ff8e72;
    }}
    body {{
      margin: 0;
      font-family: "IBM Plex Sans", "Segoe UI", sans-serif;
      background: radial-gradient(circle at top, #182033, var(--bg));
      color: var(--text);
    }}
    header {{
      padding: 24px 32px 8px 32px;
    }}
    h1 {{
      margin: 0 0 6px 0;
      font-size: 28px;
    }}
    p.subtitle {{
      margin: 0;
      color: var(--muted);
      font-size: 14px;
    }}
    .stats {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 12px;
      padding: 10px 32px 0 32px;
    }}
    .stat-card {{
      background: var(--panel);
      border: 1px solid var(--panel-border);
      border-radius: 12px;
      padding: 14px 16px;
      box-shadow: 0 10px 30px rgba(0,0,0,0.25);
    }}
    .stat-label {{
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 6px;
    }}
    .stat-value {{
      font-size: 24px;
      font-weight: 700;
      line-height: 1;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(340px, 1fr));
      gap: 16px;
      padding: 20px 32px 32px 32px;
    }}
    .section-divider {{
      display: flex;
      align-items: center;
      gap: 10px;
      color: var(--muted);
      font-size: 12px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      padding: 2px 4px;
    }}
    .section-divider::before,
    .section-divider::after {{
      content: "";
      height: 1px;
      background: var(--panel-border);
      flex: 1;
    }}
    .section-divider span {{
      white-space: nowrap;
      color: #9fb2cc;
      font-weight: 600;
    }}
    .card {{
      position: relative;
      background: var(--panel);
      border: 1px solid var(--panel-border);
      border-radius: 12px;
      padding: 12px;
      min-height: 320px;
      box-shadow: 0 10px 30px rgba(0,0,0,0.3);
      transition: opacity 0.2s ease;
    }}
    .card-wide {{
      grid-column: span 2;
    }}
    body.expanded-active {{
      overflow: hidden;
    }}
    .grid.expanded-active .card {{
      opacity: 0.2;
      pointer-events: none;
    }}
    .grid.expanded-active .card.expanded {{
      opacity: 1;
      pointer-events: auto;
      position: fixed;
      top: 16px;
      right: 16px;
      bottom: 16px;
      left: 16px;
      z-index: 1000;
      min-height: 0;
    }}
    .card-toolbar {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      margin: 2px 4px 8px 4px;
    }}
    .chart-title-btn {{
      font-size: 14px;
      color: var(--muted);
      background: transparent;
      border: 0;
      padding: 0;
      text-align: left;
      cursor: pointer;
      flex: 1;
    }}
    .chart-title-btn:hover {{
      color: var(--text);
    }}
    .chart-title-btn:focus-visible,
    .chart-expand-btn:focus-visible {{
      outline: 1px solid var(--accent);
      outline-offset: 2px;
      border-radius: 4px;
    }}
    .chart-expand-btn {{
      width: 24px;
      height: 24px;
      border: 1px solid var(--panel-border);
      border-radius: 6px;
      background: #111722;
      color: var(--text);
      font-size: 14px;
      line-height: 1;
      cursor: pointer;
    }}
    .chart-content {{
      width: 100%;
    }}
    .table-wrap {{
      height: 300px;
      overflow: auto;
      border: 1px solid var(--panel-border);
      border-radius: 10px;
    }}
    table.missing,
    table.data-table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 13px;
    }}
    table.missing thead th,
    table.data-table thead th {{
      position: sticky;
      top: 0;
      background: #111722;
      text-align: left;
      color: var(--muted);
      padding: 8px;
      border-bottom: 1px solid var(--panel-border);
    }}
    table.missing td,
    table.data-table td {{
      padding: 8px;
      border-bottom: 1px solid #253042;
    }}
    table.missing tr:last-child td,
    table.data-table tr:last-child td {{
      border-bottom: 0;
    }}
    .num-col {{
      text-align: right;
      font-variant-numeric: tabular-nums;
    }}
    @media (max-width: 1100px) {{
      .card-wide {{ grid-column: span 1; }}
    }}
  </style>
</head>
<body>
  <header>
    <h1>Stop Duration Dashboard</h1>
    <p class="subtitle">Linear regression on BIN + routine stops | Source: {input_csv.name}</p>
  </header>

  <section class="stats">
    <div class="stat-card">
      <div class="stat-label">Eligible Rows (bin/routine only)</div>
      <div class="stat-value">{total_eligible_rows:,}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Rows Used in Model</div>
      <div class="stat-value" style="color:var(--good)">{len(clean_df):,}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Rows Dropped for Missing Values</div>
      <div class="stat-value" style="color:var(--warn)">{dropped_rows:,}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Rows With Any Missing Value</div>
      <div class="stat-value" style="color:var(--bad)">{rows_with_any_nan:,}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Mean Absolute Error</div>
      <div class="stat-value">{mae:.4f}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Mean Squared Error</div>
      <div class="stat-value">{mse:.4f}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">R²</div>
      <div class="stat-value">{r2:.4f}</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">Outliers (|planned-actual|/planned &gt; {OUTLIER_DIFF_THRESHOLD_PCT:.0f}%)</div>
      <div class="stat-value" style="color:var(--bad)">{actual_planned_outlier_count:,}</div>
      <div class="stat-label">{actual_planned_outlier_pct:.1f}% of {actual_planned_rule_points:,} planned-vs-actual points</div>
    </div>
    <div class="stat-card">
      <div class="stat-label">One-Hot Mapping Saved</div>
      <div class="stat-value" style="font-size:14px;line-height:1.3">{onehot_mapping_output.name}</div>
    </div>
  </section>

  <section class="grid">
    <div class="card card-wide">
      <div class="card-toolbar">
        <button class="chart-title-btn">Missing Values by Modeling Column</button>
        <button class="chart-expand-btn" aria-label="Expand chart">⤢</button>
      </div>
      <div class="chart-content" style="height:320px;">
        <div class="table-wrap">
          <table class="missing">
            <thead>
              <tr><th>Column</th><th>Missing Count</th><th>Missing %</th></tr>
            </thead>
            <tbody>
              {missing_table_html}
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <div class="card">
      <div class="card-toolbar">
        <button class="chart-title-btn">Scatter: Actual vs Planned Stop Duration (minutes, log-log)</button>
        <button class="chart-expand-btn" aria-label="Expand chart">⤢</button>
      </div>
      <div id="scatter_actual_planned" class="chart-content" style="height:320px;"></div>
    </div>

    <div class="card">
      <div class="card-toolbar">
        <button class="chart-title-btn">Histogram: Planned Stop Duration (minutes)</button>
        <button class="chart-expand-btn" aria-label="Expand chart">⤢</button>
      </div>
      <div id="hist_planned_duration" class="chart-content" style="height:300px;"></div>
    </div>

    <div class="card">
      <div class="card-toolbar">
        <button class="chart-title-btn">Histogram: Actual Stop Duration (minutes)</button>
        <button class="chart-expand-btn" aria-label="Expand chart">⤢</button>
      </div>
      <div id="hist_actual_duration" class="chart-content" style="height:300px;"></div>
    </div>

    <div class="section-divider card-wide"><span>Linear Model</span></div>

    <div class="card">
      <div class="card-toolbar">
        <button class="chart-title-btn">Scatter: Actual vs Predicted Stop Duration (log-log)</button>
        <button class="chart-expand-btn" aria-label="Expand chart">⤢</button>
      </div>
      <div id="scatter_actual_pred" class="chart-content" style="height:320px;"></div>
    </div>

    <div class="card">
      <div class="card-toolbar">
        <button class="chart-title-btn">Feature Importance (Numeric: signed coef | Categorical: mean |coef|)</button>
        <button class="chart-expand-btn" aria-label="Expand chart">⤢</button>
      </div>
      <div id="bar_feature_importance" class="chart-content" style="height:320px;"></div>
    </div>

    <div class="card">
      <div class="card-toolbar">
        <button class="chart-title-btn">Histogram: Raw Driver Coefficients</button>
        <button class="chart-expand-btn" aria-label="Expand chart">⤢</button>
      </div>
      <div id="hist_driver_coef" class="chart-content" style="height:300px;"></div>
    </div>

    <div class="card">
      <div class="card-toolbar">
        <button class="chart-title-btn">Histogram: Raw Location Coefficients</button>
        <button class="chart-expand-btn" aria-label="Expand chart">⤢</button>
      </div>
      <div id="hist_location_coef" class="chart-content" style="height:300px;"></div>
    </div>

    <div class="card card-wide">
      <div class="card-toolbar">
        <button class="chart-title-btn">Table: Driver Coefficients (All Drivers)</button>
        <button class="chart-expand-btn" aria-label="Expand chart">⤢</button>
      </div>
      <div class="chart-content" style="height:320px;">
        <div class="table-wrap">
          <table class="data-table">
            <thead>
              <tr><th>Driver</th><th class="num-col">Coefficient</th><th class="num-col">|Coefficient|</th></tr>
            </thead>
            <tbody>
              {driver_coeff_table_html}
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <div class="card card-wide">
      <div class="card-toolbar">
        <button class="chart-title-btn">Table: Top 10 Location Coefficients (Display Name)</button>
        <button class="chart-expand-btn" aria-label="Expand chart">⤢</button>
      </div>
      <div class="chart-content" style="height:320px;">
        <div class="table-wrap">
          <table class="data-table">
            <thead>
              <tr><th>Display Name</th><th class="num-col">Coefficient</th><th class="num-col">|Coefficient|</th></tr>
            </thead>
            <tbody>
              {location_top10_table_html}
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <div class="card">
      <div class="card-toolbar">
        <button class="chart-title-btn">Bar Chart: Day-of-Week Coefficients</button>
        <button class="chart-expand-btn" aria-label="Expand chart">⤢</button>
      </div>
      <div id="hist_day_of_week_coef" class="chart-content" style="height:300px;"></div>
    </div>

    <div class="card">
      <div class="card-toolbar">
        <button class="chart-title-btn">Bar Chart: Month-of-Year Coefficients</button>
        <button class="chart-expand-btn" aria-label="Expand chart">⤢</button>
      </div>
      <div id="hist_month_of_year_coef" class="chart-content" style="height:300px;"></div>
    </div>
  </section>

  <script>
    const actualValues = {json.dumps(scatter_actual.tolist(), separators=(",", ":"))};
    const predictedValues = {json.dumps(scatter_predicted.tolist(), separators=(",", ":"))};
    const scatterMeta = {json.dumps(scatter_meta, separators=(",", ":"), ensure_ascii=False)};
    const refLine = [{ref_min}, {ref_max}];
    const actualPlannedInlierActualValues = {json.dumps(scatter_actual_planned_inlier_actual.tolist(), separators=(",", ":"))};
    const actualPlannedInlierPlannedValues = {json.dumps(scatter_actual_planned_inlier_planned.tolist(), separators=(",", ":"))};
    const scatterActualPlannedInlierMeta = {json.dumps(scatter_actual_planned_inlier_meta, separators=(",", ":"), ensure_ascii=False)};
    const actualPlannedOutlierActualValues = {json.dumps(scatter_actual_planned_outlier_actual.tolist(), separators=(",", ":"))};
    const actualPlannedOutlierPlannedValues = {json.dumps(scatter_actual_planned_outlier_planned.tolist(), separators=(",", ":"))};
    const scatterActualPlannedOutlierMeta = {json.dumps(scatter_actual_planned_outlier_meta, separators=(",", ":"), ensure_ascii=False)};
    const refLineActualPlanned = [{ref_min_ap}, {ref_max_ap}];
    const plannedDurationHistValues = {json.dumps(planned_duration_hist_values.tolist(), separators=(",", ":"))};
    const actualDurationHistValues = {json.dumps(actual_duration_hist_values.tolist(), separators=(",", ":"))};

    const importanceFeatures = {json.dumps(importance_df["feature"].tolist(), ensure_ascii=False)};
    const importanceSigned = {json.dumps(importance_df["signed_coefficient"].tolist(), separators=(",", ":"))};
    const importanceAbs = {json.dumps(importance_df["importance_abs"].tolist(), separators=(",", ":"))};
    const importanceGroup = {json.dumps(importance_df["group"].tolist())};
    const importanceColors = importanceSigned.map((v, idx) => {{
      if (importanceGroup[idx] === 'categorical') return '#5ad2f4';
      return v >= 0 ? '#8bd17c' : '#ff8e72';
    }});

    const driverCoefs = {json.dumps(driver_coefs.tolist(), separators=(",", ":"))};
    const locationCoefs = {json.dumps(location_coefs.tolist(), separators=(",", ":"))};
    const dayOfWeekCoefs = {json.dumps(day_of_week_coefs.tolist(), separators=(",", ":"))};
    const monthOfYearCoefs = {json.dumps(month_of_year_coefs.tolist(), separators=(",", ":"))};
    const dayOfWeekCategories = {json.dumps(day_of_week_categories, ensure_ascii=False)};
    const monthOfYearCategories = {json.dumps(month_of_year_categories, ensure_ascii=False)};

    const baseLayout = {{
      margin: {{t: 10, r: 10, b: 45, l: 55}},
      paper_bgcolor: 'rgba(0,0,0,0)',
      plot_bgcolor: 'rgba(0,0,0,0)',
      font: {{color: '#e6edf3'}}
    }};

    Plotly.newPlot('scatter_actual_pred', [
      {{
        x: actualValues,
        y: predictedValues,
        mode: 'markers',
        type: 'scatter',
        marker: {{size: 6, color: '#5ad2f4', opacity: 0.65}},
        customdata: scatterMeta,
        hovertemplate:
          'Actual: %{{x:.3f}}<br>' +
          'Predicted: %{{y:.3f}}<br>' +
          'Date: %{{customdata[0]}}<br>' +
          'Driver: %{{customdata[1]}}<br>' +
          'Display Name: %{{customdata[2]}}<br>' +
          'Planned Duration (min): %{{customdata[3]}}<extra></extra>'
      }},
      {{
        x: refLine,
        y: refLine,
        mode: 'lines',
        line: {{dash: 'dot', color: '#a4b1c4'}},
        hoverinfo: 'skip'
      }}
    ], {{
      ...baseLayout,
      xaxis: {{title: 'Actual Duration (minutes, log)', type: 'log'}},
      yaxis: {{title: 'Predicted Duration (minutes, log)', type: 'log'}}
    }}, {{displayModeBar: false}});

    Plotly.newPlot('hist_planned_duration', [{{
      x: plannedDurationHistValues,
      type: 'histogram',
      marker: {{color: '#8bb4ff'}},
      hovertemplate: 'Planned duration bin: %{{x:.3f}}<br>Count: %{{y}}<extra></extra>'
    }}], {{
      ...baseLayout,
      xaxis: {{title: 'Planned Duration (minutes)'}},
      yaxis: {{title: 'Count'}}
    }}, {{displayModeBar: false}});

    Plotly.newPlot('hist_actual_duration', [{{
      x: actualDurationHistValues,
      type: 'histogram',
      marker: {{color: '#ff9f7a'}},
      hovertemplate: 'Actual duration bin: %{{x:.3f}}<br>Count: %{{y}}<extra></extra>'
    }}], {{
      ...baseLayout,
      xaxis: {{title: 'Actual Duration (minutes)'}},
      yaxis: {{title: 'Count'}}
    }}, {{displayModeBar: false}});

    Plotly.newPlot('scatter_actual_planned', [
      {{
        x: actualPlannedInlierPlannedValues,
        y: actualPlannedInlierActualValues,
        mode: 'markers',
        type: 'scatter',
        marker: {{size: 6, color: '#ffcf6e', opacity: 0.65}},
        customdata: scatterActualPlannedInlierMeta,
        hovertemplate:
          'Planned: %{{x:.3f}}<br>' +
          'Actual: %{{y:.3f}}<br>' +
          'Percent Difference: %{{customdata[3]}}%<br>' +
          'Date: %{{customdata[0]}}<br>' +
          'Driver: %{{customdata[1]}}<br>' +
          'Display Name: %{{customdata[2]}}<br>' +
          'Outlier: No<extra></extra>'
      }},
      {{
        x: actualPlannedOutlierPlannedValues,
        y: actualPlannedOutlierActualValues,
        mode: 'markers',
        type: 'scatter',
        marker: {{size: 7, color: '#ff4d5f', opacity: 0.85}},
        customdata: scatterActualPlannedOutlierMeta,
        hovertemplate:
          'Planned: %{{x:.3f}}<br>' +
          'Actual: %{{y:.3f}}<br>' +
          'Percent Difference: %{{customdata[3]}}%<br>' +
          'Date: %{{customdata[0]}}<br>' +
          'Driver: %{{customdata[1]}}<br>' +
          'Display Name: %{{customdata[2]}}<br>' +
          'Outlier: Yes<extra></extra>'
      }},
      {{
        x: refLineActualPlanned,
        y: refLineActualPlanned,
        mode: 'lines',
        line: {{dash: 'dot', color: '#a4b1c4'}},
        hoverinfo: 'skip'
      }}
    ], {{
      ...baseLayout,
      xaxis: {{title: 'Planned Duration (minutes, log)', type: 'log'}},
      yaxis: {{title: 'Actual Duration (minutes, log)', type: 'log'}}
    }}, {{displayModeBar: false}});

    Plotly.newPlot('bar_feature_importance', [{{
      x: importanceFeatures,
      y: importanceSigned,
      type: 'bar',
      marker: {{color: importanceColors}},
      customdata: importanceAbs.map((abs, idx) => [abs, importanceGroup[idx]]),
      hovertemplate:
        'Feature: %{{x}}<br>' +
        'Plotted value: %{{y:.5f}}<br>' +
        '|coefficient|: %{{customdata[0]:.5f}}<br>' +
        'Group: %{{customdata[1]}}<extra></extra>'
    }}], {{
      ...baseLayout,
      margin: {{...baseLayout.margin, b: 68}},
      xaxis: {{title: 'Feature', tickangle: -35, automargin: true}},
      yaxis: {{title: 'Coefficient value'}}
    }}, {{displayModeBar: false}});

    Plotly.newPlot('hist_driver_coef', [{{
      x: driverCoefs,
      type: 'histogram',
      marker: {{color: '#ffcf6e'}},
      hovertemplate: 'Coefficient bin: %{{x:.5f}}<br>Count: %{{y}}<extra></extra>'
    }}], {{
      ...baseLayout,
      xaxis: {{title: 'Driver coefficient value'}},
      yaxis: {{title: 'Count'}}
    }}, {{displayModeBar: false}});

    Plotly.newPlot('hist_location_coef', [{{
      x: locationCoefs,
      type: 'histogram',
      marker: {{color: '#5ad2f4'}},
      hovertemplate: 'Coefficient bin: %{{x:.5f}}<br>Count: %{{y}}<extra></extra>'
    }}], {{
      ...baseLayout,
      xaxis: {{title: 'Location coefficient value'}},
      yaxis: {{title: 'Count'}}
    }}, {{displayModeBar: false}});

    Plotly.newPlot('hist_day_of_week_coef', [{{
      x: dayOfWeekCategories,
      y: dayOfWeekCoefs,
      type: 'bar',
      marker: {{color: '#8bd17c'}},
      hovertemplate: 'Day of week: %{{x}}<br>Coefficient: %{{y:.5f}}<extra></extra>'
    }}], {{
      ...baseLayout,
      xaxis: {{title: 'Day of week'}},
      yaxis: {{title: 'Coefficient'}}
    }}, {{displayModeBar: false}});

    Plotly.newPlot('hist_month_of_year_coef', [{{
      x: monthOfYearCategories,
      y: monthOfYearCoefs,
      type: 'bar',
      marker: {{color: '#f8b195'}},
      hovertemplate: 'Month of year: %{{x}}<br>Coefficient: %{{y:.5f}}<extra></extra>'
    }}], {{
      ...baseLayout,
      xaxis: {{title: 'Month of year'}},
      yaxis: {{title: 'Coefficient'}}
    }}, {{displayModeBar: false}});

    const grid = document.querySelector('.grid');
    const cards = Array.from(document.querySelectorAll('.card'));
    let expandedCard = null;

    function resizeCardContent(card) {{
      const content = card ? card.querySelector('.chart-content') : null;
      if (!content) return;
      const plotTarget = content.querySelector('div[id^=\"scatter_\"], div[id^=\"bar_\"], div[id^=\"hist_\"]') || content;
      if (plotTarget && plotTarget.id) {{
        Plotly.Plots.resize(plotTarget);
      }}
    }}

    function setExpandedUI(card, expanded) {{
      const expandBtn = card.querySelector('.chart-expand-btn');
      if (!expandBtn) return;
      expandBtn.textContent = expanded ? '✕' : '⤢';
      expandBtn.setAttribute('aria-label', expanded ? 'Collapse chart' : 'Expand chart');
    }}

    function collapseCard(card) {{
      if (!card) return;
      const content = card.querySelector('.chart-content');
      card.classList.remove('expanded');
      setExpandedUI(card, false);
      if (content && content.dataset.defaultHeight) {{
        content.style.height = content.dataset.defaultHeight;
      }}
      grid.classList.remove('expanded-active');
      document.body.classList.remove('expanded-active');
      expandedCard = null;
      setTimeout(() => resizeCardContent(card), 40);
    }}

    function expandCard(card) {{
      const content = card.querySelector('.chart-content');
      if (!content) return;
      if (expandedCard && expandedCard !== card) {{
        collapseCard(expandedCard);
      }}
      card.classList.add('expanded');
      setExpandedUI(card, true);
      content.style.height = 'calc(100vh - 130px)';
      grid.classList.add('expanded-active');
      document.body.classList.add('expanded-active');
      expandedCard = card;
      setTimeout(() => resizeCardContent(card), 40);
    }}

    cards.forEach((card) => {{
      const content = card.querySelector('.chart-content');
      const titleBtn = card.querySelector('.chart-title-btn');
      const expandBtn = card.querySelector('.chart-expand-btn');
      if (!content || !titleBtn || !expandBtn) return;
      content.dataset.defaultHeight = content.style.height || '320px';
      const toggle = () => {{
        if (expandedCard === card) {{
          collapseCard(card);
        }} else {{
          expandCard(card);
        }}
      }};
      titleBtn.addEventListener('click', toggle);
      expandBtn.addEventListener('click', (e) => {{
        e.stopPropagation();
        toggle();
      }});
    }});

    document.addEventListener('keydown', (e) => {{
      if (e.key === 'Escape' && expandedCard) {{
        collapseCard(expandedCard);
      }}
    }});

    window.addEventListener('resize', () => {{
      if (expandedCard) {{
        resizeCardContent(expandedCard);
      }}
    }});
  </script>
</body>
</html>
"""

    output_html.parent.mkdir(parents=True, exist_ok=True)
    output_html.write_text(html, encoding="utf-8")

    print(f"Wrote stop-duration dashboard: {output_html}")
    print(f"Wrote model coefficients CSV: {coefficients_output}")
    print(f"Wrote one-hot mapping JSON: {onehot_mapping_output}")
    print(
        "Model summary: "
        f"eligible_rows={total_eligible_rows}, "
        f"model_rows={len(clean_df)}, "
        f"rows_with_nan={rows_with_any_nan}, "
        f"mae={mae:.5f}, mse={mse:.5f}, r2={r2:.5f}"
    )


if __name__ == "__main__":
    main()
