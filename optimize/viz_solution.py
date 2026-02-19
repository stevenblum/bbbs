#!/usr/bin/env python3
"""Build a side-by-side HTML comparison of actual routes vs solved routes.

Workflow:
1. Run `visualizations/viz_map_routes_on_road.py` for actual route data.
2. Run `visualizations/viz_map_routes_on_road.py` for solution route data.
3. Write a comparison HTML page with two iframes:
   - Left: Actual routes driven.
   - Right: Optimal solution routes.
"""

from __future__ import annotations

import argparse
import html
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

DEFAULT_ACTUAL_CSV = PROJECT_ROOT / "data_geocode" / "latest" / "data_geocode.csv"
DEFAULT_VIZ_MAP_SCRIPT = PROJECT_ROOT / "visualizations" / "viz_map_routes_on_road.py"


def log(message: str, quiet: bool) -> None:
    if not quiet:
        print(f"[viz_solution] {message}")


def run_map_viz(
    *,
    python_executable: str,
    viz_map_script: Path,
    input_csv: Path,
    output_html: Path,
    start_date: str,
    end_date: str,
    quiet: bool,
) -> None:
    command = [
        python_executable,
        str(viz_map_script),
        "--input",
        str(input_csv),
        "--output",
        str(output_html),
        "--start-date",
        start_date,
        "--end-date",
        end_date,
        "--allow-straight-line-fallback",
    ]

    log(f"Generating map with {input_csv}", quiet=quiet)
    subprocess.run(command, check=True)
    log(f"Map written: {output_html}", quiet=quiet)


def _normalize_constraint_enabled(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if not text:
        return True
    return text not in {"0", "false", "f", "no", "n", "off"}


def _constraint_rows_html(constraints: list[dict[str, Any]]) -> str:
    if not constraints:
        return (
            "<tr>"
            "<td colspan='2' class='constraints-empty'>No constraints configured.</td>"
            "</tr>"
        )

    rows: list[str] = []
    for constraint in constraints:
        if not isinstance(constraint, dict):
            continue
        if not _normalize_constraint_enabled(constraint.get("enabled", True)):
            continue

        constraint_type_raw = str(constraint.get("type", "")).strip() or "Unknown"
        constraint_type = html.escape(constraint_type_raw)
        detail_parts: list[str] = []
        detail_parts_plain: list[str] = []
        for key, value in constraint.items():
            if key in {"type", "enabled"}:
                continue
            value_text = str(value).strip()
            if not value_text:
                continue
            key_text = str(key).strip()
            detail_parts_plain.append(f"{key_text}: {value_text}")
            detail_parts.append(
                f"<span class='constraint-key'>{html.escape(key_text)}</span>: "
                f"{html.escape(value_text)}"
            )
        details = " | ".join(detail_parts) if detail_parts else "n/a"
        details_plain = " | ".join(detail_parts_plain) if detail_parts_plain else "n/a"
        rows.append(
            "<tr>"
            f"<td title='{html.escape(constraint_type_raw)}'>{constraint_type}</td>"
            f"<td title='{html.escape(details_plain)}'>{details}</td>"
            "</tr>"
        )

    if not rows:
        return (
            "<tr>"
            "<td colspan='2' class='constraints-empty'>No enabled constraints.</td>"
            "</tr>"
        )
    return "".join(rows)


def write_comparison_html(
    *,
    output_html: Path,
    start_date: str,
    end_date: str,
    actual_map_html: Path,
    solution_map_html: Path,
    constraints: list[dict[str, Any]],
) -> None:
    actual_src = html.escape(actual_map_html.name)
    solution_src = html.escape(solution_map_html.name)
    date_range = html.escape(f"{start_date} to {end_date}")
    constraints_rows = _constraint_rows_html(constraints)

    doc = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Route Comparison</title>
  <style>
    html, body {{
      margin: 0;
      padding: 0;
      height: 100%;
      font-family: Arial, sans-serif;
      background: #f7f7f9;
      color: #1f2937;
      display: flex;
      flex-direction: column;
    }}
    .header {{
      border-bottom: 1px solid #d1d5db;
      background: #ffffff;
      padding: 14px 16px;
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
      align-items: start;
      box-sizing: border-box;
    }}
    .header-left {{
      min-width: 0;
    }}
    .header-right {{
      min-width: 0;
      border-left: 1px solid #e5e7eb;
      padding-left: 16px;
    }}
    .title {{
      margin: 0;
      font-size: 20px;
      font-weight: 700;
    }}
    .subtitle {{
      margin: 6px 0 0 0;
      font-size: 13px;
      color: #4b5563;
    }}
    .constraints-title {{
      margin: 0 0 6px 0;
      font-size: 13px;
      font-weight: 700;
      color: #374151;
    }}
    .constraints-table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 12px;
      table-layout: fixed;
    }}
    .constraints-table th,
    .constraints-table td {{
      border: 1px solid #e5e7eb;
      padding: 4px 6px;
      vertical-align: middle;
      text-align: left;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      max-width: 0;
    }}
    .constraints-table th {{
      background: #f3f4f6;
      font-weight: 700;
    }}
    .constraints-table th:first-child,
    .constraints-table td:first-child {{
      width: 14%;
    }}
    .constraint-key {{
      font-weight: 700;
    }}
    .constraints-empty {{
      color: #6b7280;
      font-style: italic;
    }}
    .constraints-table-wrap {{
      max-height: 132px;
      overflow: auto;
      border: 1px solid #e5e7eb;
      border-radius: 6px;
      background: #fff;
    }}
    .constraints-table-wrap .constraints-table {{
      border-collapse: separate;
      border-spacing: 0;
    }}
    .constraints-table-wrap .constraints-table th {{
      position: sticky;
      top: 0;
      z-index: 1;
    }}
    .grid {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 10px;
      padding: 10px;
      flex: 1;
      min-height: 0;
      box-sizing: border-box;
    }}
    .panel {{
      border: 1px solid #d1d5db;
      border-radius: 8px;
      overflow: hidden;
      background: #fff;
      display: flex;
      flex-direction: column;
      min-height: 0;
    }}
    .panel h2 {{
      margin: 0;
      padding: 10px 12px;
      font-size: 14px;
      background: #f3f4f6;
      border-bottom: 1px solid #d1d5db;
    }}
    .panel iframe {{
      border: 0;
      width: 100%;
      height: 100%;
      flex: 1;
      min-height: 0;
    }}
    @media (max-width: 1100px) {{
      .header {{
        grid-template-columns: 1fr;
      }}
      .header-right {{
        border-left: 0;
        border-top: 1px solid #e5e7eb;
        padding-left: 0;
        padding-top: 10px;
      }}
    }}
  </style>
</head>
<body>
  <div class="header">
    <div class="header-left">
      <h1 class="title">Route Comparison: Actual vs Optimal Solution</h1>
      <p class="subtitle">Date range: {date_range}</p>
    </div>
    <div class="header-right">
      <p class="constraints-title">Active Constraints</p>
      <div class="constraints-table-wrap">
        <table class="constraints-table">
          <thead>
            <tr><th>Type</th><th>Definition</th></tr>
          </thead>
          <tbody>
            {constraints_rows}
          </tbody>
        </table>
      </div>
    </div>
  </div>
  <div class="grid">
    <section class="panel">
      <h2>Actual Routes Driven (Left)</h2>
      <iframe src="{actual_src}" title="Actual Routes"></iframe>
    </section>
    <section class="panel">
      <h2>Optimal Solution Routes (Right)</h2>
      <iframe src="{solution_src}" title="Optimal Solution Routes"></iframe>
    </section>
  </div>
</body>
</html>
"""

    output_html.parent.mkdir(parents=True, exist_ok=True)
    output_html.write_text(doc, encoding="utf-8")


def derive_default_output(solution_csv: Path, start_date: str, end_date: str) -> Path:
    stem = solution_csv.stem
    return (SCRIPT_DIR / f"viz_solution_{stem}_{start_date}_{end_date}.html").resolve()


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Generate side-by-side HTML comparison of actual data routes vs solution routes."
        )
    )
    parser.add_argument(
        "solution_csv",
        help="Solution CSV generated by solve_problem_ortools.py",
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date (YYYY-MM-DD) for both route maps.",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date (YYYY-MM-DD) for both route maps.",
    )
    parser.add_argument(
        "--output",
        default="",
        help="Output comparison HTML path.",
    )
    parser.add_argument(
        "--actual-csv",
        default=str(DEFAULT_ACTUAL_CSV),
        help="Actual route CSV path (defaults to data_geocode/latest/data_geocode.csv).",
    )
    parser.add_argument(
        "--viz-map-script",
        default=str(DEFAULT_VIZ_MAP_SCRIPT),
        help="Path to visualizations/viz_map_routes_on_road.py",
    )
    parser.add_argument(
        "--python-executable",
        default=sys.executable,
        help="Python executable used to run viz_map_routes_on_road.py",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress logs from this wrapper script.",
    )
    parser.add_argument(
        "--constraints-json",
        default="",
        help=(
            "Optional JSON array of constraint objects to show in the comparison "
            "header table."
        ),
    )
    args = parser.parse_args()

    quiet = bool(args.quiet)
    solution_csv = Path(args.solution_csv).expanduser().resolve()
    actual_csv = Path(args.actual_csv).expanduser().resolve()
    viz_map_script = Path(args.viz_map_script).expanduser().resolve()

    if not solution_csv.exists():
        raise SystemExit(f"Solution CSV not found: {solution_csv}")
    if not actual_csv.exists():
        raise SystemExit(f"Actual CSV not found: {actual_csv}")
    if not viz_map_script.exists():
        raise SystemExit(f"viz_map_routes_on_road.py not found: {viz_map_script}")

    if str(args.output).strip():
        output_html = Path(args.output).expanduser().resolve()
    else:
        output_html = derive_default_output(
            solution_csv=solution_csv,
            start_date=str(args.start_date).strip(),
            end_date=str(args.end_date).strip(),
        )

    actual_map_html = output_html.with_name(output_html.stem + "_actual_map.html")
    solution_map_html = output_html.with_name(output_html.stem + "_solution_map.html")

    constraints: list[dict[str, Any]] = []
    constraints_json_raw = str(args.constraints_json).strip()
    if constraints_json_raw:
        try:
            parsed = json.loads(constraints_json_raw)
        except json.JSONDecodeError as exc:
            raise SystemExit("Invalid --constraints-json payload; expected JSON array.") from exc
        if not isinstance(parsed, list):
            raise SystemExit("Invalid --constraints-json payload; expected JSON array.")
        constraints = [item for item in parsed if isinstance(item, dict)]

    log("Creating actual route map (left panel).", quiet=quiet)
    run_map_viz(
        python_executable=str(args.python_executable),
        viz_map_script=viz_map_script,
        input_csv=actual_csv,
        output_html=actual_map_html,
        start_date=str(args.start_date).strip(),
        end_date=str(args.end_date).strip(),
        quiet=quiet,
    )

    log("Creating solution route map (right panel).", quiet=quiet)
    run_map_viz(
        python_executable=str(args.python_executable),
        viz_map_script=viz_map_script,
        input_csv=solution_csv,
        output_html=solution_map_html,
        start_date=str(args.start_date).strip(),
        end_date=str(args.end_date).strip(),
        quiet=quiet,
    )

    write_comparison_html(
        output_html=output_html,
        start_date=str(args.start_date).strip(),
        end_date=str(args.end_date).strip(),
        actual_map_html=actual_map_html,
        solution_map_html=solution_map_html,
        constraints=constraints,
    )
    log(f"Comparison HTML written: {output_html}", quiet=quiet)


if __name__ == "__main__":
    main()
