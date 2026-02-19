#!/usr/bin/env python3
"""Run all data creation and visualization scripts end-to-end."""

import argparse
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple

import create_city_data
import create_location_data
import create_route_data

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

INPUT_STOP_CSV = PROJECT_ROOT / "data_geocode" / "latest" / "data_geocode.csv"
LOCATION_CSV = SCRIPT_DIR / "data_locations.csv"
ROUTE_CSV = SCRIPT_DIR / "data_route.csv"
CITY_CSV = SCRIPT_DIR / "data_city.csv"
BINS_CSV = SCRIPT_DIR / "data_bins.csv"
ROUTINE_CSV = SCRIPT_DIR / "data_routine.csv"
ACTIVE_BINS_CSV = SCRIPT_DIR / "data_active_bins.csv"
ACTIVE_ROUTINE_CSV = SCRIPT_DIR / "data_active_routine.csv"
ACTIVE_SAVERS_CSV = SCRIPT_DIR / "data_active_savers.csv"
PERSISTENT_SAVERS_SEED_CSV = SCRIPT_DIR / "persistent_savers_addresses.csv"
SAVERS_CSV = SCRIPT_DIR / "data_savers.csv"
STOP_DURATION_CSV = SCRIPT_DIR / "data_stop_duration.csv"

DASH_LOCATION_HTML = SCRIPT_DIR / "dash_location.html"
DASH_ROUTE_HTML = SCRIPT_DIR / "dash_route.html"
DASH_CITY_HTML = SCRIPT_DIR / "dash_city.html"
DASH_BINS_HTML = SCRIPT_DIR / "dash_bins.html"
DASH_STOP_DURATION_HTML = SCRIPT_DIR / "dash_stop_duration.html"
DASH_ANALYSIS_TREE_HTML = SCRIPT_DIR / "dash_data_analysis_tree.html"
DASH_HEADER_HTML = SCRIPT_DIR / "dash_header.html"
DASH_ALL_IN_ONE_HTML = SCRIPT_DIR / "dash_all_in_one.html"

DEFAULT_OSRM_BASE_URL = "http://localhost:5000"
VENV_PYTHON = PROJECT_ROOT / ".venv" / "bin" / "python"

# (label, start_date, end_date, output_html)
JAN_ROUTE_MAPS: List[Tuple[str, str, str, Path]] = [
    (
        "Jan-2022",
        "2022-01-01",
        "2022-01-31",
        SCRIPT_DIR / "dash_route_map_range_jan-2022.html",
    ),
    (
        "Jan-2023",
        "2023-01-01",
        "2023-01-31",
        SCRIPT_DIR / "dash_route_map_range_jan-2023.html",
    ),
    (
        "Sep-2024",
        "2024-09-01",
        "2024-09-30",
        SCRIPT_DIR / "dash_route_map_range_sep-2024.html",
    ),
]


def python_has_pandas(python_bin: str) -> bool:
    try:
        result = subprocess.run(
            [python_bin, "-c", "import pandas"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return result.returncode == 0
    except Exception:
        return False


def choose_python(require_pandas: bool = False) -> str:
    current = sys.executable
    if not require_pandas:
        return current

    if python_has_pandas(current):
        return current

    if VENV_PYTHON.exists():
        venv_bin = str(VENV_PYTHON)
        if python_has_pandas(venv_bin):
            return venv_bin

    return current


def run_python_script(script_name: str, args: list[str], require_pandas: bool = False) -> None:
    script_path = SCRIPT_DIR / script_name
    python_bin = choose_python(require_pandas=require_pandas)
    cmd = [python_bin, str(script_path), *args]
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True, cwd=SCRIPT_DIR)


def main() -> None:
    parser = argparse.ArgumentParser(description="Create all data files and dashboards.")
    parser.add_argument(
        "--input",
        default=str(INPUT_STOP_CSV),
        help="Stop-level CSV input for location/route creation",
    )
    parser.add_argument(
        "--osrm-base-url",
        default=DEFAULT_OSRM_BASE_URL,
        help="OSRM base URL",
    )
    args = parser.parse_args()

    input_csv = Path(args.input).expanduser().resolve()
    if not input_csv.exists():
        raise SystemExit(f"Input CSV not found: {input_csv}")

    create_location_data.create_location_data(input_csv, LOCATION_CSV)
    create_route_data.create_route_data(input_csv, ROUTE_CSV)
    create_city_data.create_city_data(LOCATION_CSV, CITY_CSV)
    run_python_script(
        "create_bins_data.py",
        [
            "--data",
            str(input_csv),
            "--bins-output",
            str(BINS_CSV),
            "--routine-output",
            str(ROUTINE_CSV),
            "--savers-seed",
            str(PERSISTENT_SAVERS_SEED_CSV),
            "--savers-output",
            str(SAVERS_CSV),
        ],
        require_pandas=True,
    )
    run_python_script(
        "create_active_schedule.py",
        [
            "--data",
            str(input_csv),
            "--bins",
            str(BINS_CSV),
            "--routine",
            str(ROUTINE_CSV),
            "--savers",
            str(SAVERS_CSV),
            "--bins-output",
            str(ACTIVE_BINS_CSV),
            "--routine-output",
            str(ACTIVE_ROUTINE_CSV),
            "--savers-output",
            str(ACTIVE_SAVERS_CSV),
        ],
        require_pandas=True,
    )
    run_python_script(
        "create_stop_ducation.py",
        [
            "--data",
            str(input_csv),
            "--active-bins",
            str(ACTIVE_BINS_CSV),
            "--active-routine",
            str(ACTIVE_ROUTINE_CSV),
            "--bins",
            str(BINS_CSV),
            "--output",
            str(STOP_DURATION_CSV),
        ],
        require_pandas=True,
    )

    run_python_script(
        "viz_location_data.py",
        ["--input", str(LOCATION_CSV), "--output", str(DASH_LOCATION_HTML)],
    )
    run_python_script(
        "viz_route_data.py",
        ["--input", str(ROUTE_CSV), "--output", str(DASH_ROUTE_HTML)],
    )
    run_python_script(
        "viz_city_data.py",
        ["--input", str(CITY_CSV), "--output", str(DASH_CITY_HTML)],
    )
    run_python_script(
        "viz_bins.py",
        [
            "--data",
            str(input_csv),
            "--bins",
            str(BINS_CSV),
            "--routine",
            str(ROUTINE_CSV),
            "--output",
            str(DASH_BINS_HTML),
        ],
    )
    run_python_script(
        "viz_stop_duration.py",
        ["--input", str(STOP_DURATION_CSV), "--output", str(DASH_STOP_DURATION_HTML)],
        require_pandas=True,
    )
    run_python_script(
        "viz_analysis_tree.py",
        ["--output", str(DASH_ANALYSIS_TREE_HTML)],
    )
    for _, start_date, end_date, output_html in JAN_ROUTE_MAPS:
        run_python_script(
            "viz_map_routes_on_road.py",
            [
                "--input",
                str(input_csv),
                "--output",
                str(output_html),
                "--range",
                f"{start_date},{end_date}",
                "--osrm-base-url",
                args.osrm_base_url,
            ],
            require_pandas=True,
        )

    header_args = [
        "--output",
        str(DASH_HEADER_HTML),
        "--page",
        f"{DASH_ROUTE_HTML.name}=Route",
        "--page",
        f"{DASH_LOCATION_HTML.name}=Location",
        "--page",
        f"{DASH_CITY_HTML.name}=City",
        "--page",
        f"{DASH_BINS_HTML.name}=Bins",
        "--page",
        f"{DASH_STOP_DURATION_HTML.name}=Stop Duration",
        "--page",
        f"{DASH_ANALYSIS_TREE_HTML.name}=Analysis Tree",
    ]
    for label, _, _, output_html in JAN_ROUTE_MAPS:
        header_args.extend(["--page", f"{output_html.name}={label}"])
    run_python_script("viz_header.py", header_args)

    bundle_args = [
        "--output",
        str(DASH_ALL_IN_ONE_HTML),
        "--title",
        "Data Analysis Dashboard Bundle",
        "--page",
        f"{DASH_ROUTE_HTML.name}=Route",
        "--page",
        f"{DASH_LOCATION_HTML.name}=Location",
        "--page",
        f"{DASH_CITY_HTML.name}=City",
        "--page",
        f"{DASH_BINS_HTML.name}=Bins",
        "--page",
        f"{DASH_STOP_DURATION_HTML.name}=Stop Duration",
        "--page",
        f"{DASH_ANALYSIS_TREE_HTML.name}=Analysis Tree",
    ]
    for label, _, _, output_html in JAN_ROUTE_MAPS:
        bundle_args.extend(["--page", f"{output_html.name}={label}"])
    run_python_script("viz_combined_dash.py", bundle_args)

    print("Visualization pipeline complete.")
    print(f"Hub page: {DASH_HEADER_HTML}")
    print(f"Standalone bundle: {DASH_ALL_IN_ONE_HTML}")


if __name__ == "__main__":
    main()
