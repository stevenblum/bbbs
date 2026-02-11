#!/usr/bin/env python3
"""Create city-level aggregates from location-level data."""

import argparse
import csv
from pathlib import Path
from collections import defaultdict
from typing import Dict, Optional, Union

SCRIPT_DIR = Path(__file__).resolve().parent
INPUT_CSV = SCRIPT_DIR / "data_locations.csv"
OUTPUT_CSV = SCRIPT_DIR / "data_city.csv"


def to_int(value: Optional[str]) -> int:
    if value is None:
        return 0
    raw = str(value).strip()
    if not raw:
        return 0
    try:
        return int(float(raw))
    except ValueError:
        return 0


def to_float(value: Optional[str]) -> Optional[float]:
    if value is None:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def create_city_data(
    input_csv: Union[str, Path] = INPUT_CSV,
    output_csv: Union[str, Path] = OUTPUT_CSV,
) -> int:
    with open(input_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames or []

    city_column = "city"
    if city_column not in fieldnames:
        raise ValueError(f"Expected a '{city_column}' column in {input_csv}")

    grouped: Dict[str, Dict[str, float]] = defaultdict(
        lambda: {
            "number_of_locations": 0,
            "number_of_stops": 0,
            "lat_sum": 0.0,
            "lon_sum": 0.0,
            "latlon_count": 0,
        }
    )

    for row in rows:
        city = (row.get(city_column) or "").strip()
        if not city:
            city = "UNKNOWN"

        grouped[city]["number_of_locations"] += 1
        grouped[city]["number_of_stops"] += to_int(row.get("total_number_of_stops"))
        lat = to_float(row.get("latitude"))
        lon = to_float(row.get("longitude"))
        if lat is not None and lon is not None:
            grouped[city]["lat_sum"] += lat
            grouped[city]["lon_sum"] += lon
            grouped[city]["latlon_count"] += 1

    output_rows = []
    for city in sorted(grouped.keys()):
        latlon_count = int(grouped[city]["latlon_count"])
        latitude = (
            f"{(grouped[city]['lat_sum'] / latlon_count):.7f}" if latlon_count else ""
        )
        longitude = (
            f"{(grouped[city]['lon_sum'] / latlon_count):.7f}" if latlon_count else ""
        )
        output_rows.append(
            {
                "city": city,
                "number_of_locations": int(grouped[city]["number_of_locations"]),
                "number_of_stops": int(grouped[city]["number_of_stops"]),
                "latitude": latitude,
                "longitude": longitude,
            }
        )

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "city",
                "number_of_locations",
                "number_of_stops",
                "latitude",
                "longitude",
            ],
        )
        writer.writeheader()
        writer.writerows(output_rows)

    print(
        f"Wrote {len(output_rows)} cities to {output_csv} (grouped by '{city_column}' column)"
    )
    return len(output_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Create city-level aggregates.")
    parser.add_argument("--input", default=str(INPUT_CSV), help="Input location CSV")
    parser.add_argument("--output", default=str(OUTPUT_CSV), help="Output city CSV")
    args = parser.parse_args()
    create_city_data(args.input, args.output)


if __name__ == "__main__":
    main()
