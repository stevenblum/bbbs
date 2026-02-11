#!/usr/bin/env python3
"""Create route-level summary metrics from stop-level CSV data."""

import csv
import math
from collections import defaultdict
import argparse
from pathlib import Path
from typing import Iterable, List, Optional, Tuple, Dict, Union

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
INPUT_CSV = PROJECT_ROOT / "data_geocode" / "latest" / "data_geocode.csv"
OUTPUT_CSV = SCRIPT_DIR / "data_route.csv"


def to_float(value: str) -> Optional[float]:
    if value is None:
        return None
    value = str(value).strip()
    if value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def to_int(value: str) -> Optional[int]:
    number = to_float(value)
    if number is None:
        return None
    try:
        return int(number)
    except (TypeError, ValueError):
        return None


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return the great-circle distance in miles between two points."""
    # Earth radius in miles
    radius = 3958.7613
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return radius * c


def route_distance_straight_line(latlons: Iterable[Tuple[float, float]]) -> float:
    points = list(latlons)
    if len(points) < 2:
        return 0.0
    total = 0.0
    for i in range(1, len(points)):
        lat1, lon1 = points[i - 1]
        lat2, lon2 = points[i]
        total += haversine_miles(lat1, lon1, lat2, lon2)
    return total


def count_edges_executed_as_planned(
    planned_sequence: List[int],
    actual_sequence: List[int],
) -> int:
    """
    Count how many edges (predecessor relationships) match between planned and actual.
    For each node, compare its predecessor in the planned sequence vs the actual sequence.
    """
    planned_prev: Dict[int, Optional[int]] = {}
    actual_prev: Dict[int, Optional[int]] = {}

    prev = None
    for node in planned_sequence:
        planned_prev[node] = prev
        prev = node

    prev = None
    for node in actual_sequence:
        actual_prev[node] = prev
        prev = node

    count = 0
    for node, planned_predecessor in planned_prev.items():
        if node in actual_prev and actual_prev[node] == planned_predecessor:
            count += 1
    return count


def create_route_data(
    input_csv: Union[str, Path], output_csv: Union[str, Path] = OUTPUT_CSV
) -> int:
    with open(input_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    routes: Dict[Tuple[str, str], List[dict]] = defaultdict(list)

    for row in rows:
        driver = (row.get("Driver") or "").strip()
        planned_date = (row.get("Planned Date") or "").strip()
        actual_date = (row.get("Actual Date") or "").strip()
        route_date = planned_date or actual_date
        route_key = (driver, route_date)
        routes[route_key].append(row)

    output_fields = [
        "Driver",
        "Route Date",
        "number_of_stops",
        "first_stop_planned_time",
        "last_stop_planned_time",
        "first_stop_actual_time",
        "last_stop_actual_time",
        "total_planned_stop_duration",
        "total_actual_stop_duration",
        "route_distance_straight_line_planned_miles",
        "route_distance_straight_line_actual_miles",
        "edges_executed_as_planned",
        "edges_executed_as_planned_pct",
    ]

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=output_fields)
        writer.writeheader()

        for (driver, route_date), route_rows in sorted(routes.items()):
            number_of_stops = len(route_rows)

            planned_rows = [
                row
                for row in route_rows
                if to_int(row.get("Planned Stop Number", "")) is not None
            ]
            actual_rows = [
                row
                for row in route_rows
                if to_int(row.get("Actual Stop Number", "")) is not None
            ]

            planned_rows.sort(key=lambda r: to_int(r.get("Planned Stop Number")) or 0)
            actual_rows.sort(key=lambda r: to_int(r.get("Actual Stop Number")) or 0)

            first_stop_planned_time = (
                planned_rows[0].get("Planned Time") if planned_rows else ""
            )
            last_stop_planned_time = (
                planned_rows[-1].get("Planned Time") if planned_rows else ""
            )
            first_stop_actual_time = (
                actual_rows[0].get("Actual Time") if actual_rows else ""
            )
            last_stop_actual_time = (
                actual_rows[-1].get("Actual Time") if actual_rows else ""
            )

            total_planned_stop_duration = sum(
                to_float(row.get("Planned Duration")) or 0.0 for row in route_rows
            )
            total_actual_stop_duration = sum(
                to_float(row.get("Actual Duration")) or 0.0 for row in route_rows
            )

            planned_latlons = []
            for row in planned_rows:
                lat = to_float(row.get("latitude"))
                lon = to_float(row.get("longitude"))
                if lat is None or lon is None:
                    continue
                planned_latlons.append((lat, lon))

            actual_latlons = []
            for row in actual_rows:
                lat = to_float(row.get("latitude"))
                lon = to_float(row.get("longitude"))
                if lat is None or lon is None:
                    continue
                actual_latlons.append((lat, lon))

            route_distance_planned = route_distance_straight_line(planned_latlons)
            route_distance_actual = route_distance_straight_line(actual_latlons)

            planned_sequence = [
                to_int(row.get("Planned Stop Number"))
                for row in planned_rows
                if to_int(row.get("Planned Stop Number")) is not None
            ]
            actual_sequence = [
                to_int(row.get("Planned Stop Number"))
                for row in actual_rows
                if to_int(row.get("Planned Stop Number")) is not None
            ]

            edges_executed_as_planned = count_edges_executed_as_planned(
                planned_sequence, actual_sequence
            )
            if number_of_stops > 1:
                edges_executed_as_planned_pct = (
                    edges_executed_as_planned / (number_of_stops - 1)
                ) * 100.0
            else:
                edges_executed_as_planned_pct = 0.0

            writer.writerow(
                {
                    "Driver": driver,
                    "Route Date": route_date,
                    "number_of_stops": number_of_stops,
                    "first_stop_planned_time": first_stop_planned_time,
                    "last_stop_planned_time": last_stop_planned_time,
                    "first_stop_actual_time": first_stop_actual_time,
                    "last_stop_actual_time": last_stop_actual_time,
                    "total_planned_stop_duration": f"{total_planned_stop_duration:.2f}",
                    "total_actual_stop_duration": f"{total_actual_stop_duration:.2f}",
                    "route_distance_straight_line_planned_miles": f"{route_distance_planned:.4f}",
                    "route_distance_straight_line_actual_miles": f"{route_distance_actual:.4f}",
                    "edges_executed_as_planned": edges_executed_as_planned,
                    "edges_executed_as_planned_pct": f"{edges_executed_as_planned_pct:.2f}",
                }
            )
    return len(routes)


def main() -> None:
    parser = argparse.ArgumentParser(description="Create route-level summary metrics.")
    parser.add_argument("--input", default=str(INPUT_CSV), help="Input stop-level CSV")
    parser.add_argument("--output", default=str(OUTPUT_CSV), help="Output route CSV")
    args = parser.parse_args()
    create_route_data(args.input, args.output)


if __name__ == "__main__":
    main()
