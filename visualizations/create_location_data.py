#!/usr/bin/env python3
"""Build location-level aggregates from stop-level data."""

import csv
import argparse
import json
import re
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Union

try:
    from postal.parser import parse_address
except Exception:  # pragma: no cover - graceful fallback if libpostal is unavailable
    parse_address = None

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
INPUT_CSV = PROJECT_ROOT / "data_geocode" / "latest" / "data_geocode.csv"
OUTPUT_CSV = SCRIPT_DIR / "data_locations.csv"


def normalize(value: Optional[str]) -> str:
    if value is None:
        return ""
    return str(value).strip()


def normalize_key(value: Optional[str]) -> str:
    return normalize(value).lower()


def to_float(value: Optional[str]) -> Optional[float]:
    raw = normalize(value)
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def parse_nominatim_components(address_nominatim: str) -> Dict[str, str]:
    components = {
        "street_number": "",
        "street_name": "",
        "city": "",
        "state": "",
        "zip_code": "",
    }
    if not address_nominatim:
        return components

    if parse_address is not None:
        parsed = parse_address(address_nominatim)
        buckets: Dict[str, List[str]] = defaultdict(list)
        for value, label in parsed:
            buckets[label].append(value.strip())

        components["street_number"] = " ".join(buckets.get("house_number", [])).strip()
        components["street_name"] = " ".join(
            buckets.get("road", []) + buckets.get("pedestrian", [])
        ).strip()
        components["city"] = " ".join(
            buckets.get("city", [])
            + buckets.get("suburb", [])
            + buckets.get("city_district", [])
        ).strip()
        components["state"] = " ".join(
            buckets.get("state", []) + buckets.get("state_code", [])
        ).strip()
        components["zip_code"] = " ".join(
            buckets.get("postcode", []) + buckets.get("postal_code", [])
        ).strip()

    street_suffixes = {
        "street", "st", "avenue", "ave", "road", "rd", "drive", "dr", "lane", "ln",
        "boulevard", "blvd", "court", "ct", "place", "pl", "way", "terrace", "ter",
        "circle", "cir", "parkway", "pkwy", "highway", "hwy", "route", "rt",
        "trail", "trl",
    }
    state_abbrev = {
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL",
        "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT",
        "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI",
        "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC",
    }

    def looks_like_street(text: str) -> bool:
        cleaned = re.sub(r"[^A-Za-z0-9 ]+", " ", text).strip().lower()
        if not cleaned:
            return False
        tokens = [t for t in cleaned.split() if t]
        if not tokens:
            return False
        if any(tok.isdigit() for tok in tokens) and len(tokens) <= 4:
            return True
        return tokens[-1] in street_suffixes

    def normalize_city_value(text: str) -> str:
        cleaned = re.sub(r"\b\d{5}(?:-\d{4})?\b", " ", text)
        cleaned = re.sub(r"\s+", " ", cleaned).strip(" ,.-")
        if not cleaned:
            return ""

        tokens = cleaned.split()
        if tokens and tokens[-1].upper().strip(".") in state_abbrev:
            tokens = tokens[:-1]
        if not tokens:
            return ""

        lowered = " ".join(tokens).lower()
        for state_name in sorted(states, key=len, reverse=True):
            if lowered.endswith(" " + state_name):
                cut = len(tokens) - len(state_name.split())
                tokens = tokens[:cut]
                break
            if lowered == state_name:
                tokens = []
                break
        if not tokens:
            return ""

        for idx, token in enumerate(tokens):
            if token.lower().strip(".") in street_suffixes and idx + 1 < len(tokens):
                tail = tokens[idx + 1 :]
                if tail:
                    return " ".join(tail).strip(" ,.-")

        return " ".join(tokens).strip(" ,.-")

    states = {
        "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
        "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
        "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana",
        "maine", "maryland", "massachusetts", "michigan", "minnesota",
        "mississippi", "missouri", "montana", "nebraska", "nevada",
        "new hampshire", "new jersey", "new mexico", "new york",
        "north carolina", "north dakota", "ohio", "oklahoma", "oregon",
        "pennsylvania", "rhode island", "south carolina", "south dakota",
        "tennessee", "texas", "utah", "vermont", "virginia", "washington",
        "west virginia", "wisconsin", "wyoming", "dc",
    }

    parts = [p.strip() for p in address_nominatim.split(",") if p.strip()]
    if parts and parts[-1].lower() in {"united states", "usa", "us"}:
        parts = parts[:-1]

    zip_pattern = re.compile(r"\b\d{5}(?:-\d{4})?\b")
    for p in parts:
        m = zip_pattern.search(p)
        if m:
            components["zip_code"] = m.group(0)
            break

    for p in parts:
        candidate = p.lower()
        if candidate in states:
            components["state"] = p
            break

    house_idx = None
    for idx, p in enumerate(parts):
        if re.fullmatch(r"\d+[A-Za-z]?", p):
            components["street_number"] = p
            house_idx = idx
            if idx + 1 < len(parts):
                components["street_name"] = parts[idx + 1]
            break
        m = re.match(r"^(\d+[A-Za-z]?)\s+(.+)$", p)
        if m:
            components["street_number"] = m.group(1)
            components["street_name"] = m.group(2).strip()
            house_idx = idx
            break

    if components["street_name"] and not looks_like_street(components["street_name"]):
        components["street_name"] = ""

    start_city = (house_idx + 2) if (house_idx is not None and components["street_name"]) else 0
    city_candidates: List[str] = []
    for p in parts[start_city:]:
        p_lower = p.lower()
        if "county" in p_lower:
            continue
        if "parish" in p_lower:
            continue
        if components["state"] and p_lower == components["state"].lower():
            continue
        if components["zip_code"] and components["zip_code"] in p:
            continue
        if re.fullmatch(r"\d+", p):
            continue
        city_candidates.append(p)

    if not components["city"] and city_candidates:
        # Prefer the right-most non-street token (closest to state/county in Nominatim display format).
        for candidate in reversed(city_candidates):
            if looks_like_street(candidate):
                continue
            components["city"] = candidate
            break
        if not components["city"]:
            components["city"] = city_candidates[-1]

    if components["city"]:
        city_clean = normalize_city_value(components["city"])
        if looks_like_street(city_clean) or re.search(r"\d{3,}", city_clean):
            components["city"] = ""
        else:
            components["city"] = city_clean

    return components


def build_location_key(row: Dict[str, str], row_index: int) -> Tuple[str, str]:
    nominatim = normalize(row.get("address_nominatim"))
    if nominatim:
        return normalize_key(nominatim), nominatim

    # Keep unmatched/blank Nominatim stops distinct by raw address to avoid collapsing them all.
    raw_address = normalize(row.get("Address"))
    if raw_address:
        return f"__raw__::{normalize_key(raw_address)}", raw_address

    return f"__row__::{row_index}", ""


def create_location_data(
    input_csv: Union[str, Path], output_csv: Union[str, Path] = OUTPUT_CSV
) -> int:
    with open(input_csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    grouped: Dict[str, Dict[str, object]] = {}

    for idx, row in enumerate(rows):
        location_key, display_address = build_location_key(row, idx)

        if location_key not in grouped:
            grouped[location_key] = {
                "address_nominatim": normalize(row.get("address_nominatim")) or display_address,
                "raw_addresses": set(),
                "total_number_of_stops": 0,
                "total_planned_stop_duration": 0.0,
                "total_actual_stop_duration": 0.0,
                "planned_count": 0,
                "actual_count": 0,
                "delay_sum": 0.0,
                "delay_count": 0,
                "lat_sum": 0.0,
                "lon_sum": 0.0,
                "latlon_count": 0,
            }

        bucket = grouped[location_key]
        bucket["total_number_of_stops"] += 1

        raw_address = normalize(row.get("Address"))
        if raw_address:
            bucket["raw_addresses"].add(raw_address)

        planned = to_float(row.get("Planned Duration"))
        actual = to_float(row.get("Actual Duration"))

        if planned is not None:
            bucket["total_planned_stop_duration"] += planned
            bucket["planned_count"] += 1

        if actual is not None:
            bucket["total_actual_stop_duration"] += actual
            bucket["actual_count"] += 1

        if planned is not None and actual is not None:
            bucket["delay_sum"] += (actual - planned)
            bucket["delay_count"] += 1

        lat = to_float(row.get("latitude"))
        lon = to_float(row.get("longitude"))
        if lat is not None and lon is not None:
            bucket["lat_sum"] += lat
            bucket["lon_sum"] += lon
            bucket["latlon_count"] += 1

    output_rows = []

    for i, key in enumerate(sorted(grouped.keys()), start=1):
        bucket = grouped[key]
        total_stops = int(bucket["total_number_of_stops"])
        planned_total = float(bucket["total_planned_stop_duration"])
        actual_total = float(bucket["total_actual_stop_duration"])
        planned_count = int(bucket["planned_count"])
        actual_count = int(bucket["actual_count"])
        delay_count = int(bucket["delay_count"])

        avg_planned = planned_total / planned_count if planned_count else 0.0
        avg_actual = actual_total / actual_count if actual_count else 0.0
        avg_delay = float(bucket["delay_sum"]) / delay_count if delay_count else 0.0
        latlon_count = int(bucket["latlon_count"])
        latitude = f"{(float(bucket['lat_sum']) / latlon_count):.7f}" if latlon_count else ""
        longitude = f"{(float(bucket['lon_sum']) / latlon_count):.7f}" if latlon_count else ""

        address_nominatim = str(bucket["address_nominatim"])
        components = parse_nominatim_components(address_nominatim)
        raw_addresses = sorted(bucket["raw_addresses"])

        output_rows.append(
            {
                "location_id": f"LOC{i:07d}",
                "unique_stop_id": f"LOC{i:07d}",
                "address_nominatim": address_nominatim,
                "street_number": components["street_number"],
                "street_name": components["street_name"],
                "city": components["city"],
                "state": components["state"],
                "zip_code": components["zip_code"],
                "raw_address_variants_count": len(raw_addresses),
                "address_raw_list": json.dumps(raw_addresses),
                "raw_address_variants": " | ".join(raw_addresses),
                "total_number_of_stops": total_stops,
                "total_planned_stop_duration": f"{planned_total:.2f}",
                "average_planned_stop_duration": f"{avg_planned:.2f}",
                "total_actual_stop_duration": f"{actual_total:.2f}",
                "average_actual_stop_duration": f"{avg_actual:.2f}",
                "average_actual_top_duration": f"{avg_actual:.2f}",
                "average_stop_delay": f"{avg_delay:.2f}",
                "latitude": latitude,
                "longitude": longitude,
            }
        )

    fieldnames = [
        "location_id",
        "unique_stop_id",
        "address_nominatim",
        "street_number",
        "street_name",
        "city",
        "state",
        "zip_code",
        "raw_address_variants_count",
        "address_raw_list",
        "raw_address_variants",
        "total_number_of_stops",
        "total_planned_stop_duration",
        "average_planned_stop_duration",
        "total_actual_stop_duration",
        "average_actual_stop_duration",
        "average_actual_top_duration",
        "average_stop_delay",
        "latitude",
        "longitude",
    ]

    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)

    print(f"Wrote {len(output_rows)} unique locations to {output_csv}")
    return len(output_rows)


def main() -> None:
    parser = argparse.ArgumentParser(description="Create location-level aggregates.")
    parser.add_argument("--input", default=str(INPUT_CSV), help="Input stop-level CSV")
    parser.add_argument("--output", default=str(OUTPUT_CSV), help="Output location CSV")
    args = parser.parse_args()
    create_location_data(args.input, args.output)


if __name__ == "__main__":
    main()
