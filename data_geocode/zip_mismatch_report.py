#!/usr/bin/env python3
"""Count ZIP-code agreement between process metadata and geocode display names."""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

import usaddress

COUNTRY_NAMES = {"usa", "u.s.a", "united states", "united states of america"}


def normalize_zip(value: Any) -> str | None:
    if value is None:
        return None
    digits = re.sub(r"\D", "", str(value))
    if len(digits) >= 5:
        return digits[:5]
    if len(digits) == 4:
        return digits.zfill(5)
    return None


def parse_json_obj(text: str, stats: Counter[str], error_key: str) -> dict[str, Any]:
    if not text:
        return {}
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        stats[error_key] += 1
        return {}
    return parsed if isinstance(parsed, dict) else {}


def zip_from_usaddress(text: str) -> str | None:
    try:
        tagged, _ = usaddress.tag(text)
    except usaddress.RepeatedLabelError:
        tagged = {}
    except Exception:
        tagged = {}

    zip_code = normalize_zip(tagged.get("ZipCode"))
    if zip_code:
        return zip_code

    try:
        parsed = usaddress.parse(text)
    except Exception:
        return None

    for token, label in parsed:
        if label == "ZipCode":
            zip_code = normalize_zip(token)
            if zip_code:
                return zip_code
    return None


def simplify_display_name(display_name: str) -> str:
    parts = [part.strip() for part in display_name.split(",") if part.strip()]
    cleaned_parts: list[str] = []
    for part in parts:
        lowered = part.lower()
        if lowered in COUNTRY_NAMES:
            continue
        if lowered.endswith(" county"):
            continue
        cleaned_parts.append(part)
    return " ".join(cleaned_parts)


def extract_result_zip(row: dict[str, str], result_meta: dict[str, Any]) -> str | None:
    display_name = str(result_meta.get("display_name") or "").strip()
    if not display_name:
        display_name = str(row.get("address_nominatim") or "").strip()
    if not display_name:
        return None

    for candidate in (display_name, simplify_display_name(display_name)):
        if not candidate:
            continue
        zip_code = zip_from_usaddress(candidate)
        if zip_code:
            return zip_code
    return None


def extract_raw_zip(process_meta: dict[str, Any]) -> str | None:
    tags = process_meta.get("address_tags_expanded")
    if tags is None:
        tags = process_meta.get("address_tag_expanded")

    if isinstance(tags, str):
        try:
            parsed = json.loads(tags)
        except json.JSONDecodeError:
            parsed = {}
        tags = parsed if isinstance(parsed, dict) else {}

    if not isinstance(tags, dict):
        return None
    return normalize_zip(tags.get("ZipCode"))


def default_cache_path() -> Path:
    for candidate in (
        Path("data_geocode/geocode_address_cache.csv"),
        Path("clean_data/geocode_address_cache.csv"),
        Path("geocode_address_cache.csv"),
    ):
        if candidate.exists():
            return candidate
    return Path("data_geocode/geocode_address_cache.csv")


def default_report_path(cache_path: Path) -> Path:
    return cache_path.parent / "zip_mismatch_report.txt"


def build_report_lines(cache_path: Path, stats: Counter[str]) -> list[str]:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return [
        "ZIP Mismatch Report",
        f"generated_at: {ts}",
        f"cache_file: {cache_path}",
        "",
        f"rows_total: {stats['rows_total']}",
        f"zip_match: {stats['zip_match']}",
        f"zip_mismatch: {stats['zip_mismatch']}",
        f"missing_raw_zip: {stats['missing_raw_zip']}",
        f"missing_result_zip: {stats['missing_result_zip']}",
        f"missing_both_zip: {stats['missing_both_zip']}",
        f"bad_result_metadata_json: {stats['bad_result_metadata_json']}",
        f"bad_process_metadata_json: {stats['bad_process_metadata_json']}",
    ]


def generate_zip_mismatch_report(
    cache_path: Path,
    report_path: Path | None = None,
) -> tuple[Path, Counter[str]]:
    if not cache_path.exists():
        raise FileNotFoundError(f"Cache file not found: {cache_path}")

    stats: Counter[str] = Counter()

    with cache_path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            stats["rows_total"] += 1

            result_meta = parse_json_obj(
                row.get("result_metadata", ""),
                stats,
                "bad_result_metadata_json",
            )
            process_meta = parse_json_obj(
                row.get("process_metadata", ""),
                stats,
                "bad_process_metadata_json",
            )

            raw_zip = extract_raw_zip(process_meta)
            result_zip = extract_result_zip(row, result_meta)

            if raw_zip and result_zip:
                if raw_zip == result_zip:
                    stats["zip_match"] += 1
                else:
                    stats["zip_mismatch"] += 1
            elif raw_zip and not result_zip:
                stats["missing_result_zip"] += 1
            elif result_zip and not raw_zip:
                stats["missing_raw_zip"] += 1
            else:
                stats["missing_both_zip"] += 1

    final_report_path = report_path or default_report_path(cache_path)
    report_lines = build_report_lines(cache_path, stats)
    final_report_path.write_text("\n".join(report_lines) + "\n", encoding="utf-8")
    return final_report_path, stats


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Compare ZIP from process metadata (address_tags_expanded.ZipCode) "
            "against ZIP parsed by usaddress from the geocode display name."
        )
    )
    parser.add_argument(
        "--cache",
        type=Path,
        default=default_cache_path(),
        help="Path to geocode_address_cache.csv",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=None,
        help="Path to output txt report (default: <cache_dir>/zip_mismatch_report.txt)",
    )
    args = parser.parse_args()
    report_path, stats = generate_zip_mismatch_report(args.cache, args.report)
    report_lines = build_report_lines(args.cache, stats)
    print(f"report_file: {report_path}")
    for line in report_lines[2:]:
        if line:
            print(line)


if __name__ == "__main__":
    main()
