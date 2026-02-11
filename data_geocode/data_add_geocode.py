"""
Geocode stop addresses from `agg_data.csv` and produce:
- `data_geocode.csv` (row-level geocode output),
- `addresses_not_found.csv` (rows without usable coordinates), and
- `geocode_report.txt` (summary).

Cache strategy:
- Load cache once at startup into memory.
- Pass in-memory cache to NominatimSearch for lookups.
- Disable class-level cache saves.
- Append one cache row per new geocode result from this script.
"""

import csv
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
from tqdm import tqdm

from nominatim_search import NominatimSearch
from zip_mismatch_report import generate_zip_mismatch_report

NOMINATIM_URL = "http://localhost:8080/search"
SCRIPT_DIR = os.path.dirname(__file__)
LATEST_DIR = os.path.join(SCRIPT_DIR, "latest")
CACHE_FILE = os.path.join(LATEST_DIR, "geocode_address_cache.csv")
AGG_FILE = os.path.join(SCRIPT_DIR, "agg_data.csv")
OUTPUT_FILE = os.path.join(LATEST_DIR, "data_geocode.csv")
REPORT_FILE = os.path.join(LATEST_DIR, "geocode_report.txt")
NOT_FOUND_FILE = os.path.join(LATEST_DIR, "addresses_not_found.csv")
ZIP_MISMATCH_REPORT_FILE = os.path.join(LATEST_DIR, "zip_mismatch_report.txt")
NUM_THREADS = 4
TQDM_MIN_INTERVAL = 10
HTTP_TIMEOUT_SECONDS = 20
DB_STATEMENT_TIMEOUT_MS = 30000
DB_CONNECT_TIMEOUT_SECONDS = 10

CACHE_FIELDS = [
    "address_raw",
    "address_geocode",
    "address_nominatim",
    "latitude",
    "longitude",
    "method",
    "error",
    "result_metadata",
    "tag_metadata",
    "search_metadata",
    "process_metadata",
]


def log(msg: str) -> None:
    tqdm.write(msg)


def _normalize_cache_key(value: str) -> str:
    if value is None:
        return ""
    normalized = str(value).strip().casefold()
    normalized = " ".join(normalized.split())
    return normalized.strip(" ,")


def _detect_address_column(df: pd.DataFrame) -> str:
    for col in df.columns:
        if "address" in col.lower():
            return col
    raise ValueError(
        "No address column found in agg_data.csv. "
        "Please ensure there is a column with 'address' in its name."
    )


def _load_cache_map(cache_path: str) -> dict[str, dict[str, str]]:
    lookup: dict[str, dict[str, str]] = {}
    if not os.path.exists(cache_path):
        return lookup
    with open(cache_path, newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            row_clean = {
                str(k): (v.strip() if isinstance(v, str) else "")
                for k, v in row.items()
                if k is not None
            }
            raw = row_clean.get("address_raw", "")
            if not raw:
                continue
            lookup[_normalize_cache_key(raw)] = row_clean
    return lookup


def _open_cache_append_writer(cache_path: str) -> tuple[object, csv.DictWriter]:
    cache_dir = os.path.dirname(cache_path)
    if cache_dir:
        os.makedirs(cache_dir, exist_ok=True)
    file_exists = os.path.exists(cache_path)
    file_has_data = file_exists and os.path.getsize(cache_path) > 0
    if file_has_data:
        with open(cache_path, newline="", encoding="utf-8") as read_handle:
            reader = csv.DictReader(read_handle)
            existing_fields = reader.fieldnames or []
            existing_rows = list(reader)
        if existing_fields != CACHE_FIELDS:
            with open(cache_path, "w", newline="", encoding="utf-8") as rewrite_handle:
                writer = csv.DictWriter(rewrite_handle, fieldnames=CACHE_FIELDS)
                writer.writeheader()
                for row in existing_rows:
                    writer.writerow({k: row.get(k, "") for k in CACHE_FIELDS})
    handle = open(cache_path, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(handle, fieldnames=CACHE_FIELDS)
    if not file_has_data:
        writer.writeheader()
    return handle, writer


def _build_cache_row(searcher: NominatimSearch) -> dict[str, str]:
    return {
        "address_raw": searcher.raw_address or "",
        "address_geocode": searcher.query or searcher.raw_address or "",
        "address_nominatim": searcher.nominatim_address or "",
        "latitude": searcher.latitude or "",
        "longitude": searcher.longitude or "",
        "method": searcher.method or "",
        "error": searcher.error or "",
        "result_metadata": json.dumps(searcher.result_metadata or {}, sort_keys=True),
        "tag_metadata": json.dumps(searcher.tag_metadata or {}, sort_keys=True),
        "search_metadata": json.dumps(searcher.search_metadata or {}, sort_keys=True),
        "process_metadata": json.dumps(searcher.process_metadata or {}, sort_keys=True),
    }


def _search_error(searcher: NominatimSearch) -> str:
    if searcher.search_metadata.get("final_error"):
        return str(searcher.search_metadata["final_error"])
    return searcher.error or ""


def _search_query(searcher: NominatimSearch) -> str:
    search_details = searcher.search_metadata.get("search_details") or []
    if search_details:
        last_detail = search_details[-1]
        return str(last_detail.get("query") or searcher.query or "")
    return searcher.query or ""


def _search_method(searcher: NominatimSearch) -> str:
    accepted_method = searcher.search_metadata.get("search_method_accepted")
    if accepted_method and accepted_method != "none":
        return str(accepted_method)
    return searcher.method or ""


def main() -> None:
    os.makedirs(LATEST_DIR, exist_ok=True)
    df = pd.read_csv(AGG_FILE, dtype=str).fillna("")
    address_col = _detect_address_column(df)

    cache_lookup = _load_cache_map(CACHE_FILE)
    cache_lock = threading.RLock()
    starting_cache_size = len(cache_lookup)

    total = len(df)
    found = 0
    not_found = []
    cache_appends = 0

    log(f"Processing {total} rows with {NUM_THREADS} threads...")
    log(f"Using address column: {address_col}")
    log(
        "Timeout config:"
        f" http={HTTP_TIMEOUT_SECONDS}s"
        f" db_statement={DB_STATEMENT_TIMEOUT_MS}ms"
        f" db_connect={DB_CONNECT_TIMEOUT_SECONDS}s"
    )
    log(f"Address cache path: {CACHE_FILE}")
    log(f"Loaded cache rows (deduped in-memory): {starting_cache_size}")

    output_columns = list(df.columns) + [
        "osm_id",
        "display_name",
        "latitude",
        "longitude",
    ]

    def process_row(
        idx: int, row: dict[str, str]
    ) -> tuple[int, dict[str, str], dict[str, str] | None, dict[str, str] | None]:
        raw_addr = (row.get(address_col) or "").strip()
        if not raw_addr:
            result_row = {
                **row,
                "osm_id": "",
                "display_name": "",
                "latitude": "",
                "longitude": "",
            }
            return idx, result_row, {
                "raw_address": "",
                "method": "",
                "query": "",
                "error": "Empty address",
            }, None

        searcher = NominatimSearch(
            base_url=NOMINATIM_URL,
            timeout=HTTP_TIMEOUT_SECONDS,
            db_statement_timeout_ms=DB_STATEMENT_TIMEOUT_MS,
            db_connect_timeout=DB_CONNECT_TIMEOUT_SECONDS,
            use_address_cache=True,
            save_address_cache=False,
            address_cache_path=CACHE_FILE,
            address_cache_data=cache_lookup,
            address_cache_lock=cache_lock,
        )
        searcher.search(raw_addr)

        osm_id = ""
        if isinstance(searcher.result_metadata, dict):
            osm_id = str(searcher.result_metadata.get("osm_id") or "")

        lat = searcher.latitude or ""
        lon = searcher.longitude or ""
        result_row = {
            **row,
            "osm_id": osm_id,
            "display_name": searcher.nominatim_address or "",
            "latitude": lat,
            "longitude": lon,
        }

        not_found_row = None
        if not (lat and lon):
            not_found_row = {
                "raw_address": raw_addr,
                "method": _search_method(searcher),
                "query": _search_query(searcher),
                "error": _search_error(searcher),
            }

        cache_row = None
        if not searcher.search_metadata.get("address_cache_used") and searcher.raw_address:
            cache_row = _build_cache_row(searcher)

        return idx, result_row, not_found_row, cache_row

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as output_handle, open(
        NOT_FOUND_FILE, "w", newline="", encoding="utf-8"
    ) as not_found_handle:
        output_writer = csv.DictWriter(output_handle, fieldnames=output_columns)
        output_writer.writeheader()

        not_found_writer = csv.DictWriter(
            not_found_handle,
            fieldnames=["raw_address", "method", "query", "error"],
        )
        not_found_writer.writeheader()

        cache_handle, cache_writer = _open_cache_append_writer(CACHE_FILE)
        try:
            with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
                futures = {
                    executor.submit(process_row, idx, row.to_dict()): idx
                    for idx, row in df.iterrows()
                }
                for f in tqdm(
                    as_completed(futures),
                    total=total,
                    mininterval=TQDM_MIN_INTERVAL,
                    maxinterval=TQDM_MIN_INTERVAL,
                ):
                    idx = futures[f]
                    try:
                        _, result_row, not_found_row, cache_row = f.result()
                    except Exception as exc:
                        row = df.iloc[idx].to_dict()
                        raw_addr = (row.get(address_col) or "").strip()
                        result_row = {
                            **row,
                            "osm_id": "",
                            "display_name": "",
                            "latitude": "",
                            "longitude": "",
                        }
                        not_found_row = {
                            "raw_address": raw_addr,
                            "method": "",
                            "query": "",
                            "error": f"exception: {exc}",
                        }
                        cache_row = None

                    output_writer.writerow({k: result_row.get(k, "") for k in output_columns})

                    if cache_row is not None:
                        cache_writer.writerow({k: cache_row.get(k, "") for k in CACHE_FIELDS})
                        with cache_lock:
                            cache_lookup[_normalize_cache_key(cache_row["address_raw"])] = cache_row
                        cache_appends += 1

                    if result_row.get("latitude") and result_row.get("longitude"):
                        found += 1
                    else:
                        if not_found_row is None:
                            not_found_row = {
                                "raw_address": (result_row.get(address_col) or "").strip(),
                                "method": "",
                                "query": "",
                                "error": "Missing latitude/longitude",
                            }
                        not_found_writer.writerow(not_found_row)
                        not_found.append(
                            {
                                "address": not_found_row.get("raw_address", ""),
                                "error": not_found_row.get("error", ""),
                            }
                        )
        finally:
            cache_handle.close()

    log(f"Done. Output written to {OUTPUT_FILE}")
    with open(REPORT_FILE, "w", encoding="utf-8") as handle:
        handle.write(f"Total addresses processed: {total}\n")
        handle.write(f"Addresses geocoded: {found}\n")
        handle.write(f"Addresses not geocoded: {len(not_found)}\n")
        handle.write(f"Cache rows appended this run: {cache_appends}\n\n")
        if not_found:
            handle.write("Addresses not found:\n")
            for nf in not_found:
                handle.write(f"  {nf['address']} | Error: {nf['error']}\n")
    log(f"Geocode report written to {REPORT_FILE}")

    zip_report_path, _ = generate_zip_mismatch_report(
        cache_path=Path(CACHE_FILE),
        report_path=Path(ZIP_MISMATCH_REPORT_FILE),
    )
    log(f"ZIP mismatch report written to {zip_report_path}")


if __name__ == "__main__":
    main()
