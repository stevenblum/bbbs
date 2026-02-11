import csv
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from tqdm import tqdm

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_GEOCODE_DIR = str(Path(ROOT_DIR).resolve().parents[0])
if DATA_GEOCODE_DIR not in sys.path:
    sys.path.append(DATA_GEOCODE_DIR)

from nominatim_test_advanced import run_search

ADDRESSES_FILE = os.path.join(DATA_GEOCODE_DIR, "latest", "addresses_not_found.csv")
OUTPUT_FILE = os.path.join(ROOT_DIR, "nominatim_test_list.csv")
NO_RESULTS_FILE = os.path.join(ROOT_DIR, "nominatim_test_list_noresults.csv")
NUM_THREADS = 16
HTTP_TIMEOUT_SECONDS = 20
DB_STATEMENT_TIMEOUT_MS = 60000
DB_CONNECT_TIMEOUT_SECONDS = 10

# db_connect_timeout:
# How long psycopg waits to open a Postgres connection (network + auth).
# If exceeded, DB connection fails before any SQL runs.

# db_statement_timeout_ms:
# This runs SET statement_timeout = ... in Postgres
# The server cancels any SQL statement that runs too long.
# It applies to statements on that connection in _postcode_candidates.

# timeout:
# This is the HTTP timeout for requests.get(...) to Nominatim’s /search endpoint.


CSV_FIELDS = [
    "raw_address",
    "nominatim_address",
    "method",
    "search_method_accepted",
    "method_outputs",
    "query",
    "latitude",
    "longitude",
    "error",
    "final_error",
    "status",
    "search_successful",
    "bad_address_lookup_used",
    "address_cache_used",
    "search_attempts_count",
    "elapsed_ms",
    "log",
    "tag_metadata",
    "search_metadata",
]


def load_addresses(path: str) -> list[str]:
    addresses: list[str] = []
    with open(path, newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            raw = (row.get("raw_address") or "").strip()
            if raw:
                addresses.append(raw)
    return addresses


def main() -> None:
    addresses = load_addresses(ADDRESSES_FILE)
    total = len(addresses)
    print(f"Loaded {total} addresses from {ADDRESSES_FILE}")
    print(
        "Timeout config:"
        f" http={HTTP_TIMEOUT_SECONDS}s"
        f" db_statement={DB_STATEMENT_TIMEOUT_MS}ms"
        f" db_connect={DB_CONNECT_TIMEOUT_SECONDS}s"
    )
    if not addresses:
        return

    found = 0

    def process(addr: str) -> dict[str, str]:
        started = time.perf_counter()
        try:
            searcher = run_search(
                addr,
                timeout=HTTP_TIMEOUT_SECONDS,
                db_statement_timeout_ms=DB_STATEMENT_TIMEOUT_MS,
                db_connect_timeout=DB_CONNECT_TIMEOUT_SECONDS,
                use_address_cache=False,
                save_address_cache=False,
            )
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            tag_metadata = searcher.tag_metadata or {}
            search_metadata = searcher.search_metadata or {}
            status = "ok" if searcher.latitude and searcher.longitude else "not_found"
            return {
                "raw_address": addr,
                "nominatim_address": searcher.nominatim_address or "",
                "method": searcher.method or "",
                "search_method_accepted": search_metadata.get("search_method_accepted") or "",
                "method_outputs": searcher.method_outputs or "",
                "query": searcher.query or "",
                "latitude": searcher.latitude or "",
                "longitude": searcher.longitude or "",
                "error": searcher.error or "",
                "final_error": search_metadata.get("final_error") or "",
                "status": status or "",
                "search_successful": str(bool(search_metadata.get("search_successful"))),
                "bad_address_lookup_used": str(bool(search_metadata.get("bad_address_lookup_used"))),
                "address_cache_used": str(bool(search_metadata.get("address_cache_used"))),
                "search_attempts_count": str(
                    len(search_metadata.get("search_attempts") or [])
                ),
                "elapsed_ms": str(elapsed_ms),
                "log": ";".join(searcher.log).replace('"', "'") if searcher.log else "",
                "tag_metadata": json.dumps(tag_metadata, sort_keys=True),
                "search_metadata": json.dumps(search_metadata, sort_keys=True),
            }
        except Exception as exc:
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            return {
                "raw_address": addr,
                "nominatim_address": "",
                "method": "",
                "search_method_accepted": "",
                "method_outputs": "",
                "query": "",
                "latitude": "",
                "longitude": "",
                "error": f"exception: {exc}",
                "final_error": f"exception: {exc}",
                "status": "error",
                "search_successful": "False",
                "bad_address_lookup_used": "False",
                "address_cache_used": "False",
                "search_attempts_count": "0",
                "elapsed_ms": str(elapsed_ms),
                "log": "",
                "tag_metadata": "{}",
                "search_metadata": "{}",
            }

    with open(OUTPUT_FILE, "w", newline="") as handle, open(NO_RESULTS_FILE, "w", newline="") as handle_no:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer_no = csv.DictWriter(handle_no, fieldnames=CSV_FIELDS)
        writer_no.writeheader()

        with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
            future_map = {executor.submit(process, addr): addr for addr in addresses}
            for f in tqdm(as_completed(future_map), total=total):
                try:
                    row = f.result()
                except Exception as exc:
                    addr = future_map[f]
                    row = {
                        "raw_address": addr,
                        "nominatim_address": "",
                        "method": "",
                        "search_method_accepted": "",
                        "method_outputs": "",
                        "query": "",
                        "latitude": "",
                        "longitude": "",
                        "error": f"exception: {exc}",
                        "final_error": f"exception: {exc}",
                        "status": "error",
                        "search_successful": "False",
                        "bad_address_lookup_used": "False",
                        "address_cache_used": "False",
                        "search_attempts_count": "0",
                        "elapsed_ms": "",
                        "log": "",
                        "tag_metadata": "{}",
                        "search_metadata": "{}",
                    }
                writer.writerow(row)
                handle.flush()
                if row["status"] != "ok":
                    writer_no.writerow(row)
                    handle_no.flush()
                if row["latitude"] and row["longitude"]:
                    found += 1

    print(f"Found {found} / {total} addresses")


if __name__ == "__main__":
    main()
