import json
import os
import sys
from pathlib import Path
from typing import Any, Dict

# ---- Edit this ----
RAW_ADDRESS ="49 Rosegarden Street, Warwick, RI 02888" #"Dr. Day Care, 133 Delaine St, Providence, RI 02909, USA" # "1 Beacon Center, Warwick, RI 02886, USA"
# -------------------

BASE_URL = "http://localhost:8080/search"
QUERY_PARTS_ORDER = ["house_number", "road", "postcode"]

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


def _find_data_geocode_dir(start: str) -> str:
    current = Path(start).resolve()
    for candidate in [current, *current.parents]:
        if (candidate / "nominatim_search.py").exists():
            return str(candidate)
    raise RuntimeError(f"Could not locate nominatim_search.py from {start}")


DATA_GEOCODE_DIR = _find_data_geocode_dir(ROOT_DIR)
if DATA_GEOCODE_DIR not in sys.path:
    sys.path.append(DATA_GEOCODE_DIR)

from nominatim_search import NominatimSearch


def run_search(
    raw_address: str,
    timeout: int | None = None,
    db_statement_timeout_ms: int | None = None,
    db_connect_timeout: int | None = None,
    use_address_cache: bool = False,
    save_address_cache: bool = False,
) -> NominatimSearch:
    kwargs: dict[str, int | str | bool] = {"base_url": BASE_URL}
    if timeout is not None:
        kwargs["timeout"] = timeout
    if db_statement_timeout_ms is not None:
        kwargs["db_statement_timeout_ms"] = db_statement_timeout_ms
    if db_connect_timeout is not None:
        kwargs["db_connect_timeout"] = db_connect_timeout
    kwargs["use_address_cache"] = use_address_cache
    kwargs["save_address_cache"] = save_address_cache
    searcher = NominatimSearch(**kwargs)
    searcher.search(raw_address.strip())
    return searcher


def _print_search_details(search_metadata: Dict[str, Any]) -> None:
    search_details = search_metadata.get("search_details") or []
    if not search_details:
        print("Search details: none")
        return
    print("Search details:")
    for detail in search_details:
        search_name = detail.get("search_name", "")
        status = detail.get("result_status", "")
        attempted = detail.get("attempted", False)
        elapsed_ms = detail.get("elapsed_ms", 0)
        number_results = detail.get("number_results", 0)
        print(
            "  - "
            f"{search_name}: attempted={attempted}, status={status}, "
            f"results={number_results}, elapsed_ms={elapsed_ms}"
        )
        for result in detail.get("results") or []:
            print(
                "      "
                f"idx={result.get('result_index')}, accepted={result.get('accepted')}, "
                f"reason={result.get('rejection_reason')}, logic={result.get('rejection_logic')}"
            )


def main() -> None:
    searcher = run_search(RAW_ADDRESS)
    tag_metadata = searcher.tag_metadata or {}
    search_metadata = searcher.search_metadata or {}
    result_metadata = searcher.result_metadata or {}
    print("="*60)
    print("\nNominatim Test Advanced Script Ouput\n")

    if searcher.error:
        print(f"Error: {searcher.error}")
    if searcher.log:
        print("Log:")
        for line in searcher.log:
            print(f"  - {line}")
    if searcher.result_metadata:
        print("Result metadata:")
        for key, value in searcher.result_metadata.items():
            print(f"  {key}: {value}")
    if searcher.response is not None:
        print(json.dumps(searcher.response, indent=2))
    else:
        print("No response data.")
    _print_search_details(search_metadata)
    print("="*60)
    print("Tag metadata:")
    print(json.dumps(tag_metadata, indent=2, sort_keys=True))
    print("="*60)
    print("Search metadata:")
    print(json.dumps(search_metadata, indent=2, sort_keys=True))
    print("="*60)
    print("Process metadata (compat view):")
    print(json.dumps(searcher.process_metadata, indent=2, sort_keys=True))

    print("="*60)
    print("\nNominatim Test Advanced SUMMARY:")
    print(f"      Metadata is Above\n")
    print(f"Raw address: {RAW_ADDRESS!r}")
    print(f"Repaired address: {searcher.address_repaired!r}")
    print(f"Parsed components: {tag_metadata.get('address_tags_expanded', {})!r}")
    print(f"Constructed query: {searcher.query!r}")
    print(f"Method used: {searcher.method!r}")
    print(f"Method outputs: {searcher.method_outputs!r}")
    print(f"Search method accepted: {search_metadata.get('search_method_accepted')!r}")
    print(f"Final error: {search_metadata.get('final_error')!r}")
    print(f"Search successful: {search_metadata.get('search_successful')!r}")
    print(f"Result Class: {result_metadata.get('class')!r}")
    print(f"Result Type: {result_metadata.get('type')!r}")
    print(f"Result addresstype: {result_metadata.get('addresstype')!r}")
    print(f"Result Place Rank: {result_metadata.get('place_rank')!r}")
    print(f"Diplay Name: {result_metadata.get('display_name')!r}\n\n")
    
    


if __name__ == "__main__":
    main()
