import os
import pandas as pd
import requests

from tqdm import tqdm
from postal.parser import parse_address
from concurrent.futures import ThreadPoolExecutor, as_completed

NOMINATIM_URL = "http://localhost:8080/search"
CACHE_FILE = os.path.join(os.path.dirname(__file__), "geocode_address_cache.csv")
AGG_FILE = os.path.join(os.path.dirname(__file__), "agg_data.csv")
OUTPUT_FILE = os.path.join(os.path.dirname(__file__), "data_geocode.csv")
REPORT_FILE = os.path.join(os.path.dirname(__file__), "geocode_report.txt")
NUM_THREADS = 32  # Adjust this value as needed


# Load or initialize cache
def load_cache():
    if os.path.exists(CACHE_FILE):
        return pd.read_csv(CACHE_FILE, dtype=str).fillna("")
    else:
        return pd.DataFrame(columns=["address_raw", "address_geocode", "address_nominatim", "latitude", "longitude", "method", "error"])

def save_cache(cache):
    cache.drop_duplicates(subset=["address_raw"], inplace=True)
    cache.to_csv(CACHE_FILE, index=False)

def geocode_address(address):
    params = {"q": address, "format": "json", "addressdetails": 0, "limit": 1}
    try:
        r = requests.get(NOMINATIM_URL, params=params, timeout=5)
        r.raise_for_status()
        data = r.json()
        if data:
            display_name = data[0].get("display_name", "")
            return data[0]["lat"], data[0]["lon"], address, display_name, "raw", ""
        else:
            return None, None, None, None, "raw", "No result"
    except Exception as e:
        return None, None, None, None, "raw", str(e)

def build_search_address(raw_addr):
    try:
        parsed = parse_address(raw_addr)
    except Exception:
        parsed = []

    parts = {}
    for value, label in parsed:
        parts.setdefault(label, []).append(value)

    house_number = " ".join(parts.get("house_number", [])).strip()
    road = " ".join(parts.get("road", [])).strip()
    city = " ".join(parts.get("city", [])).strip()

    structured = ", ".join([p for p in [house_number, road, city] if p])
    return structured or raw_addr

def main():
    df = pd.read_csv(AGG_FILE, dtype=str).fillna("")
    print("DEBUG: agg_data.csv columns:", list(df.columns))
    if not df.empty:
        print("DEBUG: First row sample:", df.iloc[0].to_dict())

    # Try to find the address column automatically
    address_col = None
    for col in df.columns:
        if 'address' in col.lower():
            address_col = col
            break
    if address_col is None:
        raise ValueError("No address column found in agg_data.csv. Please ensure there is a column with 'address' in its name.")
    print(f"DEBUG: Using address column: {address_col}")
    cache = load_cache()
    cache_dict = {row["address_raw"]: row for _, row in cache.iterrows()}
    results = [None] * len(df)
    total = len(df)
    parsed_success = 0
    not_found = []
    print(f"Processing {total} rows with {NUM_THREADS} threads...")
    debug_limit = 10

    def process_row(idx_row):
        idx, row = idx_row
        raw_addr = row.get(address_col, "")
        raw_addr = raw_addr.strip()
        search_addr = build_search_address(raw_addr) if raw_addr else ""
        if idx < debug_limit:
            print(f"DEBUG: Row {idx+1} address after strip: '{raw_addr}'")
        if not raw_addr:
            return idx, {**row, "address_geocode": "", "address_nominatim": "", "latitude": "", "longitude": "", "method": "", "error": ""}
        if raw_addr in cache_dict:
            cached = cache_dict[raw_addr]
            lat, lon, addr_used = cached["latitude"], cached["longitude"], cached["address_geocode"]
            address_nominatim = cached.get("address_nominatim", "")
            method = cached.get("method", "")
            error = cached.get("error", "")
            if idx < debug_limit:
                print(
                    f"DEBUG: Raw address (row {idx+1}): {raw_addr} | "
                    f"Search address: {search_addr} | "
                    f"Nominatim address: {address_nominatim}"
                )
        else:
            lat, lon, addr_used, address_nominatim, method, error = geocode_address(search_addr)
            if idx < debug_limit:
                print(
                    f"DEBUG: Raw address (row {idx+1}): {raw_addr} | "
                    f"Search address: {search_addr} | "
                    f"Nominatim address: {address_nominatim}"
                )
                print(f"DEBUG: Nominatim response: lat={lat}, lon={lon}, used_addr={addr_used}, error={error}")
            cache_dict[raw_addr] = {
                "address_geocode": addr_used or search_addr,
                "address_nominatim": address_nominatim or "",
                "latitude": lat or "",
                "longitude": lon or "",
                "method": "parsed",
                "error": error,
            }
            method = "parsed"
            # Don't update cache DataFrame here, do it after all threads complete
        return idx, {**row, "address_geocode": addr_used or search_addr, "address_nominatim": address_nominatim or "", "latitude": lat or "", "longitude": lon or "", "method": method, "error": error}

    with ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
        futures = [executor.submit(process_row, (idx, row)) for idx, row in df.iterrows()]
        for f in tqdm(as_completed(futures), total=total):
            idx, result = f.result()
            results[idx] = result

    # After all threads, update cache DataFrame and stats
    cache = pd.DataFrame([
        {"address_raw": k, **v} for k, v in cache_dict.items()
    ])
    save_cache(cache)

    # Count stats
    for r in results:
        if r["method"] == "parsed":
            parsed_success += 1
        if not r["latitude"] or not r["longitude"]:
            not_found.append({"address": r.get(address_col, ""), "error": r.get("error", "")})

    pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)
    print(f"Done. Output written to {OUTPUT_FILE}")
    # Write report
    with open(REPORT_FILE, "w") as f:
        f.write(f"Total addresses processed: {total}\n")
        f.write(f"Parsed addresses geocoded: {parsed_success}\n")
        f.write(f"Addresses not geocoded: {len(not_found)}\n\n")
        if not_found:
            f.write("Addresses not found:\n")
            for nf in not_found:
                f.write(f"  {nf['address']} | Error: {nf['error']}\n")
    print(f"Geocode report written to {REPORT_FILE}")

if __name__ == "__main__":
    main()
