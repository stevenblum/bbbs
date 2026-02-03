import os
import pandas as pd
import requests

import time
from tqdm import tqdm
from postal.expand import expand_address
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

def try_normalize_and_geocode(address):
    try:
        expansions = expand_address(address)
        for exp in expansions:
            print(f"DEBUG: Trying normalized address: {exp}")
            lat, lon, used_addr, display_name, _, _ = geocode_address(exp)
            if lat and lon:
                print(f"DEBUG: Nominatim display_name for normalized: {display_name}")
                return lat, lon, exp, display_name, "normalized", ""
        return None, None, None, None, "normalized", "No result"
    except Exception as e:
        return None, None, None, None, "normalized", str(e)

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
    raw_success = 0
    norm_success = 0
    not_found = []
    print(f"Processing {total} rows with {NUM_THREADS} threads...")
    debug_limit = 10

    def process_row(idx_row):
        idx, row = idx_row
        raw_addr = row.get(address_col, "")
        print(f"DEBUG: Raw address (row {idx+1}): {raw_addr}")
        raw_addr = raw_addr.strip()
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
        else:
            lat, lon, addr_used, address_nominatim, method, error = geocode_address(raw_addr)
            if idx < debug_limit:
                print(f"DEBUG: Nominatim response for raw: lat={lat}, lon={lon}, used_addr={addr_used}, address_nominatim={address_nominatim}, error={error}")
            if lat and lon:
                result_method = "raw"
            else:
                lat, lon, addr_used, address_nominatim, method, error = try_normalize_and_geocode(raw_addr)
                if idx < debug_limit:
                    print(f"DEBUG: Nominatim response for normalized: lat={lat}, lon={lon}, used_addr={addr_used}, address_nominatim={address_nominatim}, error={error}")
                result_method = "normalized" if lat and lon else method
            cache_dict[raw_addr] = {"address_geocode": addr_used or raw_addr, "address_nominatim": address_nominatim or "", "latitude": lat or "", "longitude": lon or "", "method": result_method, "error": error}
            # Don't update cache DataFrame here, do it after all threads complete
        return idx, {**row, "address_geocode": addr_used or "", "address_nominatim": address_nominatim or "", "latitude": lat or "", "longitude": lon or "", "method": method, "error": error}

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
        if r["method"] == "raw":
            raw_success += 1
        elif r["method"] == "normalized":
            norm_success += 1
        if not r["latitude"] or not r["longitude"]:
            not_found.append({"address": r.get(address_col, ""), "error": r.get("error", "")})

    pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)
    print(f"Done. Output written to {OUTPUT_FILE}")
    pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)
    print(f"Done. Output written to {OUTPUT_FILE}")
    # Write report
    with open(REPORT_FILE, "w") as f:
        f.write(f"Total addresses processed: {total}\n")
        f.write(f"Raw addresses geocoded: {raw_success}\n")
        f.write(f"Normalized addresses geocoded: {norm_success}\n")
        f.write(f"Addresses not geocoded: {len(not_found)}\n\n")
        if not_found:
            f.write("Addresses not found:\n")
            for nf in not_found:
                f.write(f"  {nf['address']} | Error: {nf['error']}\n")
    print(f"Geocode report written to {REPORT_FILE}")

if __name__ == "__main__":
    main()
