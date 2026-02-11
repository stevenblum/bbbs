#!/usr/bin/env python3
"""Quick search helper for OpenAddresses RI data."""
from pathlib import Path
import sys

# --- Search terms (edit these) ---
STATE = "ri"
SEARCH_TERMS = {
    "postcode": "",  # example: "02903" or "02903|02904"
    "city": "",      # example: "Providence"
    "street": "Main",    # example: "Main"
    "number": "",    # example: "123"
    "unit": "",      # example: "Apt"
    "id": "",        # example: "123456"
}
ANY_TERM = ""          # optional global search across all columns
MAX_RESULTS = 50        # number of rows to print

# --- Data location ---
BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "openaddress" / "us" / STATE

try:
    import pandas as pd
except ImportError:
    print("pandas is required. Install with: pip install pandas")
    sys.exit(1)


def find_dataset_file(data_dir: Path) -> Path:
    if not data_dir.exists():
        print(f"Data directory not found: {data_dir}")
        sys.exit(1)

    patterns = ["*.parquet", "*.csv", "*.tsv", "*.txt", "*.geojson", "*.json", "*.zip"]
    candidates = []
    for pattern in patterns:
        candidates.extend(sorted(data_dir.rglob(pattern)))

    if not candidates:
        print(f"No dataset files found under: {data_dir}")
        sys.exit(1)

    return candidates[0]


def load_zip_csv(path: Path) -> "pd.DataFrame":
    import zipfile

    with zipfile.ZipFile(path) as zf:
        names = [n for n in zf.namelist() if n.lower().endswith((".csv", ".tsv", ".txt"))]
        if not names:
            raise ValueError(f"No CSV/TSV/TXT files inside zip: {path}")
        name = sorted(names)[0]
        sep = "\t" if name.lower().endswith(".tsv") else ","
        with zf.open(name) as f:
            return pd.read_csv(
                f,
                sep=sep,
                dtype=str,
                keep_default_na=False,
                na_filter=False,
                low_memory=False,
            )


def load_df(path: Path) -> "pd.DataFrame":
    ext = path.suffix.lower()
    if ext == ".parquet":
        return pd.read_parquet(path)
    if ext == ".zip":
        return load_zip_csv(path)
    if ext in {".csv", ".tsv", ".txt"}:
        sep = "\t" if ext == ".tsv" else ","
        return pd.read_csv(
            path,
            sep=sep,
            dtype=str,
            keep_default_na=False,
            na_filter=False,
            low_memory=False,
        )
    if ext in {".json", ".geojson"}:
        import json
        from json import JSONDecodeError

        def features_to_df(features: list[dict]) -> "pd.DataFrame":
            rows = []
            for feature in features:
                if not isinstance(feature, dict):
                    continue
                props = feature.get("properties") or {}
                geom = feature.get("geometry") or {}
                row = {}
                if isinstance(props, dict):
                    row.update(props)
                if isinstance(geom, dict):
                    row["geometry_type"] = geom.get("type", "")
                    row["geometry"] = geom.get("coordinates", "")
                rows.append(row)
            return pd.DataFrame(rows).astype(str)

        try:
            with path.open("r", encoding="utf-8") as f:
                obj = json.load(f)

            # Handle GeoJSON FeatureCollection
            if isinstance(obj, dict) and obj.get("type") == "FeatureCollection":
                features = obj.get("features", [])
                return features_to_df(features)

            # Handle single Feature object
            if isinstance(obj, dict) and obj.get("type") == "Feature":
                return features_to_df([obj])

            # Fallback to pandas for non-GeoJSON JSON
            df = pd.read_json(path)
            return df.astype(str)
        except JSONDecodeError:
            # Likely newline-delimited JSON (one Feature per line)
            rows = []
            with path.open("r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        obj = json.loads(line)
                    except JSONDecodeError:
                        continue
                    if isinstance(obj, dict) and obj.get("type") == "FeatureCollection":
                        features = obj.get("features", [])
                        rows.extend(features)
                        continue
                    if isinstance(obj, dict) and obj.get("type") == "Feature":
                        rows.append(obj)
                        continue
                    if isinstance(obj, dict):
                        rows.append(obj)
            return features_to_df(rows)

    raise ValueError(f"Unsupported file type: {path}")


def apply_filters(df: "pd.DataFrame") -> "pd.DataFrame":
    col_map = {c.lower(): c for c in df.columns}
    missing = []
    mask = pd.Series(True, index=df.index)

    for field, term in SEARCH_TERMS.items():
        term = str(term).strip()
        if not term:
            continue
        col = col_map.get(field.lower())
        if not col:
            missing.append(field)
            continue
        terms = [t.strip() for t in term.split("|") if t.strip()]
        col_values = df[col].astype(str)
        field_mask = False
        for t in terms:
            field_mask = field_mask | col_values.str.contains(t, case=False, na=False)
        mask &= field_mask

    any_term = str(ANY_TERM).strip()
    if any_term:
        any_mask = pd.DataFrame(
            {c: df[c].astype(str).str.contains(any_term, case=False, na=False) for c in df.columns}
        ).any(axis=1)
        mask &= any_mask

    if missing:
        print(f"Warning: missing columns skipped: {', '.join(missing)}")

    return df[mask]


def main() -> None:
    path = find_dataset_file(DATA_DIR)
    print(f"Using dataset: {path}")

    df = load_df(path)
    print(f"Loaded {len(df):,} rows, {len(df.columns)} columns")
    print("Columns:", ", ".join(df.columns))

    filtered = apply_filters(df)
    if filtered.empty:
        print("No matches.")
        return

    print(f"Matches: {len(filtered):,}")
    print(filtered.head(MAX_RESULTS).to_string(index=False))


if __name__ == "__main__":
    main()
