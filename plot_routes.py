import math
import pandas as pd
import folium
from folium.map import CustomPane
from pathlib import Path

# ---- Data + filters (edit these) ----
CSV_PATH = "clean_data/data_geocode.csv"
PLANNED_DATE = "2024-11-01"  # format: YYYY-MM-DD
OUTPUT_HTML = "route_map_2024-11-01.html"
SAVERS_CSV = "savers_addresses.csv"  # optional; set to "" to skip
# ------------------------------------

COL_DRIVER = "Driver"
COL_DATE = "Planned Date"
COL_STOP = "Planned Stop Number"
COL_LAT = "latitude"
COL_LON = "longitude"

colors = [
    "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
    "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
]

def _offset_lat_lon(lat, lon, seed, magnitude=0.0006):
    # Small deterministic nudge to avoid covering other markers
    seed_hash = abs(hash(seed))
    angle = (seed_hash % 360) * math.pi / 180.0
    scale = magnitude * (1.0 + (seed_hash % 3) * 0.15)
    return lat + scale * math.cos(angle), lon + scale * math.sin(angle)

def _first_stop_icon(color):
    return folium.DivIcon(
        html=f"""
        <div style="
            width:22px;height:22px;border-radius:50%;
            background:{color};color:#fff;font-weight:700;
            display:flex;align-items:center;justify-content:center;
            border:2px solid #fff;box-shadow:0 0 0 1px {color};
            font-size:12px;">
            1
        </div>
        """,
        icon_size=(22, 22),
        icon_anchor=(11, 11),
    )

def _last_stop_icon(color):
    return folium.DivIcon(
        html=f"""
        <div style="
            width:22px;height:22px;border-radius:50%;
            background:#fff;border:2px solid {color};
            display:flex;align-items:center;justify-content:center;
            box-shadow:0 0 0 1px {color};">
            <div style="
                width:10px;height:10px;background:{color};
            "></div>
        </div>
        """,
        icon_size=(22, 22),
        icon_anchor=(11, 11),
    )

def _savers_icon():
    # Red map pin with a white "S" (larger, with a longer neck)
    return folium.DivIcon(
        html="""
        <div style="position: relative; width: 36px; height: 58px;">
            <div style="
                position: absolute; top: 0; left: 4px;
                width: 28px; height: 28px;
                background: #d62728;
                border-radius: 50%;
                display: flex; align-items: center; justify-content: center;
                color: #fff; font-weight: 700; font-size: 16px;
                box-shadow: 0 0 0 1px #b51f1f;">
                S
            </div>
            <div style="
                position: absolute; top: 28px; left: 17px;
                width: 2px; height: 22px;
                background: #d62728;">
            </div>
            <div style="
                position: absolute; bottom: 2px; left: 13px;
                width: 10px; height: 10px;
                background: #d62728;
                transform: rotate(45deg);
                box-shadow: 0 0 0 1px #b51f1f;">
            </div>
        </div>
        """,
        icon_size=(36, 58),
        icon_anchor=(18, 58),
    )

path = Path(CSV_PATH)
if not path.exists():
    raise SystemExit(f"CSV not found: {path}")

df = pd.read_csv(path)
if COL_DATE not in df.columns:
    raise SystemExit(f"Missing '{COL_DATE}' column")

# Filter to date
filtered = df[df[COL_DATE].astype(str) == PLANNED_DATE].copy()

# Clean columns
filtered[COL_DRIVER] = filtered[COL_DRIVER].fillna("Unknown").astype(str)
filtered[COL_STOP] = pd.to_numeric(filtered[COL_STOP], errors="coerce")
filtered[COL_LAT] = pd.to_numeric(filtered[COL_LAT], errors="coerce")
filtered[COL_LON] = pd.to_numeric(filtered[COL_LON], errors="coerce")

if filtered.empty:
    print(f"No rows for Planned Date = {PLANNED_DATE}")
    missing_mask = pd.Series(dtype=bool)
    missing_total = 0
    missing_by_driver = {}
    routes_by_driver = pd.Series(dtype=int)
else:
    missing_mask = filtered[COL_LAT].isna() | filtered[COL_LON].isna()
    missing_total = int(missing_mask.sum())
    missing_by_driver = (
        filtered[missing_mask].groupby(COL_DRIVER).size().to_dict()
    )

    routes_by_driver = filtered.groupby(COL_DRIVER).size().sort_values(ascending=False)
    print("Routes by driver (stop count):")
    print(routes_by_driver)
    print(f"Total drivers/routes: {len(routes_by_driver)}")
    print(f"Missing coord rows: {missing_total}")

valid = filtered[~missing_mask].copy() if not filtered.empty else pd.DataFrame()

# Load Savers drop-off locations (red house markers)
savers_df = None
if SAVERS_CSV:
    savers_path = Path(SAVERS_CSV)
    if savers_path.exists():
        savers_df = pd.read_csv(savers_path)
        if {"address", "latitude", "longitude"}.issubset(savers_df.columns):
            savers_df["latitude"] = pd.to_numeric(savers_df["latitude"], errors="coerce")
            savers_df["longitude"] = pd.to_numeric(savers_df["longitude"], errors="coerce")
            savers_df = savers_df.dropna(subset=["latitude", "longitude"])
        else:
            print("SAVERS_CSV missing required columns: address, latitude, longitude")
            savers_df = None
    else:
        print(f"SAVERS_CSV not found: {savers_path}")

# Choose map center
if not valid.empty:
    center_lat = float(valid[COL_LAT].mean())
    center_lon = float(valid[COL_LON].mean())
    zoom = 9
elif savers_df is not None and not savers_df.empty:
    center_lat = float(savers_df["latitude"].mean())
    center_lon = float(savers_df["longitude"].mean())
    zoom = 9
else:
    center_lat, center_lon, zoom = 41.7, -71.5, 8

m = folium.Map(location=[center_lat, center_lon], zoom_start=zoom, tiles="OpenStreetMap")
CustomPane("savers", z_index=300).add_to(m)
CustomPane("routes", z_index=500).add_to(m)

drivers = sorted(filtered[COL_DRIVER].unique()) if not filtered.empty else []
legend_rows = []

for i, driver in enumerate(drivers):
    color = colors[i % len(colors)]
    driver_df = filtered[filtered[COL_DRIVER] == driver].copy()
    driver_valid = driver_df.dropna(subset=[COL_LAT, COL_LON]).sort_values(COL_STOP, na_position="last")

    coords = list(zip(driver_valid[COL_LAT], driver_valid[COL_LON]))
    if len(coords) >= 2:
        folium.PolyLine(coords, color=color, weight=3, opacity=0.9, pane="routes").add_to(m)

    # Markers for each stop
    for _, row in driver_valid.iterrows():
        stop = row[COL_STOP]
        popup = f"{driver} | Stop {stop}"
        folium.CircleMarker(
            location=[row[COL_LAT], row[COL_LON]],
            radius=4,
            color=color,
            fill=True,
            fill_opacity=0.9,
            popup=popup,
            pane="routes",
        ).add_to(m)

    # First/last stop markers (offset to avoid covering stop marker)
    if not driver_valid.empty:
        first_row = driver_valid.iloc[0]
        last_row = driver_valid.iloc[-1]

        f_lat, f_lon = _offset_lat_lon(
            float(first_row[COL_LAT]),
            float(first_row[COL_LON]),
            seed=f"{driver}-first",
        )
        l_lat, l_lon = _offset_lat_lon(
            float(last_row[COL_LAT]),
            float(last_row[COL_LON]),
            seed=f"{driver}-last",
        )

        folium.Marker(
            location=[f_lat, f_lon],
            icon=_first_stop_icon(color),
            popup=f"{driver} | First Stop",
            pane="routes",
        ).add_to(m)
        folium.Marker(
            location=[l_lat, l_lon],
            icon=_last_stop_icon(color),
            popup=f"{driver} | Last Stop",
            pane="routes",
        ).add_to(m)

    missing_driver = missing_by_driver.get(driver, 0)
    legend_rows.append(
        f"<div><span style='display:inline-block;width:12px;height:12px;"
        f"background:{color};margin-right:6px;'></span>"
        f"{driver} (stops {len(driver_df)}, missing {missing_driver})</div>"
    )

legend_html = f"""
<div style="position: fixed; bottom: 20px; left: 20px; z-index: 9999;
            background: white; border:1px solid #ccc; padding:10px;
            font-size:12px; max-width: 320px;">
  <b>Routes for {PLANNED_DATE}</b><br>
  {"".join(legend_rows)}
  <div style="margin-top:8px; font-size:11px; color:#555;">
    Missing coord rows (total): {missing_total}
  </div>
</div>
"""

m.get_root().html.add_child(folium.Element(legend_html))

# Plot Savers drop-off locations (red house markers)
if savers_df is not None and not savers_df.empty:
    for _, row in savers_df.iterrows():
        popup = f"Savers Drop-off | {row['address']}"
        folium.Marker(
            location=[row["latitude"], row["longitude"]],
            popup=popup,
            icon=_savers_icon(),
            pane="savers",
        ).add_to(m)

m.save(OUTPUT_HTML)
print(f"Saved map: {OUTPUT_HTML}")
