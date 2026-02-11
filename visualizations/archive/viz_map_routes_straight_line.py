import math
import pandas as pd
import folium
from folium.map import CustomPane
from pathlib import Path

# ---- Data + filters (edit these) ----
CSV_PATH = "data_geocode_20260203_30KNotFound.csv"
START_DATE = "2023-01-01"  # YYYY-MM-DD; leave blank to auto-pick range
END_DATE = "2023-02-01"    # YYYY-MM-DD; leave blank to auto-pick range
DEFAULT_RANGE_DAYS = 30  # used when START_DATE/END_DATE are blank
OUTPUT_HTML = "dash_route_map_range.html"
SAVERS_CSV = "data_savers_addresses.csv"  # optional; set to "" to skip
# ------------------------------------


def main():
    COL_DRIVER = "Driver"
    COL_DATE = "Planned Date"
    COL_STOP = "Planned Stop Number"
    COL_LAT = "latitude"
    COL_LON = "longitude"
    COL_ADDRESS = "Address"

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

    def _fmt_address(value):
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return "Address: (missing)"
        text = str(value).strip()
        return f"Address: {text}" if text else "Address: (missing)"

    path = Path(CSV_PATH)
    if not path.exists():
        raise SystemExit(f"CSV not found: {path}")

    df = pd.read_csv(path)
    if COL_DATE not in df.columns:
        raise SystemExit(f"Missing '{COL_DATE}' column")

    # Clean columns once
    for col in [COL_DATE, COL_DRIVER]:
        df[col] = df[col].astype(str)

    df[COL_DRIVER] = df[COL_DRIVER].fillna("Unknown")
    df[COL_STOP] = pd.to_numeric(df[COL_STOP], errors="coerce")
    df[COL_LAT] = pd.to_numeric(df[COL_LAT], errors="coerce")
    df[COL_LON] = pd.to_numeric(df[COL_LON], errors="coerce")

    df["_date"] = pd.to_datetime(df[COL_DATE], errors="coerce")
    df = df[~df["_date"].isna()].copy()
    if df.empty:
        raise SystemExit("No valid Planned Dates found in CSV")

    min_date = df["_date"].min()
    max_date = df["_date"].max()

    if START_DATE or END_DATE:
        start = pd.to_datetime(START_DATE, errors="coerce") if START_DATE else min_date
        end = pd.to_datetime(END_DATE, errors="coerce") if END_DATE else max_date
    else:
        end = max_date
        start = end - pd.Timedelta(days=max(DEFAULT_RANGE_DAYS - 1, 0))

    if pd.isna(start) or pd.isna(end):
        raise SystemExit("Invalid START_DATE/END_DATE; use YYYY-MM-DD")
    if start > end:
        start, end = end, start

    df_range = df[(df["_date"] >= start) & (df["_date"] <= end)].copy()
    df_range[COL_DATE] = df_range["_date"].dt.strftime("%Y-%m-%d")

    dates = sorted(df_range[COL_DATE].unique())
    if not dates:
        raise SystemExit("No Planned Dates found in selected range")

    # Load Savers drop-off locations (S markers)
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
    valid_all = df_range.dropna(subset=[COL_LAT, COL_LON])
    if not valid_all.empty:
        center_lat = float(valid_all[COL_LAT].mean())
        center_lon = float(valid_all[COL_LON].mean())
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

    # Plot Savers drop-off locations (always visible, behind routes)
    if savers_df is not None and not savers_df.empty:
        for _, row in savers_df.iterrows():
            popup = f"Savers Drop-off | {row['address']}"
            folium.Marker(
                location=[row["latitude"], row["longitude"]],
                popup=popup,
                icon=_savers_icon(),
                pane="savers",
            ).add_to(m)

    legend_blocks = []
    date_layers = []

    for date_index, date in enumerate(dates):
        filtered = df_range[df_range[COL_DATE] == date].copy()

        if filtered.empty:
            print(f"No rows for Planned Date = {date}")
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
            print(f"Date: {date}")
            print(routes_by_driver)
            print(f"Total drivers/routes: {len(routes_by_driver)}")
            print(f"Missing coord rows: {missing_total}")

        fg = folium.FeatureGroup(name=str(date), show=(date_index == 0))
        fg.add_to(m)

        drivers = sorted(filtered[COL_DRIVER].unique()) if not filtered.empty else []
        legend_rows = []

        for i, driver in enumerate(drivers):
            color = colors[i % len(colors)]
            driver_df = filtered[filtered[COL_DRIVER] == driver].copy()
            driver_valid = driver_df.dropna(subset=[COL_LAT, COL_LON]).sort_values(
                COL_STOP, na_position="last"
            )

            coords = list(zip(driver_valid[COL_LAT], driver_valid[COL_LON]))
            if len(coords) >= 2:
                folium.PolyLine(
                    coords, color=color, weight=3, opacity=0.9, pane="routes"
                ).add_to(fg)

            # Markers for each stop
            for _, row in driver_valid.iterrows():
                stop = row[COL_STOP]
                addr_text = _fmt_address(row.get(COL_ADDRESS))
                popup = f"{driver} | Stop {stop}<br>{addr_text}"
                folium.CircleMarker(
                    location=[row[COL_LAT], row[COL_LON]],
                    radius=4,
                    color=color,
                    fill=True,
                    fill_opacity=0.9,
                    popup=popup,
                    pane="routes",
                ).add_to(fg)

            # First/last stop markers (offset to avoid covering stop marker)
            if not driver_valid.empty:
                first_row = driver_valid.iloc[0]
                last_row = driver_valid.iloc[-1]

                f_lat, f_lon = _offset_lat_lon(
                    float(first_row[COL_LAT]),
                    float(first_row[COL_LON]),
                    seed=f"{driver}-{date}-first",
                )
                l_lat, l_lon = _offset_lat_lon(
                    float(last_row[COL_LAT]),
                    float(last_row[COL_LON]),
                    seed=f"{driver}-{date}-last",
                )

            first_addr = _fmt_address(first_row.get(COL_ADDRESS))
            last_addr = _fmt_address(last_row.get(COL_ADDRESS))

            folium.Marker(
                location=[f_lat, f_lon],
                icon=_first_stop_icon(color),
                popup=f"{driver} | First Stop<br>{first_addr}",
                pane="routes",
            ).add_to(fg)
            folium.Marker(
                location=[l_lat, l_lon],
                icon=_last_stop_icon(color),
                popup=f"{driver} | Last Stop<br>{last_addr}",
                pane="routes",
            ).add_to(fg)

            missing_driver = missing_by_driver.get(driver, 0)
            legend_rows.append(
                f"<div><span style='display:inline-block;width:12px;height:12px;"
                f"background:{color};margin-right:6px;'></span>"
                f"{driver} (stops {len(driver_df)}, missing {missing_driver})</div>"
            )

        if not legend_rows:
            legend_rows.append("<div>No routes for this date.</div>")

        legend_html = f"""
        <div id="legend-{date}" style="position: fixed; bottom: 20px; left: 20px; z-index: 9999;
                    background: white; border:1px solid #ccc; padding:10px;
                    font-size:12px; max-width: 320px; display: {'block' if date_index == 0 else 'none'};">
          <b>Routes for {date}</b><br>
          {"".join(legend_rows)}
          <div style="margin-top:8px; font-size:11px; color:#555;">
            Missing coord rows (total): {missing_total}
          </div>
        </div>
        """
        legend_blocks.append(legend_html)
        date_layers.append((date, fg.get_name()))

    for block in legend_blocks:
        m.get_root().html.add_child(folium.Element(block))

    # Top slider bar
    selector_html = f"""
    <div id="date-control" style="position: fixed; top: 16px; left: 50%; transform: translateX(-50%);
                z-index: 9999; background: white; border:1px solid #ccc; padding:8px 12px;
                font-size:12px; display:flex; align-items:center; gap:8px;">
      <button id="play-toggle" style="padding:2px 6px; font-size:12px;">Play</button>
      <input id="date-slider" type="range" min="0" max="{max(len(dates) - 1, 0)}" value="0"
             style="width: 260px;" />
      <div id="date-label" style="font-weight:600; min-width:90px; text-align:center;"></div>
      <label for="speed-select" style="margin-left:4px;">Delay</label>
      <select id="speed-select" style="padding:2px 4px; font-size:12px;">
        <option value="1">1s</option>
        <option value="3" selected>3s</option>
        <option value="5">5s</option>
        <option value="10">10s</option>
      </select>
      <div style="margin-left:6px; font-size:11px; color:#555;">
        Range: {start.strftime("%Y-%m-%d")} to {end.strftime("%Y-%m-%d")}
      </div>
    </div>
    """

    m.get_root().html.add_child(folium.Element(selector_html))

    # Layer toggle JS (attach after map script)
    layer_map = ",\n".join(
        [f'"{d}": window["{layer_name}"]' for d, layer_name in date_layers]
    )
    dates_js = ",\n".join([f'"{d}"' for d in dates])
    js = f"""
    window.addEventListener("load", function() {{
      var map = window["{m.get_name()}"];
      if (!map) return;
      var dateLayers = {{
    {layer_map}
      }};
      var dates = [{dates_js}];

      function showDate(date) {{
        for (var d in dateLayers) {{
          if (map.hasLayer(dateLayers[d])) {{
            map.removeLayer(dateLayers[d]);
          }}
          var legend = document.getElementById("legend-" + d);
          if (legend) legend.style.display = "none";
        }}
        if (dateLayers[date]) {{
          map.addLayer(dateLayers[date]);
        }}
        var legend = document.getElementById("legend-" + date);
        if (legend) legend.style.display = "block";
      }}

      var slider = document.getElementById("date-slider");
      var label = document.getElementById("date-label");
      var playBtn = document.getElementById("play-toggle");
      var speedSelect = document.getElementById("speed-select");
      var timer = null;
      var playing = false;

      function setIndex(idx) {{
        var clamped = Math.max(0, Math.min(dates.length - 1, idx));
        if (slider) slider.value = clamped;
        var date = dates[clamped];
        if (label) label.textContent = date || "";
        if (date) showDate(date);
      }}

      function stop() {{
        playing = false;
        if (timer) clearInterval(timer);
        timer = null;
        if (playBtn) playBtn.textContent = "Play";
      }}

      function start() {{
        if (dates.length === 0) return;
        playing = true;
        if (playBtn) playBtn.textContent = "Pause";
        var delaySec = parseInt(speedSelect ? speedSelect.value : "3", 10) || 3;
        timer = setInterval(function() {{
          var next = (parseInt(slider.value || 0, 10) + 1) % dates.length;
          setIndex(next);
        }}, delaySec * 1000);
      }}

      if (slider) {{
        slider.addEventListener("input", function(e) {{
          stop();
          setIndex(parseInt(e.target.value, 10));
        }});
      }}

      if (speedSelect) {{
        speedSelect.addEventListener("change", function() {{
          if (playing) {{
            stop();
            start();
          }}
        }});
      }}

      if (playBtn) {{
        playBtn.addEventListener("click", function() {{
          if (playing) stop();
          else start();
        }});
      }}

      setIndex(0);
    }});
    """

    m.get_root().script.add_child(folium.Element(js))

    m.save(OUTPUT_HTML)
    print(f"Saved map: {OUTPUT_HTML}")


if __name__ == "__main__":
    main()
