# Problem Instance Format

Each problem instance file is named like:

- `problem_instance_YYYY_MM_DD.csv`

Important: the file extension is `.csv`, but the file content is JSON.

## Top-Level JSON Schema

```json
{
  "start_date": "YYYY-MM-DD",
  "end_date": "YYYY-MM-DD",
  "stop_count": 0,
  "travel_time_unit": "seconds",
  "travel_distance_unit": "meters",
  "stops": [...],
  "travel_time_matrix_seconds": [[...]],
  "travel_distance_matrix_meters": [[...]]
}
```

Notes:

- `stop_count == len(stops)`.
- Both matrices are `N x N`, where `N = stop_count`.
- Matrix row/column index `i` corresponds to `stops[i]`.
- `stops[0]` is always the manually injected depot:
  - `display_name`: `DEPOT, OLD PLAINFEILD PIKE AND TUNK HILL ROAD`
  - `latitude`: `41.766160818147014`
  - `longitude`: `-71.63312226854363`
- Older files may not include `travel_time_unit` / `travel_distance_unit`. If missing, use matrix names (`*_seconds`, `*_meters`) as the units.

## Stop Object Schema

Each item in `stops` has:

- `stop_index` (int): index in `stops`; should match its list position.
- `display_name` (string): canonical stop/location name.
- `visits_in_range` (int): number of visits in `[start_date, end_date]`.
- `stops_in_previous_7` (int): visits in the 7 days before `start_date`.
- `stops_in_previous_30` (int): visits in the 30 days before `start_date`.
- `is_bin` (bool): stop matched a BIN location.
- `is_routine` (bool): stop matched a routine location.
- `bin_id` (string): BIN id or empty string.
- `bin_cluster_id` (string): BIN cluster id or empty string.
- `bin_location_name_primary` (string): BIN label or empty string.
- `routine_total_stop_count` (int or null): historical routine total.
- `routine_max_monthly_stop_count` (int or null): historical routine monthly max.
- `latitude` (float or null)
- `longitude` (float or null)

## Matrix Semantics

- `travel_time_matrix_seconds[i][j]` = OSRM driving travel time from stop `i` to stop `j` in **seconds**.
- `travel_distance_matrix_meters[i][j]` = OSRM driving distance from stop `i` to stop `j` in **meters**.
- Matrix values can be `null` if coordinates are missing or OSRM has no route.
- Diagonal values (`i == j`) are usually 0 when OSRM data is available.

## Loading Pattern (Python)

```python
import json

with open("problem_instance_2024_11_01.csv", "r", encoding="utf-8") as f:
    inst = json.load(f)

stops = inst["stops"]
time_s = inst["travel_time_matrix_seconds"]
dist_m = inst["travel_distance_matrix_meters"]
n = inst["stop_count"]

assert n == len(stops) == len(time_s) == len(dist_m)
assert all(len(row) == n for row in time_s)
assert all(len(row) == n for row in dist_m)
```
