# BBBS File Organization Plan

## Goals
- Keep raw data immutable and easy to locate by year/month.
- Make each pipeline run reproducible and auditable.
- Separate code, data, outputs, and infrastructure concerns.
- Support frequent data updates without breaking downstream EDA/geocoding/routing.

## Recommended Project Structure
```text
bbbs/
  README.md  - Project overview, setup, and usage.
  requirements.txt  - Python dependencies for the pipeline.
  .gitignore  - Git ignore rules for generated/local artifacts.

  pipeline/
    configs/
      pipeline.yaml  - Cross-stage pipeline defaults and run controls.
      run_profiles.yaml  - Named run configurations (dev/prod/backfill).
    src/
      utils/
        io.py  - Shared file/path read-write helpers.
        logging.py  - Shared logging setup and formatters.
        run_context.py  - Run ID context and metadata helpers.
    scripts/
      run_full_pipeline.py  - Orchestrate ingest->clean->geocode->route->analysis->optimization.
    registry/
      runs.csv  - Run-level audit log and quality metrics.
      datasets.csv  - Dataset inventory, lineage, and version metadata.
    logs/
      pipeline/  - End-to-end orchestration logs.

  ingest/
    config/
      ingest.yaml  - Ingestion source and schema configuration.
    src/
      load_optimoroute.py  - Load and normalize raw Optimoroute exports.
    scripts/
      run_ingest.py  - CLI entry point for ingest stage.
    input/
      optimoroute/
        2021/
        2022/
        2023/
        2024/
        2025/
        2026/
    output/
      <run_id>/
        stops_ingested.csv  - Raw rows normalized into canonical ingest schema.
      latest/
        stops_ingested.csv  - Latest approved ingest output.

  clean/
    config/
      clean.yaml  - Cleaning, validation, and standardization rules.
    src/
      address_standardization.py  - Standardize and validate address fields.
      zip_repair.py  - Repair missing/invalid ZIP codes during cleaning.
    scripts/
      run_clean.py  - CLI entry point for clean stage.
    input/
      <run_id>/
        stops_ingested.csv  - Ingest output consumed by cleaner.
    interim/
      <run_id>/
        addresses_tagged.csv  - Address parsing/quality flags per stop.
        zip_repaired.csv  - ZIP corrections before final clean output.
    output/
      <run_id>/
        stops_clean.csv  - Canonical cleaned stop dataset.
      latest/
        stops_clean.csv  - Latest approved clean output.

  geocode/
    config/
      geocoding.yaml  - Geocoding provider/options configuration.
    src/
      nominatim_search.py  - Query Nominatim and parse geocode responses.
      tiger_lookup.py  - Use TIGER data to enrich/validate coordinates.
      fuzzy_match.py  - Fuzzy matching for unresolved address variants.
    scripts/
      run_geocode.py  - CLI entry point for geocode stage.
    input/
      <run_id>/
        stops_clean.csv  - Clean output consumed by geocoder.
    interim/
      <run_id>/
        geocode_candidates.csv  - Candidate coordinates before final selection.
    output/
      <run_id>/
        stops_geocoded.csv  - Cleaned stops with final coordinates.
      latest/
        stops_geocoded.csv  - Latest approved geocode output.
    logs/
      geocoding/  - Geocoding request and error logs.

  route/
    config/
      routing.yaml  - Routing engine, matrix, and metric configuration.
    src/
      osrm_client.py  - Client wrapper for OSRM route requests.
      route_metrics.py  - Compute route-level KPIs from stops/routes.
    scripts/
      run_routing.py  - CLI entry point for route stage.
    input/
      <run_id>/
        stops_geocoded.csv  - Geocode output consumed by routing.
    output/
      <run_id>/
        route_sequences.csv  - Ordered stop sequences by route.
        route_metrics.csv  - Per-route performance and distance/time metrics.
      latest/
        route_sequences.csv  - Latest approved route sequence output.
        route_metrics.csv  - Latest approved route metrics output.
    logs/
      routing/  - Routing request and metric generation logs.

  analysis/
    config/
      analysis.yaml  - Summary tables and KPI rollup settings.
      viz.yaml  - Dashboard/figure generation settings.
    src/
      city_summary.py  - City-level operational summaries.
      location_summary.py  - Location/site-level summary outputs.
      route_summary.py  - Route-level aggregate and comparison summaries.
      build_dashboards.py  - Build HTML dashboards and charts.
    scripts/
      run_eda.py  - CLI entry point for analysis/dashboard stage.
    input/
      <run_id>/
        route_sequences.csv  - Route order dataset from route stage.
        route_metrics.csv  - Route KPI dataset from route stage.
    output/
      <run_id>/
        dashboards/
          city/
          location/
          route/
          maps/
        figures/
        tables/
        reports/
      latest/
        dashboards/  - Latest approved dashboards.
        reports/  - Latest approved analysis reports.

  optimization/
    config/
      optimization.yaml  - Solver constraints, objectives, and penalties.
    src/
      optimize_routes.py  - Build and solve route optimization scenarios.
      scenario_compare.py  - Compare baseline vs optimized outcomes.
    scripts/
      run_optimization.py  - CLI entry point for optimization stage.
    input/
      <run_id>/
        route_sequences.csv  - Baseline route sequences.
        route_metrics.csv  - Baseline route metrics.
    output/
      <run_id>/
        optimized_routes.csv  - Optimized route assignments and order.
        optimization_summary.csv  - KPI deltas for optimization scenarios.
      latest/
        optimized_routes.csv  - Latest approved optimization output.
        optimization_summary.csv  - Latest approved optimization summary.

  external/
    osm/
      planet_extracts/  - OSM extracts or region-specific map data.
    nominatim/
      docker-compose.nominatim.yml  - Local Nominatim service definition.
      run_nominatim.sh  - Helper script to start/manage Nominatim stack.
    osrm/
      docker-compose.osrm.yml  - Local OSRM service definition.
    tiger/
      reference/  - TIGER boundary/reference data used by geocoding.

  tests/
    unit/
      ingest/
      clean/
      geocode/
      route/
      analysis/
      optimization/
    integration/
    manual/

  notebooks/
    01_data_quality.ipynb  - Data quality profiling and issue review.
    02_geocode_coverage.ipynb  - Geocoding success/failure exploration.
    03_route_efficiency.ipynb  - Route efficiency deep-dive analysis.
    scratch/

  plan/
    prompts.md  - Prompt templates/notes for agent workflows.
    commands.md  - Frequently used project command reference.
    code_structure.md  - Target repository layout and migration plan.

  archive/
    legacy_scripts/
    legacy_outputs/
```

## Run ID and Versioning Standard
- Use one `run_id` per pipeline execution: `YYYYMMDD_HHMMSS` (example: `20260209_151500`).
- Store run-specific artifacts under each stage directory, for example:
  - `ingest/output/<run_id>/`
  - `clean/interim/<run_id>/` and `clean/output/<run_id>/`
  - `geocode/interim/<run_id>/` and `geocode/output/<run_id>/`
  - `route/output/<run_id>/`
  - `analysis/output/<run_id>/`
  - `optimization/output/<run_id>/`
- Copy or symlink newest approved artifacts to each stage's `latest/` folder.
- Track every run in `pipeline/registry/runs.csv`.

Suggested `runs.csv` columns:
- `run_id`
- `run_timestamp_utc`
- `source_files`
- `raw_row_count`
- `clean_row_count`
- `geocode_success_rate`
- `routing_success_rate`
- `code_version` (git commit hash)
- `notes`

## Operating Workflow (Each Data Update)
1. Add new raw files only to `ingest/input/optimoroute/<year>/`.
2. Execute `pipeline/scripts/run_full_pipeline.py --run-id <run_id>`.
3. Write stage outputs in order: `ingest/output/<run_id>/` -> `clean/output/<run_id>/` -> `geocode/output/<run_id>/` -> `route/output/<run_id>/`.
4. Save analysis artifacts to `analysis/output/<run_id>/`.
5. Save optimization artifacts to `optimization/output/<run_id>/` (if optimization runs).
6. Append run metadata and quality metrics to `pipeline/registry/runs.csv`.
7. Promote validated artifacts to each stage's `latest/` folder.

## Current-to-New Mapping
- `clean_data/raw_data/*` -> `ingest/input/optimoroute/*`
- `clean_data/nominatim_search.py` -> `geocode/src/nominatim_search.py`
- `clean_data/nominatim_helpers/*` -> `clean/src/*` or `geocode/src/*` (depending on function)
- `clean_data/data_add_geocode.py` -> `geocode/scripts/run_geocode.py` + `geocode/src/*`
- `create_*_data.py` -> `analysis/src/*` with wrapper in `analysis/scripts/`
- `viz_*.py` -> `analysis/src/build_dashboards.py` (or related `analysis/src/*`)
- `dash_*.html` -> `analysis/output/<run_id>/dashboards/*`
- `data_*.csv` in repo root -> stage-specific `output/<run_id>/` folders (or `latest/`)
- `docker-compose.nominatim.yml` and `docker-compose.osrm.yml` -> `external/nominatim/` and `external/osrm/`
- `run_nominatim_docker.sh` -> `external/nominatim/run_nominatim.sh`
- `nominatim_test*.py`, `parser_test.py`, `rapidfuzz_demo.py` -> `tests/manual/` or `notebooks/scratch/`

## Git Hygiene Recommendations
- Keep large generated data and HTML out of git by default.
- Track only:
  - source code (`*/src/`, `*/scripts/`, `tests/`)
  - configs (`*/config/`, `pipeline/configs/`, `external/*/docker-compose*.yml`)
  - small sample datasets (if needed for tests)
  - plans/docs (`plan/`, `README.md`)
- Extend `.gitignore` for:
  - `*/interim/`
  - `*/output/`
  - `*/logs/`
  - `external/osm/planet_extracts/`
  - `external/tiger/reference/`

## Suggested Cleanup Sequence
1. Create the new directories.
2. Move raw files first (no transformations) into `ingest/input/`.
3. Move code into stage folders (`ingest/src`, `clean/src`, `geocode/src`, `route/src`, `analysis/src`, `optimization/src`).
4. Add stage-local entry points in each `*/scripts/` folder.
5. Move generated CSV/HTML outputs into each stage's `output/` folder.
6. Archive legacy one-off scripts and old artifacts in `archive/`.
