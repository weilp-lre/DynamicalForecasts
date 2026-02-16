# Shiny Dashboard (Initial Build)

This folder contains an initial Shiny implementation for a personal Dynamical forecast dashboard.

## Run

```bash
Rscript -e "shiny::runApp('shiny-dashboard', host='0.0.0.0', port=8080)"
```

## Features in this initial build

- Unified workflow for forecast and optional obs+forecast plotting.
- Source selector (GEFS and ECMWF ENS in v1).
- Canonical variable dictionary with source-specific mappings and metric/imperial unit labels.
- Point click and pre-canned locations.
- Spaghetti forecast traces + percentile lines (25/50/75).
- Raster statistic map for current extent with refresh control.
- Dynamic downsampling by map extent to improve responsiveness.
- User-triggered metadata refresh.
- Basic request metrics output (latency and request size).

## Configuration model

All source and variable metadata live in:

- `config/source_registry.json`

### How to update with new sources/variables

1. Add a new `sources` entry:
   - `id`
   - `label`
   - `forecast_url`
   - `obs_url` (empty string if unavailable)
   - `supports_ensemble` (true/false)
2. Add source variable names to each canonical variable in `forecast_var_by_source` and `obs_var_by_source`.
3. Set `metric_units` and `imperial_units` for each canonical variable.
4. Restart the app or click **Refresh metadata**.

## Variable mapping validation guidance

To keep updates seamless:

- Treat `source_registry.json` as a versioned schema file in git.
- During updates, validate that each new mapping exists in the target dataset by checking `data_vars` names via xarray.
- If an obs/forecast mapping is missing, leave it blank (`""`) so the app can show a partial-data indicator instead of failing.


## Query-performance benchmark script

Use this benchmark helper to probe hosted zarr responsiveness across ten query shapes (from tiny point lookups to large regional slices):

```bash
python shiny-dashboard/scripts/benchmark_zarr_queries.py
```

Optional flags:

- `--queries 10` to control how many benchmark cases to run.
- `--registry shiny-dashboard/config/source_registry.json` to use a different source catalog.
- `--output-dir shiny-dashboard/data/benchmarks` to change where reports are written.

Outputs:

- `shiny-dashboard/data/benchmarks/zarr_query_benchmark_results.csv` (per-query timing/size table)
- `shiny-dashboard/data/benchmarks/zarr_query_benchmark_summary.md` (aggregate performance summary + slowest query list)
