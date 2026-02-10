# Test Scripts

This folder contains quick validation scripts to prove access to Dynamical-hosted Zarr weather datasets from R.

## Script included

- `simple_precip_timeseries_raster.R`
  - Loads NOAA GEFS forecast data from Dynamical (`latest.zarr`).
  - Extracts one precipitation forecast trace (`ensemble_member = 0`) at `lat/lon = 39.940/-105.518` and saves a time-series figure.
  - Extracts a raster for a 1-degree box around the same point at ~24h lead time and overlays US counties.

## Expected outputs

Generated in `test scripts/output/`:

- `point_precip_timeseries.png`
- `bbox_precip_raster_with_counties.png`

## Dependencies

R packages:

- `reticulate`
- `ggplot2`
- `dplyr`
- `sf`
- `tigris`

Python packages accessible via `reticulate`:

- `xarray`
- `zarr`
- `numpy`

## Run

```bash
Rscript "test scripts/simple_precip_timeseries_raster.R"
```
