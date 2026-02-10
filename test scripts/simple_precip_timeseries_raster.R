#!/usr/bin/env Rscript

# Simple test script that accesses Dynamical Zarr data and creates:
# 1) Point precipitation time-series for one forecast trace.
# 2) Raster precipitation map for a 1-degree box around a point with US county overlays.

if (!requireNamespace("reticulate", quietly = TRUE)) stop("Please install 'reticulate'.")
if (!requireNamespace("ggplot2", quietly = TRUE)) stop("Please install 'ggplot2'.")
if (!requireNamespace("dplyr", quietly = TRUE)) stop("Please install 'dplyr'.")
if (!requireNamespace("sf", quietly = TRUE)) stop("Please install 'sf'.")
if (!requireNamespace("tigris", quietly = TRUE)) stop("Please install 'tigris'.")

library(reticulate)
library(ggplot2)
library(dplyr)
library(sf)
library(tigris)

options(tigris_use_cache = TRUE)

# -------------------- User-configurable test settings --------------------
source_url <- "https://data.dynamical.org/noaa/gefs/forecast-35-day/latest.zarr?email=optional@email.com"
lat0 <- 39.940
lon0 <- -105.518
bbox_pad <- 1.0
lead_time_target <- "24h"   # nearest available lead time around +24h
output_dir <- "test scripts/output"
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

cat("Opening Zarr dataset:\n", source_url, "\n\n")

xr <- import("xarray")
np <- import("numpy")

# chunks = NULL keeps this script simple for local testing
# decode_timedelta = TRUE helps when lead_time is timedelta encoded
ds <- xr$open_zarr(source_url, decode_timedelta = TRUE, chunks = NULL)

# -------------------- 1) Point time-series --------------------
# One trace = one ensemble member from latest init_time
point_da <- ds[["precipitation_surface"]]$isel(init_time = as.integer(-1), ensemble_member = as.integer(0))$sel(
  latitude = lat0,
  longitude = lon0,
  method = "nearest"
)

valid_time <- as.POSIXct(py_to_r(point_da$coords$get("valid_time")$values), tz = "UTC")
precip_vals <- as.numeric(py_to_r(point_da$values))

point_df <- data.frame(
  valid_time = valid_time,
  precipitation_surface = precip_vals
)

cat("Point selection complete at nearest grid point to:", lat0, lon0, "\n")
print(head(point_df, 5))

ts_plot <- ggplot(point_df, aes(x = valid_time, y = precipitation_surface)) +
  geom_line(color = "steelblue", linewidth = 0.7) +
  labs(
    title = "GEFS precipitation trace (single ensemble member)",
    subtitle = sprintf("Latest init_time, member 0, nearest point to lat=%.3f lon=%.3f", lat0, lon0),
    x = "Valid time (UTC)",
    y = "Precipitation"
  ) +
  theme_minimal(base_size = 12)

ts_path <- file.path(output_dir, "point_precip_timeseries.png")
ggsave(ts_path, ts_plot, width = 10, height = 4, dpi = 140)
cat("Saved:", ts_path, "\n")

# -------------------- 2) Raster for 1-degree bounding box --------------------
lat_min <- lat0 - bbox_pad
lat_max <- lat0 + bbox_pad
lon_min <- lon0 - bbox_pad
lon_max <- lon0 + bbox_pad

# latitude can be descending in many weather grids; using slice(max, min) is common
raster_da <- ds[["precipitation_surface"]]$isel(init_time = as.integer(-1), ensemble_member = as.integer(0))$sel(
  lead_time = lead_time_target,
  method = "nearest"
)$sel(
  latitude = reticulate::tuple(lat_max, lat_min),
  longitude = reticulate::tuple(lon_min, lon_max)
)

lats <- as.numeric(py_to_r(raster_da$coords$get("latitude")$values))
lons <- as.numeric(py_to_r(raster_da$coords$get("longitude")$values))
vals <- as.numeric(py_to_r(np$array(raster_da$values)))

# Build grid; ordering is sufficient for a quick test visualization
raster_df <- expand.grid(
  longitude = lons,
  latitude = lats
)
raster_df$precipitation_surface <- vals

# County overlays (cartographic boundary files)
counties_sf <- tigris::counties(cb = TRUE, year = 2023, class = "sf")
counties_crop <- sf::st_crop(
  counties_sf,
  xmin = lon_min,
  ymin = lat_min,
  xmax = lon_max,
  ymax = lat_max
)

ras_plot <- ggplot() +
  geom_raster(
    data = raster_df,
    aes(x = longitude, y = latitude, fill = precipitation_surface)
  ) +
  scale_fill_viridis_c(option = "C", na.value = "transparent") +
  geom_sf(data = counties_crop, fill = NA, color = "black", linewidth = 0.2) +
  coord_sf(
    xlim = c(lon_min, lon_max),
    ylim = c(lat_min, lat_max),
    expand = FALSE
  ) +
  labs(
    title = "GEFS precipitation raster (single trace)",
    subtitle = sprintf("Latest init_time, member 0, lead_time nearest '%s'", lead_time_target),
    x = "Longitude",
    y = "Latitude",
    fill = "Precip"
  ) +
  theme_minimal(base_size = 12)

ras_path <- file.path(output_dir, "bbox_precip_raster_with_counties.png")
ggsave(ras_path, ras_plot, width = 7, height = 7, dpi = 140)
cat("Saved:", ras_path, "\n")

cat("\nDone.\n")
