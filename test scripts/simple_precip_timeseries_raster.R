#!/usr/bin/env Rscript

# Simple test script that accesses Dynamical Zarr data and creates:
# 1) Point variable time-series for one forecast trace.
# 2) Raster variable map for a 1-degree box around a point with US county overlays.

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

ensure_python_modules <- function(modules, method = "auto") {
  missing <- modules[!vapply(modules, reticulate::py_module_available, logical(1))]

  if (length(missing) == 0) {
    cat("Python modules already available:", paste(modules, collapse = ", "), "\n")
    return(invisible(TRUE))
  }

  cat("Missing Python modules detected:", paste(missing, collapse = ", "), "\n")
  cat("Attempting installation via reticulate::py_install() ...\n")

  tryCatch(
    {
      reticulate::py_install(missing, pip = TRUE, method = method)
      still_missing <- missing[!vapply(missing, reticulate::py_module_available, logical(1))]
      if (length(still_missing) > 0) {
        stop(
          sprintf(
            "Python modules are still unavailable after install: %s",
            paste(still_missing, collapse = ", ")
          )
        )
      }
      cat("Python module installation completed.\n")
      invisible(TRUE)
    },
    error = function(e) {
      stop(
        paste0(
          "Failed to install required Python modules via reticulate. ",
          "Please install these modules into your reticulate Python environment and rerun: ",
          paste(missing, collapse = ", "),
          "\nOriginal error: ",
          conditionMessage(e)
        )
      )
    }
  )
}

# -------------------- User-configurable test settings --------------------
source_url <- "https://data.dynamical.org/noaa/gefs/forecast-35-day/latest.zarr?email=optional@email.com"
lat0 <- 39.940
lon0 <- -105.518
bbox_pad <- 1.0
lead_time_target <- "24h"   # nearest available lead time around +24h
variable_name <- "precipitation_surface"
output_dir <- "test scripts/output"
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

cat("Opening Zarr dataset:\n", source_url, "\n\n")

# Ensure Python packages are available in the active reticulate environment.
ensure_python_modules(c("numpy", "xarray", "zarr", "fsspec","requests","aiohttp"))

xr <- import("xarray")
np <- import("numpy")
py_builtins <- import_builtins()

# chunks = NULL keeps this script simple for local testing
# decode_timedelta = TRUE helps when lead_time is timedelta encoded
ds <- xr$open_zarr(source_url, decode_timedelta = TRUE, chunks = NULL)

if (!(variable_name %in% names(ds$data_vars))) {
  stop(
    sprintf(
      "Variable '%s' not found in dataset. Available data_vars: %s",
      variable_name,
      paste(names(ds$data_vars), collapse = ", ")
    )
  )
}

safe_variable_name <- gsub("[^A-Za-z0-9_]+", "_", variable_name)
cat("Using variable:", variable_name, "\n")

# -------------------- 1) Point time-series --------------------
# One trace = one ensemble member from latest init_time
point_da <- ds[[variable_name]]$isel(init_time = as.integer(-1), ensemble_member = as.integer(0))$sel(
  latitude = lat0,
  longitude = lon0,
  method = "nearest"
)

valid_time <- as.POSIXct(py_to_r(point_da$coords$get("valid_time")$values), tz = "UTC")
point_vals <- as.numeric(py_to_r(point_da$values))

point_df <- data.frame(
  valid_time = valid_time,
  value = point_vals
)

cat("Point selection complete at nearest grid point to:", lat0, lon0, "\n")
print(head(point_df, 5))

ts_plot <- ggplot(point_df, aes(x = valid_time, y = value)) +
  geom_line(color = "steelblue", linewidth = 0.7) +
  labs(
    title = sprintf("GEFS %s trace (single ensemble member)", variable_name),
    subtitle = sprintf("Latest init_time, member 0, nearest point to lat=%.3f lon=%.3f", lat0, lon0),
    x = "Valid time (UTC)",
    y = variable_name
  ) +
  theme_minimal(base_size = 12)

ts_path <- file.path(output_dir, sprintf("point_%s_timeseries.png", safe_variable_name))
ggsave(ts_path, ts_plot, width = 10, height = 4, dpi = 140)
cat("Saved:", ts_path, "\n")

# -------------------- 2) Raster for 1-degree bounding box --------------------
lat_min <- lat0 - bbox_pad
lat_max <- lat0 + bbox_pad
lon_min <- lon0 - bbox_pad
lon_max <- lon0 + bbox_pad

# latitude can be descending in many weather grids; use Python built-in slice() via reticulate
var_slice <- ds[[variable_name]]$isel(init_time = as.integer(-1), ensemble_member = as.integer(0))$sel(
  lead_time = lead_time_target,
  method = "nearest"
)

grid_lats <- as.numeric(py_to_r(var_slice$coords$get("latitude")$values))
grid_lons <- as.numeric(py_to_r(var_slice$coords$get("longitude")$values))

lat_slice <- if (length(grid_lats) >= 2 && grid_lats[1] > grid_lats[2]) {
  py_builtins$slice(lat_max, lat_min)
} else {
  py_builtins$slice(lat_min, lat_max)
}

lon_slice <- if (length(grid_lons) >= 2 && grid_lons[1] > grid_lons[2]) {
  py_builtins$slice(lon_max, lon_min)
} else {
  py_builtins$slice(lon_min, lon_max)
}

raster_da <- var_slice$sel(latitude = lat_slice, longitude = lon_slice)

lats <- as.numeric(py_to_r(raster_da$coords$get("latitude")$values))
lons <- as.numeric(py_to_r(raster_da$coords$get("longitude")$values))
vals_matrix <- py_to_r(np$array(raster_da$values))

# Build a plotting data frame (longitude x latitude grid)
raster_df <- do.call(
  rbind,
  lapply(seq_along(lats), function(i) {
    data.frame(
      longitude = lons,
      latitude = lats[i],
      value = as.numeric(vals_matrix[i, ])
    )
  })
)

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
    aes(x = longitude, y = latitude, fill = value)
  ) +
  scale_fill_viridis_c(option = "C", na.value = "transparent") +
  geom_sf(data = counties_crop, fill = NA, color = "black", linewidth = 0.2) +
  coord_sf(
    xlim = c(lon_min, lon_max),
    ylim = c(lat_min, lat_max),
    expand = FALSE
  ) +
  labs(
    title = sprintf("GEFS %s raster (single trace)", variable_name),
    subtitle = sprintf("Latest init_time, member 0, lead_time nearest '%s'", lead_time_target),
    x = "Longitude",
    y = "Latitude",
    fill = variable_name
  ) +
  theme_minimal(base_size = 12)

ras_path <- file.path(output_dir, sprintf("bbox_%s_raster_with_counties.png", safe_variable_name))
ggsave(ras_path, ras_plot, width = 7, height = 7, dpi = 140)
cat("Saved:", ras_path, "\n")

cat("\nDone.\n")
