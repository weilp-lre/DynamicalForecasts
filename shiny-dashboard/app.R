library(shiny)
library(leaflet)
library(plotly)
library(jsonlite)
library(reticulate)
library(dplyr)
library(ggplot2)
library(sf)
library(rnaturalearth)
library(rnaturalearthdata)

# Clear variables
rm(list = ls())
# sets working directory to location of script
source_path = rstudioapi::getActiveDocumentContext()$path
setwd(dirname(source_path))
getwd()


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
# Ensure Python packages are available in the active reticulate environment.
ensure_python_modules(c("numpy", "xarray", "zarr", "fsspec","requests","aiohttp"))


registry <- jsonlite::fromJSON("config/source_registry.json", simplifyVector = TRUE)

normalize_registry <- function(registry) {
  if (!is.data.frame(registry$sources)) {
    source_ids <- names(registry$sources)
    registry$sources <- bind_rows(lapply(source_ids, function(source_id) {
      src <- registry$sources[[source_id]]
      tibble(
        id = source_id,
        label = src$label %||% source_id,
        forecast_url = src$forecast_url %||% "",
        obs_url = src$obs_url %||% "",
        supports_ensemble = isTRUE(src$supports_ensemble)
      )
    }))
  }

  if (!is.data.frame(registry$canonical_variables)) {
    canonical_ids <- names(registry$canonical_variables)
    registry$canonical_variables <- bind_rows(lapply(canonical_ids, function(canonical_id) {
      var_info <- registry$canonical_variables[[canonical_id]]
      tibble(
        id = canonical_id,
        label = var_info$label %||% canonical_id,
        forecast_var_by_source = list(var_info$forecast_var_by_source %||% list()),
        obs_var_by_source = list(var_info$obs_var_by_source %||% list()),
        metric_units = var_info$metric_units %||% "",
        imperial_units = var_info$imperial_units %||% ""
      )
    }))
  }

  if (!is.data.frame(registry$pre_canned_points)) {
    point_names <- names(registry$pre_canned_points)
    registry$pre_canned_points <- bind_rows(lapply(point_names, function(point_name) {
      pt <- registry$pre_canned_points[[point_name]]
      tibble(
        name = point_name,
        lat = as.numeric(pt$lat %||% NA_real_),
        lon = as.numeric(pt$lon %||% NA_real_)
      )
    }))
  }

  registry
}

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x

registry <- normalize_registry(registry)

xr <- reticulate::import("xarray")
np <- reticulate::import("numpy")
py_builtins <- reticulate::import_builtins()


convert_units <- function(values, canonical_id, output_system, registry) {
  if (canonical_id == "t2m") {
    # assume native K
    if (output_system == "metric") return(values - 273.15)
    return((values - 273.15) * 9/5 + 32)
  }
  if (canonical_id == "precip_surface") {
    # assume native mm
    if (output_system == "metric") return(values)
    return(values / 25.4)
  }
  if (canonical_id == "wind_u_100m") {
    # assume native m/s
    if (output_system == "metric") return(values)
    return(values * 2.23694)
  }
  values
}

resolve_units <- function(canonical_id, output_system, registry) {
  v <- registry$canonical_variables[registry$canonical_variables$id == canonical_id, ]
  if (nrow(v) == 0) return("")
  if (output_system == "metric") v$metric_units[[1]] else v$imperial_units[[1]]
}

resolve_variable_label <- function(canonical_id, registry) {
  v <- registry$canonical_variables[registry$canonical_variables$id == canonical_id, ]
  if (nrow(v) == 0) return(canonical_id)

  label <- v$label[[1]]
  label <- as.character(label)
  if (!length(label) || !nzchar(label[[1]])) canonical_id else label[[1]]
}

safe_open_zarr <- function(url) {
  if (!nzchar(url)) return(NULL)
  tryCatch({
    xr$open_zarr(url, decode_timedelta = TRUE, chunks = NULL)
  }, error = function(e) {
    NULL
  })
}

get_var_name <- function(source_id, canonical_id, mode = c("forecast", "obs")) {
  mode <- match.arg(mode)
  v <- registry$canonical_variables[registry$canonical_variables$id == canonical_id, ]
  if (nrow(v) == 0) return("")
  map_col <- if (mode == "forecast") "forecast_var_by_source" else "obs_var_by_source"
  map <- v[[map_col]][[1]]
  out <- map[[source_id]]
  if (is.null(out)) "" else out
}

extract_timeseries <- function(ds, var_name, lat, lon, source_supports_ensemble = TRUE, lead_hours = 14 * 24) {
  if (is.null(ds) || !nzchar(var_name)) return(NULL)

  da <- ds[[var_name]]
  dims <- names(reticulate::py_to_r(da$dims))

  if ("init_time" %in% dims) da <- da$isel(init_time = as.integer(-1))
  if ("lead_time" %in% dims) {
    lead_max <- as.integer(lead_hours)
    da <- da$sel(lead_time = py_builtins$slice(NULL, paste0(lead_max, "h")))
  }

  if (source_supports_ensemble && "ensemble_member" %in% dims) {
    point_da <- da$sel(latitude = lat, longitude = lon, method = "nearest")
    valid_time <- as.POSIXct(reticulate::py_to_r(point_da$coords$get("valid_time")$values), tz = "UTC")
    member <- as.integer(reticulate::py_to_r(point_da$coords$get("ensemble_member")$values))
    vals <- reticulate::py_to_r(np$array(point_da$values))

    df <- expand.grid(valid_time = valid_time, ensemble_member = member)
    df$value <- as.numeric(as.vector(t(vals)))

    pct <- df %>%
      group_by(valid_time) %>%
      summarize(
        p10 = quantile(value, 0.10, na.rm = TRUE),
        p25 = quantile(value, 0.25, na.rm = TRUE),
        p50 = quantile(value, 0.50, na.rm = TRUE),
        p75 = quantile(value, 0.75, na.rm = TRUE),
        p90 = quantile(value, 0.90, na.rm = TRUE),
        n_members = sum(!is.na(value)),
        .groups = "drop"
      )

    list(trace_df = df, pct_df = pct, n_members = length(unique(df$ensemble_member)))
  } else {
    point_da <- da$sel(latitude = lat, longitude = lon, method = "nearest")
    valid_time <- if (!is.null(point_da$coords$get("valid_time"))) {
      as.POSIXct(reticulate::py_to_r(point_da$coords$get("valid_time")$values), tz = "UTC")
    } else {
      as.POSIXct(reticulate::py_to_r(point_da$coords$get("time")$values), tz = "UTC")
    }

    vals <- as.numeric(reticulate::py_to_r(point_da$values))
    df <- data.frame(valid_time = valid_time, ensemble_member = 0, value = vals)
    pct <- df %>% transmute(valid_time, p10 = value, p25 = value, p50 = value, p75 = value, p90 = value, n_members = 1)

    list(trace_df = df, pct_df = pct, n_members = 1)
  }
}

select_single_time_slice <- function(da, preferred_hour = "36h") {
  dims <- names(reticulate::py_to_r(da$dims))

  if ("init_time" %in% dims) {
    da <- da$isel(init_time = as.integer(-1))
    dims <- names(reticulate::py_to_r(da$dims))
  }

  if ("lead_time" %in% dims) {
    da <- da$sel(lead_time = preferred_hour, method = "nearest")
    dims <- names(reticulate::py_to_r(da$dims))
  }

  if ("valid_time" %in% dims) {
    da <- da$isel(valid_time = as.integer(-1))
    dims <- names(reticulate::py_to_r(da$dims))
  }

  if ("time" %in% dims) {
    da <- da$isel(time = as.integer(-1))
  }

  da
}

extract_raster <- function(ds, var_name, bounds, stat_name = "single_member", source_supports_ensemble = TRUE, alpha_stride = 1, valid_hour = "36h") {
  if (is.null(ds) || !nzchar(var_name) || is.null(bounds)) return(NULL)

  lat_min <- bounds$south
  lat_max <- bounds$north
  lon_min <- bounds$west
  lon_max <- bounds$east

  da <- ds[[var_name]]
  da <- select_single_time_slice(da, preferred_hour = valid_hour)

  grid_lats <- as.numeric(reticulate::py_to_r(da$coords$get("latitude")$values))
  grid_lons <- as.numeric(reticulate::py_to_r(da$coords$get("longitude")$values))
  if (length(grid_lats) == 0 || length(grid_lons) == 0) return(NULL)

  normalize_lon <- function(lon_values, lon_input) {
    if (all(lon_values >= 0, na.rm = TRUE)) {
      (lon_input + 360) %% 360
    } else {
      ((lon_input + 180) %% 360) - 180
    }
  }

  lon_min_norm <- normalize_lon(grid_lons, lon_min)
  lon_max_norm <- normalize_lon(grid_lons, lon_max)

  lat_idx <- which(grid_lats >= min(lat_min, lat_max) & grid_lats <= max(lat_min, lat_max))
  if (lon_min_norm <= lon_max_norm) {
    lon_idx <- which(grid_lons >= lon_min_norm & grid_lons <= lon_max_norm)
  } else {
    lon_idx <- which(grid_lons >= lon_min_norm | grid_lons <= lon_max_norm)
  }
  if (length(lat_idx) == 0 || length(lon_idx) == 0) return(NULL)

  lat_idx <- lat_idx[seq(1, length(lat_idx), by = alpha_stride)]
  lon_idx <- lon_idx[seq(1, length(lon_idx), by = alpha_stride)]

  da <- da$isel(latitude = as.integer(lat_idx - 1), longitude = as.integer(lon_idx - 1))
  dims <- names(reticulate::py_to_r(da$dims))

  if (source_supports_ensemble && "ensemble_member" %in% dims) {
    if (stat_name == "single_member") {
      da <- da$isel(ensemble_member = as.integer(0))
    } else {
      q <- c(p10 = 0.10, p25 = 0.25, median = 0.50, p75 = 0.75, p90 = 0.90)
      if (stat_name %in% names(q)) {
        da <- da$quantile(q[[stat_name]], dim = "ensemble_member")
      } else if (stat_name == "mean") {
        da <- da$mean(dim = "ensemble_member")
      } else if (stat_name == "min") {
        da <- da$min(dim = "ensemble_member")
      } else if (stat_name == "max") {
        da <- da$max(dim = "ensemble_member")
      } else {
        da <- da$isel(ensemble_member = as.integer(0))
      }
    }
  }

  dims <- names(reticulate::py_to_r(da$dims))
  for (dim_name in c("lead_time", "valid_time", "time", "init_time", "step", "forecast_hour", "quantile")) {
    if (dim_name %in% dims) {
      da <- da$isel(structure(list(as.integer(0)), names = dim_name))
      dims <- names(reticulate::py_to_r(da$dims))
    }
  }

  if (!all(c("latitude", "longitude") %in% dims) || length(dims) != 2) return(NULL)

  lats <- as.numeric(reticulate::py_to_r(da$coords$get("latitude")$values))
  lons <- as.numeric(reticulate::py_to_r(da$coords$get("longitude")$values))
  vals <- as.matrix(reticulate::py_to_r(da$values))
  if (length(lats) == 0 || length(lons) == 0) return(NULL)

  expand.grid(longitude = lons, latitude = lats) %>%
    mutate(value = as.numeric(as.vector(t(vals))))
}


ui <- fluidPage(
  tags$head(tags$style(HTML("@keyframes pulsebar {0% {opacity: 0.45;} 50% {opacity: 1;} 100% {opacity: 0.45;}}"))),
  titlePanel("Dynamical Forecast Dashboard (Initial Build)"),
  sidebarLayout(
    sidebarPanel(
      checkboxInput("debug_mode", "Debug mode (terminal logging)", value = FALSE),
      selectInput("data_mode", "Data mode", choices = c("forecast", "obs + forecast"), selected = "obs + forecast"),
      selectInput("source", "Source", choices = setNames(registry$sources$id, registry$sources$label)),
      selectInput("variable", "Variable", choices = setNames(registry$canonical_variables$id, registry$canonical_variables$label)),
      radioButtons("units", "Unit system", choices = c("Metric" = "metric", "Imperial" = "imperial"), inline = TRUE),
      radioButtons("time_axis", "Time axis", choices = c("Valid time" = "valid_time", "Lead time" = "lead_time"), inline = TRUE),
      sliderInput("lead_days", "Lead-time window (days)", min = 1, max = 35, value = 14),
      selectInput(
        "raster_stat",
        "Raster statistic",
        choices = c("single_member", "mean", "median", "min", "max", "p10", "p25", "p75", "p90"),
        selected = "mean"
      ),
      sliderInput("raster_alpha", "Raster transparency", min = 0, max = 1, value = 0.8, step = 0.05),
      selectInput("preset_point", "Pre-canned location", choices = c("-- None --", registry$pre_canned_points$name)),
      actionButton("refresh_data", "Refresh data"),
      actionButton("refresh_metadata", "Refresh metadata")
    ),
    mainPanel(
      textOutput("processing_status"),
      uiOutput("loading_bar"),
      textOutput("missing_indicator"),
      tabsetPanel(
        tabPanel(
          "Map",
          br(),
          leafletOutput("map", height = 480),
          br(),
          plotOutput("raster_plot", height = 420)
        ),
        tabPanel(
          "Forecast Ensemble (point data)",
          br(),
          plotlyOutput("timeseries", height = 420)
        )
      ),
      verbatimTextOutput("metrics")
    )
  )
)

server <- function(input, output, session) {
  debug_log <- function(step, ..., force = FALSE) {
    debug_on <- isTRUE(force) || isTRUE(input$debug_mode)
    if (!debug_on) return(invisible(NULL))

    detail <- paste0(..., collapse = "")
    timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")

    if (nzchar(detail)) {
      message(sprintf("[DEBUG %s] %s | %s", timestamp, step, detail))
    } else {
      message(sprintf("[DEBUG %s] %s", timestamp, step))
    }

    invisible(NULL)
  }

  debug_log("server_init", "Server started and reactive values initialized.", force = TRUE)

  format_named_dims <- function(da) {
    if (is.null(da)) return("<null>")

    dims_map <- reticulate::py_to_r(da$dims)
    if (is.null(dims_map) || length(dims_map) == 0) return("<none>")

    dim_names <- names(dims_map)
    if (is.null(dim_names) || !length(dim_names)) {
      return(paste(as.character(dims_map), collapse = ", "))
    }

    paste0(dim_names, "=", as.integer(unname(dims_map)), collapse = ", ")
  }

  log_zarr_request <- function(context, ds, var_name, extra = "") {
    if (is.null(ds) || !nzchar(var_name)) {
      debug_log(
        paste0("zarr_request:", context),
        "dataset_loaded=", !is.null(ds), ", var_name='", var_name, "'", if (nzchar(extra)) paste0(", ", extra) else ""
      )
      return(invisible(NULL))
    }

    da <- NULL
    da <- tryCatch(ds[[var_name]], error = function(e) NULL)
    if (is.null(da)) {
      debug_log(
        paste0("zarr_request:", context),
        "var_name='", var_name, "' not found in dataset", if (nzchar(extra)) paste0(", ", extra) else ""
      )
      return(invisible(NULL))
    }

    dtype <- tryCatch(as.character(da$dtype), error = function(e) "unknown")
    dims_txt <- tryCatch(format_named_dims(da), error = function(e) paste0("<error: ", conditionMessage(e), ">"))

    debug_log(
      paste0("zarr_request:", context),
      "var=", var_name,
      ", dtype=", dtype,
      ", dims=", dims_txt,
      if (nzchar(extra)) paste0(", ", extra) else ""
    )

    invisible(NULL)
  }

  rv <- reactiveValues(
    click_lat = 40.015,
    click_lon = -105.2705,
    click_name = "Boulder, CO",
    forecast_ds = NULL,
    obs_ds = NULL,
    last_latency_ms = NA_real_,
    last_request_size = NA_character_,
    bounds = list(north = 55, south = 20, west = -130, east = -60),
    processing_count = 0,
    processing_label = "Idle",
    point_data = NULL,
    raster_data = NULL
  )

  start_processing <- function(label) {
    isolate({
      rv$processing_count <- rv$processing_count + 1
      rv$processing_label <- label
    })
    debug_log("processing:start", "label=", label, ", count=", rv$processing_count)
  }

  finish_processing <- function() {
    isolate({
      rv$processing_count <- max(0, rv$processing_count - 1)
      if (rv$processing_count == 0) {
        rv$processing_label <- "Idle"
      }
    })
    debug_log("processing:finish", "count=", rv$processing_count)
  }

  output$processing_status <- renderText({
    if (rv$processing_count > 0) {
      paste0("Processing: ", rv$processing_label)
    } else {
      "Status: Ready"
    }
  })

  output$loading_bar <- renderUI({
    if (rv$processing_count <= 0) return(NULL)
    tags$div(
      style = "width:100%;height:12px;background:#e9ecef;border-radius:6px;margin:6px 0 12px 0;overflow:hidden;",
      tags$div(
        style = "width:100%;height:100%;background:linear-gradient(90deg,#1f78b4,#7db8ff);animation: pulsebar 1.1s infinite ease-in-out;"
      )
    )
  })
  load_datasets <- function() {
    start_processing("Loading datasets")
    on.exit(finish_processing(), add = TRUE)

    sid <- input$source
    debug_log("load_datasets:start", "source=", sid)
    src_row <- registry$sources[registry$sources$id == sid, ]
    if (nrow(src_row) == 0) {
      debug_log("load_datasets:error", "No source found in registry for source id=", sid)
      rv$forecast_ds <- NULL
      rv$obs_ds <- NULL
      return(invisible(NULL))
    }

    debug_log(
      "load_datasets:urls",
      "forecast_url=", src_row$forecast_url[[1]],
      ", obs_url=", src_row$obs_url[[1]]
    )

    rv$forecast_ds <- safe_open_zarr(src_row$forecast_url[[1]])
    rv$obs_ds <- safe_open_zarr(src_row$obs_url[[1]])

    debug_log(
      "load_datasets:result",
      "forecast_loaded=", !is.null(rv$forecast_ds),
      ", obs_loaded=", !is.null(rv$obs_ds)
    )
  }

  observeEvent(input$refresh_metadata, {
    debug_log("refresh_metadata:triggered", "Manual metadata refresh requested.")
    showNotification("Refreshing source metadata and reopening datasets...", type = "message")
    load_datasets()
  })

  observe({
    debug_log("source_observer:triggered", "Source observer triggered.")
    load_datasets()
  })

  output$map <- renderLeaflet({
    countries <- rnaturalearth::ne_countries(scale = "medium", returnclass = "sf")
    states <- rnaturalearth::ne_states(returnclass = "sf")

    leaflet() %>%
      addProviderTiles(providers$CartoDB.Positron) %>%
      addPolygons(data = countries, color = "#6b6b6b", weight = 0.5, fill = FALSE, group = "countries") %>%
      addPolygons(data = states, color = "#7f7f7f", weight = 0.25, fill = FALSE, group = "states_provinces") %>%
      addCircleMarkers(
        data = registry$pre_canned_points,
        lng = ~lon,
        lat = ~lat,
        layerId = ~name,
        radius = 5,
        color = "#0c7cba",
        fillOpacity = 0.9,
        label = ~name,
        group = "pre_canned"
      ) %>%
      setView(lng = rv$click_lon, lat = rv$click_lat, zoom = 4)
  })

  observeEvent(input$preset_point, {
    req(input$preset_point)
    if (input$preset_point == "-- None --") return()
    row <- registry$pre_canned_points[registry$pre_canned_points$name == input$preset_point, ]
    if (nrow(row) == 1) {
      rv$click_lat <- row$lat[[1]]
      rv$click_lon <- row$lon[[1]]
      rv$click_name <- row$name[[1]]
      leafletProxy("map") %>% setView(lng = rv$click_lon, lat = rv$click_lat, zoom = 6)
    }
  })

  observeEvent(input$map_click, {
    rv$click_lat <- input$map_click$lat
    rv$click_lon <- input$map_click$lng
    rv$click_name <- sprintf("Clicked point (%.3f, %.3f)", rv$click_lat, rv$click_lon)
  })

  observeEvent(input$map_bounds, {
    rv$bounds <- input$map_bounds
  })

  missing_text <- reactive({
    sid <- input$source
    canonical_id <- input$variable
    debug_log("missing_text:start", "source=", sid, ", variable=", canonical_id)
    fv <- get_var_name(sid, canonical_id, "forecast")
    ov <- get_var_name(sid, canonical_id, "obs")
    debug_log("missing_text:mapping", "forecast_var=", fv, ", obs_var=", ov)
    missing <- c()
    if (!nzchar(fv)) missing <- c(missing, "forecast")
    if (input$data_mode == "obs + forecast" && !nzchar(ov)) missing <- c(missing, "obs")
    if (length(missing) == 0) {
      "Variable availability: forecast and obs mappings available."
    } else {
      sprintf("Variable availability warning: missing mapping for %s in selected source.", paste(missing, collapse = ", "))
    }
  })

  output$missing_indicator <- renderText(missing_text())

  observeEvent(input$refresh_data, {
    req(rv$forecast_ds)
    start_processing("Refreshing point and map data")
    on.exit(finish_processing(), add = TRUE)

    withProgress(message = "Requesting data", value = 0, {
      sid <- isolate(input$source)
      canonical_id <- isolate(input$variable)
      src_row <- registry$sources[registry$sources$id == sid, ]
      fvar <- get_var_name(sid, canonical_id, "forecast")
      ovar <- get_var_name(sid, canonical_id, "obs")
      t0 <- Sys.time()
      rv$last_request_size <- NULL

      debug_log("refresh_data:start", "source=", sid, ", variable=", canonical_id)

      log_zarr_request(
        "point_forecast",
        rv$forecast_ds,
        fvar,
        extra = paste0(
          "lat=", sprintf("%.4f", rv$click_lat),
          ", lon=", sprintf("%.4f", rv$click_lon),
          ", lead_hours=", isolate(input$lead_days) * 24,
          ", supports_ensemble=", isTRUE(src_row$supports_ensemble[[1]])
        )
      )

      incProgress(0.15, detail = "Fetching point forecast")
      forecast_ts <- extract_timeseries(
        rv$forecast_ds,
        fvar,
        rv$click_lat,
        rv$click_lon,
        source_supports_ensemble = isTRUE(src_row$supports_ensemble[[1]]),
        lead_hours = isolate(input$lead_days) * 24
      )

      incProgress(0.45, detail = "Fetching point observations")
      obs_ts <- NULL
      if (isolate(input$data_mode) == "obs + forecast" && !is.null(rv$obs_ds) && nzchar(ovar)) {
        log_zarr_request(
          "point_observation",
          rv$obs_ds,
          ovar,
          extra = paste0(
            "lat=", sprintf("%.4f", rv$click_lat),
            ", lon=", sprintf("%.4f", rv$click_lon),
            ", lead_hours=", isolate(input$lead_days) * 24,
            ", supports_ensemble=FALSE"
          )
        )

        obs_ts <- extract_timeseries(
          rv$obs_ds,
          ovar,
          rv$click_lat,
          rv$click_lon,
          source_supports_ensemble = FALSE,
          lead_hours = isolate(input$lead_days) * 24
        )
      }

      if (!is.null(forecast_ts)) {
        forecast_ts$trace_df$value <- convert_units(forecast_ts$trace_df$value, canonical_id, isolate(input$units), registry)
        forecast_ts$pct_df <- forecast_ts$pct_df %>%
          mutate(across(starts_with("p"), ~convert_units(.x, canonical_id, isolate(input$units), registry)))
      }
      if (!is.null(obs_ts)) {
        obs_ts$trace_df$value <- convert_units(obs_ts$trace_df$value, canonical_id, isolate(input$units), registry)
      }
      rv$point_data <- list(forecast = forecast_ts, obs = obs_ts)

      incProgress(0.75, detail = "Fetching map raster")
      raster_df <- NULL
      if (!is.null(rv$bounds) && !any(vapply(rv$bounds, is.null, logical(1)))) {
        area <- abs((rv$bounds$north - rv$bounds$south) * (rv$bounds$east - rv$bounds$west))
        stride <- if (area > 1000) 6 else if (area > 300) 4 else if (area > 100) 2 else 1
        log_zarr_request(
          "map_raster",
          rv$forecast_ds,
          fvar,
          extra = paste0(
            "bounds=[N:", sprintf("%.3f", rv$bounds$north),
            ", S:", sprintf("%.3f", rv$bounds$south),
            ", W:", sprintf("%.3f", rv$bounds$west),
            ", E:", sprintf("%.3f", rv$bounds$east),
            "], area_deg2=", round(area, 2),
            ", stride=", stride,
            ", stat=", isolate(input$raster_stat),
            ", valid_hour=36h"
          )
        )

        raster_df <- extract_raster(
          rv$forecast_ds,
          fvar,
          bounds = rv$bounds,
          stat_name = isolate(input$raster_stat),
          source_supports_ensemble = isTRUE(src_row$supports_ensemble[[1]]),
          alpha_stride = stride,
          valid_hour = "36h"
        )
        if (!is.null(raster_df) && nrow(raster_df) > 0) {
          raster_df$value <- convert_units(raster_df$value, canonical_id, isolate(input$units), registry)
          rv$last_request_size <- paste0("point: ", ifelse(is.null(forecast_ts), 0, nrow(forecast_ts$trace_df)), " rows; raster: ", nrow(raster_df), " cells, stride=", stride)
        }
      }
      rv$raster_data <- raster_df

      rv$last_latency_ms <- as.numeric(difftime(Sys.time(), t0, units = "secs")) * 1000
      if (is.null(rv$last_request_size)) {
        rv$last_request_size <- paste0("point: ", ifelse(is.null(forecast_ts), 0, nrow(forecast_ts$trace_df)), " rows; raster: 0 cells")
      }
      incProgress(1, detail = "Complete")
      debug_log("refresh_data:done", "latency_ms=", round(rv$last_latency_ms, 2), ", request_size=", rv$last_request_size)
    })
  }, ignoreInit = FALSE)

  output$timeseries <- renderPlotly({
    pd <- rv$point_data
    validate(need(!is.null(pd) && !is.null(pd$forecast), "Forecast data unavailable for the selected source/variable."))
    validate(need(nrow(pd$forecast$trace_df) > 0, "Forecast data request returned no rows."))

    unit_label <- resolve_units(input$variable, input$units, registry)
    variable_label <- resolve_variable_label(input$variable, registry)

    p <- ggplot() +
      geom_line(
        data = pd$forecast$trace_df,
        aes(x = valid_time, y = value, group = ensemble_member),
        color = "#7db8ff",
        alpha = 0.4,
        linewidth = 0.4
      ) +
      geom_line(data = pd$forecast$pct_df, aes(x = valid_time, y = p25), color = "#1f78b4", linewidth = 0.8) +
      geom_line(data = pd$forecast$pct_df, aes(x = valid_time, y = p50), color = "#08306b", linewidth = 1.0) +
      geom_line(data = pd$forecast$pct_df, aes(x = valid_time, y = p75), color = "#1f78b4", linewidth = 0.8)

    if (!is.null(pd$obs)) {
      p <- p + geom_line(
        data = pd$obs$trace_df,
        aes(x = valid_time, y = value),
        color = "#e6550d",
        linewidth = 1.1
      )
    }

    p <- p +
      labs(
        title = paste0(variable_label, " @ ", rv$click_name),
        subtitle = "Forecast spaghetti + percentile lines (25/50/75)",
        x = ifelse(input$time_axis == "valid_time", "Valid time", "Lead time"),
        y = unit_label
      ) +
      theme_minimal(base_size = 12)

    ggplotly(p)
  })

  output$raster_plot <- renderPlot({
    req(rv$forecast_ds)
    rdf <- rv$raster_data
    if (is.null(rdf) || nrow(rdf) == 0) {
      plot.new()
      text(0.5, 0.5, "Raster will appear after clicking Refresh data.")
      return(invisible(NULL))
    }

    canonical_id <- input$variable
    ggplot(rdf, aes(x = longitude, y = latitude, fill = value)) +
      geom_raster(alpha = input$raster_alpha) +
      scale_fill_viridis_c(option = "C") +
      coord_quickmap(expand = FALSE) +
      theme_minimal(base_size = 12) +
      labs(
        title = paste("Raster:", input$raster_stat, "for", resolve_variable_label(canonical_id, registry)),
        subtitle = "Map request for one timestamp + bounded extent",
        x = "Longitude",
        y = "Latitude",
        fill = resolve_units(canonical_id, input$units, registry)
      )
  })

  output$metrics <- renderText({
    paste0(
      "Latency (ms): ", round(rv$last_latency_ms, 1), "\n",
      "Last request size: ", rv$last_request_size, "\n",
      "Source: ", input$source, "\n",
      "Clicked point: ", sprintf("%.3f, %.3f", rv$click_lat, rv$click_lon)
    )
  })
}

shinyApp(ui, server)
