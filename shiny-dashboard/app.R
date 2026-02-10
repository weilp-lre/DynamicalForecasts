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

extract_raster <- function(ds, var_name, bounds, stat_name = "single_member", source_supports_ensemble = TRUE, alpha_stride = 1, valid_hour = "36h") {
  if (is.null(ds) || !nzchar(var_name) || is.null(bounds)) return(NULL)

  lat_min <- bounds$south
  lat_max <- bounds$north
  lon_min <- bounds$west
  lon_max <- bounds$east

  da <- ds[[var_name]]
  dims <- names(reticulate::py_to_r(da$dims))

  if ("init_time" %in% dims) da <- da$isel(init_time = as.integer(-1))
  if ("lead_time" %in% dims) da <- da$sel(lead_time = valid_hour, method = "nearest")

  if (source_supports_ensemble && "ensemble_member" %in% dims) {
    if (stat_name == "single_member") {
      da <- da$isel(ensemble_member = as.integer(0))
    } else {
      q <- c(
        p10 = 0.10,
        p25 = 0.25,
        median = 0.50,
        p75 = 0.75,
        p90 = 0.90
      )
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

  grid_lats <- as.numeric(reticulate::py_to_r(da$coords$get("latitude")$values))
  lat_slice <- if (length(grid_lats) >= 2 && grid_lats[1] > grid_lats[2]) {
    py_builtins$slice(lat_max, lat_min)
  } else {
    py_builtins$slice(lat_min, lat_max)
  }
  lon_slice <- py_builtins$slice(lon_min, lon_max)

  clipped <- da$sel(latitude = lat_slice, longitude = lon_slice)

  lats <- as.numeric(reticulate::py_to_r(clipped$coords$get("latitude")$values))
  lons <- as.numeric(reticulate::py_to_r(clipped$coords$get("longitude")$values))
  vals <- reticulate::py_to_r(np$array(clipped$values))

  if (length(dim(vals)) == 2) {
    vals <- vals[seq(1, nrow(vals), by = alpha_stride), seq(1, ncol(vals), by = alpha_stride), drop = FALSE]
    lats <- lats[seq(1, length(lats), by = alpha_stride)]
    lons <- lons[seq(1, length(lons), by = alpha_stride)]
  }

  if (length(lats) == 0 || length(lons) == 0) return(NULL)

  expand.grid(longitude = lons, latitude = lats) %>%
    mutate(value = as.numeric(as.vector(t(vals))))
}

ui <- fluidPage(
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
      actionButton("refresh_raster", "Refresh raster"),
      actionButton("refresh_metadata", "Refresh metadata")
    ),
    mainPanel(
      textOutput("processing_status"),
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
    processing_label = "Idle"
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

  plot_data <- reactive({
    req(rv$forecast_ds)
    t0 <- Sys.time()

    sid <- input$source
    canonical_id <- input$variable
    src_row <- registry$sources[registry$sources$id == sid, ]

    fvar <- get_var_name(sid, canonical_id, "forecast")
    ovar <- get_var_name(sid, canonical_id, "obs")
    debug_log(
      "plot_data:config",
      "source=", sid,
      ", variable=", canonical_id,
      ", forecast_var=", fvar,
      ", obs_var=", ovar,
      ", lead_days=", input$lead_days,
      ", units=", input$units,
      ", data_mode=", input$data_mode
    )

    forecast_ts <- extract_timeseries(
      rv$forecast_ds,
      fvar,
      rv$click_lat,
      rv$click_lon,
      source_supports_ensemble = isTRUE(src_row$supports_ensemble[[1]]),
      lead_hours = input$lead_days * 24
    )

    obs_ts <- NULL
    if (input$data_mode == "obs + forecast" && !is.null(rv$obs_ds) && nzchar(ovar)) {
      obs_ts <- extract_timeseries(
        rv$obs_ds,
        ovar,
        rv$click_lat,
        rv$click_lon,
        source_supports_ensemble = FALSE,
        lead_hours = input$lead_days * 24
      )
    }

    if (!is.null(forecast_ts)) {
      debug_log("plot_data:forecast_ts", "rows=", nrow(forecast_ts$trace_df), ", members=", forecast_ts$n_members)
      forecast_ts$trace_df$value <- convert_units(forecast_ts$trace_df$value, canonical_id, input$units, registry)
      forecast_ts$pct_df <- forecast_ts$pct_df %>%
        mutate(across(starts_with("p"), ~convert_units(.x, canonical_id, input$units, registry)))
    }

    if (!is.null(obs_ts)) {
      debug_log("plot_data:obs_ts", "rows=", nrow(obs_ts$trace_df))
      obs_ts$trace_df$value <- convert_units(obs_ts$trace_df$value, canonical_id, input$units, registry)
    }

    elapsed <- as.numeric(difftime(Sys.time(), t0, units = "secs")) * 1000
    rv$last_latency_ms <- elapsed
    rv$last_request_size <- paste0("timeseries: ", ifelse(is.null(forecast_ts), 0, nrow(forecast_ts$trace_df)), " rows")
    debug_log("plot_data:done", "latency_ms=", round(elapsed, 2), ", request_size=", rv$last_request_size)

    list(forecast = forecast_ts, obs = obs_ts)
  })

  output$timeseries <- renderPlotly({
    pd <- plot_data()
    validate(need(!is.null(pd$forecast), "Forecast data unavailable for the selected source/variable."))

    unit_label <- resolve_units(input$variable, input$units, registry)

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
        title = paste0(registry$canonical_variables$label[registry$canonical_variables$id == input$variable], " @ ", rv$click_name),
        subtitle = "Forecast spaghetti + percentile lines (25/50/75)",
        x = ifelse(input$time_axis == "valid_time", "Valid time", "Lead time"),
        y = unit_label
      ) +
      theme_minimal(base_size = 12)

    ggplotly(p)
  })

  observeEvent(list(input$refresh_raster, input$map_bounds), {
    req(rv$forecast_ds)
    start_processing("Refreshing map raster")
    on.exit(finish_processing(), add = TRUE)

    sid <- isolate(input$source)
    canonical_id <- isolate(input$variable)
    src_row <- registry$sources[registry$sources$id == sid, ]
    debug_log(
      "raster:triggered",
      "source=", sid,
      ", variable=", canonical_id,
      ", stat=", isolate(input$raster_stat)
    )

    if (is.null(rv$bounds) || any(vapply(rv$bounds, is.null, logical(1)))) {
      showNotification("Map bounds unavailable. Move the map and click Refresh raster.", type = "warning")
      return()
    }

    area <- abs((rv$bounds$north - rv$bounds$south) * (rv$bounds$east - rv$bounds$west))
    stride <- if (area > 1000) 6 else if (area > 300) 4 else if (area > 100) 2 else 1
    debug_log("raster:bounds", "area=", round(area, 2), ", stride=", stride)

    t0 <- Sys.time()
    raster_df <- extract_raster(
      rv$forecast_ds,
      get_var_name(sid, canonical_id, "forecast"),
      bounds = rv$bounds,
      stat_name = isolate(input$raster_stat),
      source_supports_ensemble = isTRUE(src_row$supports_ensemble[[1]]),
      alpha_stride = stride,
      valid_hour = "36h"
    )

    if (is.null(raster_df) || nrow(raster_df) == 0) {
      debug_log("raster:empty", "No raster data returned for current map extent.")
      showNotification("No raster data in current map extent. Move map and click Refresh raster.", type = "warning")
      return()
    }

    raster_df$value <- convert_units(raster_df$value, canonical_id, isolate(input$units), registry)
    rv$last_latency_ms <- as.numeric(difftime(Sys.time(), t0, units = "secs")) * 1000
    rv$last_request_size <- paste0("raster: ", nrow(raster_df), " cells, stride=", stride)
    debug_log("raster:done", "latency_ms=", round(rv$last_latency_ms, 2), ", request_size=", rv$last_request_size)

    output$raster_plot <- renderPlot({
      ggplot(raster_df, aes(x = longitude, y = latitude, fill = value)) +
        geom_raster(alpha = isolate(input$raster_alpha)) +
        scale_fill_viridis_c(option = "C") +
        coord_quickmap(expand = FALSE) +
        theme_minimal(base_size = 12) +
        labs(
          title = paste("Raster:", isolate(input$raster_stat), "for", registry$canonical_variables$label[registry$canonical_variables$id == canonical_id]),
          subtitle = "Map extent based extraction + dynamic downsampling",
          x = "Longitude",
          y = "Latitude",
          fill = resolve_units(canonical_id, isolate(input$units), registry)
        )
    })
  }, ignoreInit = FALSE)

  output$raster_plot <- renderPlot({
    plot.new()
    text(0.5, 0.5, "Raster will appear after data is processed.")
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
