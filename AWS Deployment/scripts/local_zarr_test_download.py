#!/usr/bin/env python3
"""Benchmark lightweight point queries against forecast + observed Zarr stores.

Targets two realistic Shiny-style payloads:
1) Forecast traces: 14 days on a 6h cadence at one point, per variable/source/trace.
2) Observations: 7 days of preceding data on a 6h cadence at the same point for one variable.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import xarray as xr

LAT_CANDIDATES = ("latitude", "lat", "y")
LON_CANDIDATES = ("longitude", "lon", "x")
TIME_CANDIDATES = ("valid_time", "time", "forecast_time")
INIT_TIME_CANDIDATES = ("init_time", "forecast_reference_time", "reference_time")
LEAD_TIME_CANDIDATES = ("lead_time", "step", "forecast_period")
ENSEMBLE_CANDIDATES = ("ensemble_member", "member", "number")


@dataclass(frozen=True)
class QuerySpec:
    query_id: str
    mode: str
    source_id: str
    variable_alias: str
    lat_index_fraction: float
    lon_index_fraction: float
    window_hours: int
    interval_hours: int
    trace_index: int = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, default=Path("shiny-dashboard/config/source_registry.json"))
    parser.add_argument("--output-dir", type=Path, default=Path("AWS Deployment/test_output/local_zarr"))
    parser.add_argument("--log-file", type=Path, default=Path("AWS Deployment/test_output/local_zarr/query_performance.log"))
    parser.add_argument("--interval-hours", type=int, default=6)
    parser.add_argument("--forecast-days", type=int, default=14)
    parser.add_argument("--obs-days", type=int, default=7)
    parser.add_argument(
        "--trace-indices",
        type=int,
        nargs="+",
        default=[0],
        help="Forecast trace/member indices to benchmark (one query per index where supported).",
    )
    parser.add_argument("--lat-fraction", type=float, default=0.5)
    parser.add_argument("--lon-fraction", type=float, default=0.5)
    return parser.parse_args()


def setup_logger(log_file: Path) -> logging.Logger:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("local_zarr_test_download")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def pick_dim(dataset: xr.Dataset, candidates: tuple[str, ...], label: str) -> str:
    for candidate in candidates:
        if candidate in dataset.dims:
            return candidate
    raise ValueError(f"Could not find {label} dimension in dataset dims {tuple(dataset.dims)}")


def pick_optional_dim(dataset: xr.Dataset, candidates: tuple[str, ...]) -> str | None:
    for candidate in candidates:
        if candidate in dataset.dims:
            return candidate
    return None


def choose_var(dataset: xr.Dataset, preferred: str) -> str:
    if preferred and preferred in dataset.data_vars:
        return preferred
    if not dataset.data_vars:
        raise ValueError("Dataset has no data variables")
    return next(iter(dataset.data_vars))


def clamp_index(total: int, fraction: float) -> int:
    if total <= 1:
        return 0
    idx = int(round((total - 1) * fraction))
    return max(0, min(total - 1, idx))


def _ds_for_da(da: xr.DataArray) -> xr.Dataset:
    return da.to_dataset(name="_tmp")


def select_at_6h_points(da: xr.DataArray, dim: str, window_hours: int, interval_hours: int, anchor: str) -> xr.DataArray:
    coord = da.coords[dim]
    values = coord.values
    if values.size == 0:
        return da

    dtype_kind = np.asarray(values).dtype.kind
    if dtype_kind == "m":
        max_value = values.max()
        targets = np.arange(0, window_hours + interval_hours, interval_hours, dtype="timedelta64[h]")
        targets = targets[targets <= max_value]
        if targets.size == 0:
            return da.isel({dim: slice(0, 0)})
        return da.sel({dim: targets}, method="nearest")

    if dtype_kind == "M":
        end = values.max() if anchor == "end" else values.min() + np.timedelta64(window_hours, "h")
        start = end - np.timedelta64(window_hours, "h")
        targets = np.arange(start, end + np.timedelta64(interval_hours, "h"), np.timedelta64(interval_hours, "h"))
        targets = targets[(targets >= values.min()) & (targets <= values.max())]
        if targets.size == 0:
            return da.isel({dim: slice(0, 0)})
        return da.sel({dim: targets}, method="nearest")

    requested_points = (window_hours // interval_hours) + 1
    stride = max(1, int(round(da.sizes[dim] / max(requested_points, 1))))
    sliced = da.isel({dim: slice(None, None, stride)})
    return sliced.isel({dim: slice(0, requested_points)})


def select_forecast_trace(da: xr.DataArray, spec: QuerySpec) -> xr.DataArray:
    ds = _ds_for_da(da)
    init_dim = pick_optional_dim(ds, INIT_TIME_CANDIDATES)
    if init_dim:
        da = da.isel({init_dim: -1})

    ds = _ds_for_da(da)
    ens_dim = pick_optional_dim(ds, ENSEMBLE_CANDIDATES)
    if ens_dim:
        member_index = max(0, min(da.sizes[ens_dim] - 1, spec.trace_index))
        da = da.isel({ens_dim: member_index})

    ds = _ds_for_da(da)
    lead_dim = pick_optional_dim(ds, LEAD_TIME_CANDIDATES)
    if lead_dim:
        return select_at_6h_points(da, lead_dim, spec.window_hours, spec.interval_hours, anchor="start")

    time_dim = pick_optional_dim(ds, TIME_CANDIDATES)
    if time_dim:
        return select_at_6h_points(da, time_dim, spec.window_hours, spec.interval_hours, anchor="start")

    return da


def select_observed_series(da: xr.DataArray, spec: QuerySpec) -> xr.DataArray:
    ds = _ds_for_da(da)
    time_dim = pick_optional_dim(ds, TIME_CANDIDATES)
    if not time_dim:
        raise ValueError("Observed query requires a time-like dimension")
    return select_at_6h_points(da, time_dim, spec.window_hours, spec.interval_hours, anchor="end")


def to_time_strings(da: xr.DataArray) -> list[str]:
    for coord_name in TIME_CANDIDATES + LEAD_TIME_CANDIDATES:
        if coord_name in da.coords:
            return [str(v) for v in da.coords[coord_name].values]
    return [str(i) for i in range(da.size)]


def run_query(
    ds: xr.Dataset,
    spec: QuerySpec,
    var_name: str,
    output_dir: Path,
    logger: logging.Logger,
) -> dict[str, Any]:
    lat_dim = pick_dim(ds, LAT_CANDIDATES, "latitude")
    lon_dim = pick_dim(ds, LON_CANDIDATES, "longitude")

    lat_idx = clamp_index(ds.sizes[lat_dim], spec.lat_index_fraction)
    lon_idx = clamp_index(ds.sizes[lon_dim], spec.lon_index_fraction)

    da = ds[var_name].isel({lat_dim: lat_idx, lon_dim: lon_idx})
    if spec.mode == "forecast":
        da = select_forecast_trace(da, spec)
    elif spec.mode == "obs":
        da = select_observed_series(da, spec)
    else:
        raise ValueError(f"Unknown query mode: {spec.mode}")

    started = time.perf_counter()
    loaded = da.load()
    duration = time.perf_counter() - started

    values = [float(v) if v == v else None for v in loaded.values.ravel()]
    result = {
        "query_id": spec.query_id,
        "mode": spec.mode,
        "source_id": spec.source_id,
        "variable": var_name,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "lat_index": lat_idx,
        "lon_index": lon_idx,
        "dims": {k: int(v) for k, v in loaded.sizes.items()},
        "time_axis": to_time_strings(loaded),
        "values": values,
        "request": asdict(spec),
        "metrics": {
            "duration_seconds": round(duration, 3),
            "elements_returned": int(loaded.size),
            "estimated_bytes": int(loaded.size * loaded.dtype.itemsize),
            "requested_points": int((spec.window_hours // spec.interval_hours) + 1),
        },
        "status": "ok",
    }

    out = output_dir / f"{spec.query_id}_{spec.mode}_{spec.source_id}.json"
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")

    logger.info(
        "query=%s mode=%s source=%s duration_s=%.3f elements=%s bytes=%s status=ok",
        spec.query_id,
        spec.mode,
        spec.source_id,
        duration,
        loaded.size,
        loaded.size * loaded.dtype.itemsize,
    )
    return result


def build_suite(registry: dict[str, Any], args: argparse.Namespace) -> list[QuerySpec]:
    forecast_hours = args.forecast_days * 24
    obs_hours = args.obs_days * 24

    suite: list[QuerySpec] = []
    query_counter = 1

    for source_id in registry["sources"]:
        for variable_alias in registry["canonical_variables"]:
            for trace_index in args.trace_indices:
                suite.append(
                    QuerySpec(
                        query_id=f"Q{query_counter:02d}",
                        mode="forecast",
                        source_id=source_id,
                        variable_alias=variable_alias,
                        lat_index_fraction=args.lat_fraction,
                        lon_index_fraction=args.lon_fraction,
                        window_hours=forecast_hours,
                        interval_hours=args.interval_hours,
                        trace_index=trace_index,
                    )
                )
                query_counter += 1

    obs_var_alias = next(
        (
            alias
            for alias, payload in registry["canonical_variables"].items()
            if any(bool(v) for v in payload.get("obs_var_by_source", {}).values())
        ),
        None,
    )
    if obs_var_alias:
        for source_id, source_payload in registry["sources"].items():
            if source_payload.get("obs_url") and registry["canonical_variables"][obs_var_alias]["obs_var_by_source"].get(source_id):
                suite.append(
                    QuerySpec(
                        query_id=f"Q{query_counter:02d}",
                        mode="obs",
                        source_id=source_id,
                        variable_alias=obs_var_alias,
                        lat_index_fraction=args.lat_fraction,
                        lon_index_fraction=args.lon_fraction,
                        window_hours=obs_hours,
                        interval_hours=args.interval_hours,
                        trace_index=0,
                    )
                )
                query_counter += 1
                break

    return suite


def main() -> None:
    args = parse_args()
    logger = setup_logger(args.log_file)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    registry = json.loads(args.registry.read_text(encoding="utf-8"))
    suite = build_suite(registry, args)

    datasets: dict[tuple[str, str], xr.Dataset] = {}
    summary: list[dict[str, Any]] = []

    logger.info("starting local zarr lightweight run query_count=%s", len(suite))

    for spec in suite:
        payload = registry["sources"][spec.source_id]
        var_meta = registry["canonical_variables"][spec.variable_alias]
        if spec.mode == "forecast":
            url = payload.get("forecast_url", "")
            var_hint = var_meta.get("forecast_var_by_source", {}).get(spec.source_id, "")
        else:
            url = payload.get("obs_url", "")
            var_hint = var_meta.get("obs_var_by_source", {}).get(spec.source_id, "")

        try:
            if not url:
                raise ValueError(f"No URL configured for mode={spec.mode}")

            ds_key = (spec.mode, spec.source_id)
            if ds_key not in datasets:
                datasets[ds_key] = xr.open_zarr(url, decode_timedelta=True, chunks=None)

            ds = datasets[ds_key]
            var_name = choose_var(ds, var_hint)
            summary.append(run_query(ds, spec, var_name, args.output_dir, logger))
        except Exception as exc:  # noqa: BLE001
            logger.exception("query=%s mode=%s source=%s status=failed error=%s", spec.query_id, spec.mode, spec.source_id, exc)
            summary.append(
                {
                    "query_id": spec.query_id,
                    "mode": spec.mode,
                    "source_id": spec.source_id,
                    "request": asdict(spec),
                    "status": f"failed: {exc}",
                }
            )

    summary_path = args.output_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    logger.info("completed local zarr lightweight run summary_path=%s", summary_path)


if __name__ == "__main__":
    main()
