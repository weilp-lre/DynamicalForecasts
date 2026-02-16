#!/usr/bin/env python3
"""Run local Zarr query smoke tests and write extracted traces to JSON.

This script performs up to 10 point-trace queries from forecast Zarr sources defined
in shiny-dashboard/config/source_registry.json and stores results under
AWS Deployment/test_output/local_zarr/.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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
    source_id: str
    variable_alias: str
    lat_index_fraction: float
    lon_index_fraction: float
    lead_count: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry", type=Path, default=Path("shiny-dashboard/config/source_registry.json"))
    parser.add_argument("--output-dir", type=Path, default=Path("AWS Deployment/test_output/local_zarr"))
    parser.add_argument("--max-queries", type=int, default=10)
    parser.add_argument(
        "--log-file",
        type=Path,
        default=Path("AWS Deployment/test_output/local_zarr/query_performance.log"),
    )
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


def build_suite(registry: dict[str, Any]) -> list[QuerySpec]:
    sources = [sid for sid, payload in registry["sources"].items() if payload.get("forecast_url")]
    aliases = list(registry["canonical_variables"].keys())
    if not sources or not aliases:
        raise ValueError("Registry missing sources or canonical variables")

    src_a = sources[0]
    src_b = sources[1] if len(sources) > 1 else sources[0]
    var = aliases[0]

    return [
        QuerySpec("Q01", src_a, var, 0.50, 0.50, 6),
        QuerySpec("Q02", src_b, var, 0.45, 0.55, 6),
        QuerySpec("Q03", src_a, var, 0.40, 0.60, 12),
        QuerySpec("Q04", src_b, var, 0.35, 0.65, 12),
        QuerySpec("Q05", src_a, var, 0.30, 0.70, 24),
        QuerySpec("Q06", src_b, var, 0.25, 0.75, 24),
        QuerySpec("Q07", src_a, var, 0.60, 0.40, 24),
        QuerySpec("Q08", src_b, var, 0.65, 0.35, 24),
        QuerySpec("Q09", src_a, var, 0.70, 0.30, 48),
        QuerySpec("Q10", src_b, var, 0.75, 0.25, 48),
    ]


def clamp_index(total: int, fraction: float) -> int:
    if total <= 1:
        return 0
    idx = int(round((total - 1) * fraction))
    return max(0, min(total - 1, idx))


def _ds_for_da(da: xr.DataArray) -> xr.Dataset:
    return da.to_dataset(name="_tmp")


def select_trace(da: xr.DataArray, lead_count: int) -> xr.DataArray:
    ds = _ds_for_da(da)
    init_dim = pick_optional_dim(ds, INIT_TIME_CANDIDATES)
    if init_dim:
        da = da.isel({init_dim: -1})

    ds = _ds_for_da(da)
    lead_dim = pick_optional_dim(ds, LEAD_TIME_CANDIDATES)
    if lead_dim:
        n = min(da.sizes[lead_dim], max(1, lead_count))
        da = da.isel({lead_dim: slice(0, n)})

    ds = _ds_for_da(da)
    ens_dim = pick_optional_dim(ds, ENSEMBLE_CANDIDATES)
    if ens_dim:
        da = da.isel({ens_dim: 0})

    return da


def to_time_strings(da: xr.DataArray) -> list[str]:
    for coord_name in TIME_CANDIDATES + LEAD_TIME_CANDIDATES:
        if coord_name in da.coords:
            return [str(v) for v in da.coords[coord_name].values]
    return [str(i) for i in range(da.size)]


def run_query(ds: xr.Dataset, spec: QuerySpec, var_name: str, output_dir: Path, logger: logging.Logger) -> dict[str, Any]:
    lat_dim = pick_dim(ds, LAT_CANDIDATES, "latitude")
    lon_dim = pick_dim(ds, LON_CANDIDATES, "longitude")

    lat_idx = clamp_index(ds.sizes[lat_dim], spec.lat_index_fraction)
    lon_idx = clamp_index(ds.sizes[lon_dim], spec.lon_index_fraction)

    da = ds[var_name].isel({lat_dim: lat_idx, lon_dim: lon_idx})
    da = select_trace(da, lead_count=spec.lead_count)

    started = time.perf_counter()
    loaded = da.load()
    duration = time.perf_counter() - started

    values = [float(v) if v == v else None for v in loaded.values.ravel()]
    result = {
        "query_id": spec.query_id,
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
        },
        "status": "ok",
    }

    out = output_dir / f"{spec.query_id}_{spec.source_id}.json"
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")

    logger.info(
        "query=%s source=%s duration_s=%.3f elements=%s bytes=%s status=ok",
        spec.query_id,
        spec.source_id,
        duration,
        loaded.size,
        loaded.size * loaded.dtype.itemsize,
    )
    return result


def main() -> None:
    args = parse_args()
    logger = setup_logger(args.log_file)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    registry = json.loads(args.registry.read_text(encoding="utf-8"))
    suite = build_suite(registry)[: max(1, min(args.max_queries, 10))]

    datasets: dict[str, xr.Dataset] = {}
    summary: list[dict[str, Any]] = []

    logger.info("starting local zarr smoke run query_count=%s", len(suite))

    for spec in suite:
        payload = registry["sources"][spec.source_id]
        var_hint = registry["canonical_variables"][spec.variable_alias]["forecast_var_by_source"].get(spec.source_id, "")

        try:
            if spec.source_id not in datasets:
                datasets[spec.source_id] = xr.open_zarr(payload["forecast_url"], decode_timedelta=True, chunks=None)

            ds = datasets[spec.source_id]
            var_name = choose_var(ds, var_hint)
            summary.append(run_query(ds, spec, var_name, args.output_dir, logger))
        except Exception as exc:  # noqa: BLE001
            logger.exception("query=%s source=%s status=failed error=%s", spec.query_id, spec.source_id, exc)
            summary.append(
                {
                    "query_id": spec.query_id,
                    "source_id": spec.source_id,
                    "request": asdict(spec),
                    "status": f"failed: {exc}",
                }
            )

    summary_path = args.output_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    logger.info("completed local zarr smoke run summary_path=%s", summary_path)


if __name__ == "__main__":
    main()
