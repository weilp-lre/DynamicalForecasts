#!/usr/bin/env python3
"""Benchmark query responsiveness for hosted forecast zarr datasets.

Runs a fixed suite of query shapes (10 by default) over configured forecast sources,
then emits CSV + markdown summary suitable for dashboard tuning.
"""

from __future__ import annotations

import argparse
import json
import math
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
import xarray as xr


LAT_CANDIDATES = ("latitude", "lat", "y")
LON_CANDIDATES = ("longitude", "lon", "x")
TIME_CANDIDATES = ("valid_time", "time", "forecast_time")
ENSEMBLE_CANDIDATES = ("number", "member", "ensemble_member")


@dataclass(frozen=True)
class QuerySpec:
    query_id: str
    description: str
    source_id: str
    variable_alias: str
    region_fraction: float
    time_fraction: float
    ensemble_fraction: float


class BenchmarkError(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--registry",
        type=Path,
        default=Path("shiny-dashboard/config/source_registry.json"),
        help="Path to source registry JSON file.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("shiny-dashboard/data/benchmarks"),
        help="Directory where CSV and markdown summary will be written.",
    )
    parser.add_argument(
        "--queries",
        type=int,
        default=10,
        help="How many benchmark queries to run (max 10 in current suite).",
    )
    return parser.parse_args()


def pick_dim(dataset: xr.Dataset, candidates: tuple[str, ...], label: str) -> str:
    for candidate in candidates:
        if candidate in dataset.dims:
            return candidate
    raise BenchmarkError(f"Could not find {label} dimension in dataset dims {tuple(dataset.dims)}")


def choose_var(dataset: xr.Dataset, preferred: str) -> str:
    if preferred in dataset.data_vars:
        return preferred
    if not dataset.data_vars:
        raise BenchmarkError("Dataset has no data variables.")
    return next(iter(dataset.data_vars))


def fraction_to_count(total: int, fraction: float) -> int:
    if total <= 0:
        return 0
    return max(1, min(total, int(math.ceil(total * fraction))))


def centered_slice(total: int, count: int) -> slice:
    if total <= count:
        return slice(0, total)
    start = (total - count) // 2
    end = start + count
    return slice(start, end)


def estimate_nbytes(da: xr.DataArray) -> int:
    size = da.size
    itemsize = getattr(da.dtype, "itemsize", 8)
    return int(size * itemsize)


def build_query_suite(registry: dict[str, Any]) -> list[QuerySpec]:
    sources = [source_id for source_id, payload in registry["sources"].items() if payload.get("forecast_url")]
    if not sources:
        raise BenchmarkError("No forecast sources found in registry.")

    aliases = list(registry["canonical_variables"].keys())
    if not aliases:
        raise BenchmarkError("No canonical variables found in registry.")

    # Alternate sources so the suite exercises both endpoints.
    src_a = sources[0]
    src_b = sources[1] if len(sources) > 1 else sources[0]
    var = aliases[0]

    return [
        QuerySpec("Q01", "Point sample, single time, 1 member", src_a, var, 0.01, 0.01, 0.01),
        QuerySpec("Q02", "Point sample, ~1 day window, 1 member", src_b, var, 0.01, 0.04, 0.01),
        QuerySpec("Q03", "Point sample, ~7 day window, 1 member", src_a, var, 0.01, 0.20, 0.01),
        QuerySpec("Q04", "Point sample, full time, all members", src_b, var, 0.01, 1.00, 1.00),
        QuerySpec("Q05", "Small regional box, single time, all members", src_a, var, 0.05, 0.01, 1.00),
        QuerySpec("Q06", "Small regional box, ~3 day window, all members", src_b, var, 0.05, 0.10, 1.00),
        QuerySpec("Q07", "Medium regional box, ~3 day window, all members", src_a, var, 0.15, 0.10, 1.00),
        QuerySpec("Q08", "Medium regional box, ~7 day window, all members", src_b, var, 0.15, 0.20, 1.00),
        QuerySpec("Q09", "Large regional box, ~7 day window, all members", src_a, var, 0.35, 0.20, 1.00),
        QuerySpec("Q10", "Large regional box, full time, all members", src_b, var, 0.35, 1.00, 1.00),
    ]


def run_one_query(ds: xr.Dataset, source_id: str, var_name: str, spec: QuerySpec) -> dict[str, Any]:
    lat_dim = pick_dim(ds, LAT_CANDIDATES, "latitude")
    lon_dim = pick_dim(ds, LON_CANDIDATES, "longitude")
    time_dim = pick_dim(ds, TIME_CANDIDATES, "time")
    ens_dim = None
    for candidate in ENSEMBLE_CANDIDATES:
        if candidate in ds.dims:
            ens_dim = candidate
            break

    lat_count = fraction_to_count(ds.sizes[lat_dim], spec.region_fraction)
    lon_count = fraction_to_count(ds.sizes[lon_dim], spec.region_fraction)
    time_count = fraction_to_count(ds.sizes[time_dim], spec.time_fraction)

    indexers: dict[str, Any] = {
        lat_dim: centered_slice(ds.sizes[lat_dim], lat_count),
        lon_dim: centered_slice(ds.sizes[lon_dim], lon_count),
        time_dim: centered_slice(ds.sizes[time_dim], time_count),
    }

    if ens_dim is not None:
        ens_count = fraction_to_count(ds.sizes[ens_dim], spec.ensemble_fraction)
        indexers[ens_dim] = centered_slice(ds.sizes[ens_dim], ens_count)
    else:
        ens_count = 0

    query = ds[var_name].isel(**indexers)

    started = time.perf_counter()
    loaded = query.load()
    duration = time.perf_counter() - started

    nbytes = estimate_nbytes(loaded)
    mib = nbytes / (1024 ** 2)
    throughput = mib / duration if duration > 0 else float("inf")

    return {
        "query_id": spec.query_id,
        "description": spec.description,
        "source_id": source_id,
        "variable": var_name,
        "duration_seconds": round(duration, 3),
        "output_size_bytes": nbytes,
        "output_size_mib": round(mib, 3),
        "elements_returned": int(loaded.size),
        "throughput_mib_per_second": round(throughput, 3),
        "lat_points": lat_count,
        "lon_points": lon_count,
        "time_steps": time_count,
        "ensemble_members": ens_count,
        "status": "ok",
    }




def markdown_table(frame: pd.DataFrame, columns: list[str]) -> str:
    subset = frame[columns].fillna("")
    header = "| " + " | ".join(columns) + " |"
    sep = "| " + " | ".join(["---"] * len(columns)) + " |"
    rows = ["| " + " | ".join(str(row[col]) for col in columns) + " |" for _, row in subset.iterrows()]
    return "\n".join([header, sep, *rows])

def summarize(results: pd.DataFrame) -> str:
    ok = results[results["status"] == "ok"].copy()
    failed = results[results["status"] != "ok"].copy()
    lines = ["# Hosted Zarr Query Benchmark Summary", ""]

    if ok.empty:
        lines.append("No successful queries were recorded.")
        if not failed.empty:
            lines.extend(["", "## Failed queries", "", markdown_table(failed, ["query_id", "source_id", "status"])])
        return "\n".join(lines)

    mean_latency = ok["duration_seconds"].mean()
    p95_latency = ok["duration_seconds"].quantile(0.95)
    mean_throughput = ok["throughput_mib_per_second"].mean()

    lines.extend(
        [
            f"- Successful queries: {len(ok)} / {len(results)}",
            f"- Mean latency: {mean_latency:.3f} s",
            f"- P95 latency: {p95_latency:.3f} s",
            f"- Mean throughput: {mean_throughput:.3f} MiB/s",
            "",
            "## Slowest queries",
            "",
        ]
    )

    slow = ok.sort_values("duration_seconds", ascending=False).head(3)
    for _, row in slow.iterrows():
        lines.append(
            f"- {row['query_id']} ({row['source_id']}): {row['duration_seconds']:.3f}s, "
            f"{row['output_size_mib']:.3f} MiB, {row['description']}"
        )

    lines.extend(["", "## Full results", "", markdown_table(ok, ["query_id", "source_id", "variable", "duration_seconds", "output_size_mib", "throughput_mib_per_second", "description"])])
    return "\n".join(lines)


def main() -> None:
    args = parse_args()

    with args.registry.open("r", encoding="utf-8") as fh:
        registry = json.load(fh)

    suite = build_query_suite(registry)
    requested_queries = max(1, min(args.queries, len(suite)))
    suite = suite[:requested_queries]

    args.output_dir.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    datasets: dict[str, xr.Dataset] = {}

    for spec in suite:
        source_payload = registry["sources"][spec.source_id]
        url = source_payload["forecast_url"]
        var_hint = registry["canonical_variables"][spec.variable_alias]["forecast_var_by_source"].get(spec.source_id, "")

        try:
            if spec.source_id not in datasets:
                datasets[spec.source_id] = xr.open_zarr(url, chunks=None, decode_timedelta=True)
            ds = datasets[spec.source_id]
            var_name = choose_var(ds, var_hint)
            result = run_one_query(ds, spec.source_id, var_name, spec)
        except Exception as exc:  # noqa: BLE001 - benchmark should continue on failure.
            result = {
                "query_id": spec.query_id,
                "description": spec.description,
                "source_id": spec.source_id,
                "variable": var_hint or "unknown",
                "duration_seconds": None,
                "output_size_bytes": None,
                "output_size_mib": None,
                "elements_returned": None,
                "throughput_mib_per_second": None,
                "lat_points": None,
                "lon_points": None,
                "time_steps": None,
                "ensemble_members": None,
                "status": f"error: {exc}",
            }
        results.append(result)

    frame = pd.DataFrame(results)
    csv_path = args.output_dir / "zarr_query_benchmark_results.csv"
    summary_path = args.output_dir / "zarr_query_benchmark_summary.md"

    frame.to_csv(csv_path, index=False)
    summary_path.write_text(summarize(frame), encoding="utf-8")

    print(f"Wrote {csv_path}")
    print(f"Wrote {summary_path}")


if __name__ == "__main__":
    main()
