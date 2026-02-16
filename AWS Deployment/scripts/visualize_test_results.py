#!/usr/bin/env python3
"""Generate a simple visualization from extracted test JSON traces."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input-dir", type=Path, default=Path("AWS Deployment/test_output/local_zarr"))
    p.add_argument("--output", type=Path, default=Path("AWS Deployment/test_output/local_zarr/trace_preview.png"))
    p.add_argument("--max-series", type=int, default=6)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    files = sorted(args.input_dir.glob("Q*.json"))[: max(1, args.max_series)]
    if not files:
        raise SystemExit(f"No trace JSON files found under {args.input_dir}")

    plt.figure(figsize=(11, 5))

    for f in files:
        data = json.loads(f.read_text(encoding="utf-8"))
        values = data.get("values", [])
        qid = data.get("query_id", f.stem)
        sid = data.get("source_id", "unknown")
        if values:
            plt.plot(range(len(values)), values, label=f"{qid}-{sid}", linewidth=1.4)

    plt.title("Forecast trace preview from local test extraction")
    plt.xlabel("Time index")
    plt.ylabel("Value (native units)")
    plt.legend(loc="best", fontsize=8)
    plt.grid(alpha=0.25)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(args.output, dpi=150)
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
