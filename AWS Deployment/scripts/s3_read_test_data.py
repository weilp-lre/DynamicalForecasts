#!/usr/bin/env python3
"""Read test JSON artifacts from S3 and write a local verification summary."""

from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path

import boto3


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bucket", required=True)
    p.add_argument("--prefix", default="dashboard/test/local_zarr")
    p.add_argument("--max-files", type=int, default=20)
    p.add_argument("--output-dir", type=Path, default=Path("AWS Deployment/test_output/s3_read"))
    p.add_argument("--log-file", type=Path, default=Path("AWS Deployment/test_output/s3_read.log"))
    return p.parse_args()


def mk_logger(path: Path) -> logging.Logger:
    path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("s3_read_test_data")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fh = logging.FileHandler(path, encoding="utf-8")
    fh.setFormatter(fmt)
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger


def main() -> None:
    args = parse_args()
    logger = mk_logger(args.log_file)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    s3 = boto3.client("s3")

    resp = s3.list_objects_v2(Bucket=args.bucket, Prefix=args.prefix)
    contents = resp.get("Contents", [])
    keys = [obj["Key"] for obj in contents if obj["Key"].endswith(".json")][: max(1, args.max_files)]

    if not keys:
        raise SystemExit(f"No JSON objects found for s3://{args.bucket}/{args.prefix}")

    summary = []

    for key in keys:
        t0 = time.perf_counter()
        obj = s3.get_object(Bucket=args.bucket, Key=key)
        body = obj["Body"].read().decode("utf-8")
        data = json.loads(body)
        dt = time.perf_counter() - t0

        local_name = key.split("/")[-1]
        local_path = args.output_dir / local_name
        local_path.write_text(json.dumps(data, indent=2), encoding="utf-8")

        item = {
            "key": key,
            "duration_seconds": round(dt, 3),
            "bytes": len(body.encode("utf-8")),
            "status": data.get("status", "unknown"),
            "query_id": data.get("query_id", "n/a"),
            "source_id": data.get("source_id", "n/a"),
        }
        summary.append(item)

        logger.info(
            "read key=s3://%s/%s duration_s=%.3f bytes=%s query=%s status=%s",
            args.bucket,
            key,
            dt,
            item["bytes"],
            item["query_id"],
            item["status"],
        )

    out = {
        "bucket": args.bucket,
        "prefix": args.prefix,
        "files_read": len(summary),
        "items": summary,
    }
    out_path = args.output_dir / "s3_read_summary.json"
    out_path.write_text(json.dumps(out, indent=2), encoding="utf-8")
    logger.info("read complete files=%s summary=%s", len(summary), out_path)


if __name__ == "__main__":
    main()
