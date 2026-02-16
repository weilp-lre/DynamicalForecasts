#!/usr/bin/env python3
"""Upload locally extracted test JSON files to S3.

Requires AWS credentials configured (env vars, shared credentials file, or IAM role).
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bucket", required=True, help="Target S3 bucket")
    p.add_argument("--prefix", default="dashboard/test/local_zarr", help="S3 key prefix")
    p.add_argument("--input-dir", type=Path, default=Path("AWS Deployment/test_output/local_zarr"))
    p.add_argument("--log-file", type=Path, default=Path("AWS Deployment/test_output/s3_upload.log"))
    return p.parse_args()


def logger_for(path: Path) -> logging.Logger:
    path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("s3_upload_test_data")
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
    logger = logger_for(args.log_file)

    files = sorted(args.input_dir.glob("*.json"))
    if not files:
        raise SystemExit(f"No JSON files found under {args.input_dir}")

    s3 = boto3.client("s3")

    uploaded = []
    started = time.perf_counter()

    for file_path in files:
        key = f"{args.prefix.rstrip('/')}/{file_path.name}"
        t0 = time.perf_counter()
        payload = file_path.read_bytes()
        s3.put_object(
            Bucket=args.bucket,
            Key=key,
            Body=payload,
            ContentType="application/json",
            Metadata={"uploaded_utc": datetime.now(timezone.utc).isoformat()},
        )
        dt = time.perf_counter() - t0
        logger.info("uploaded file=%s key=s3://%s/%s bytes=%s duration_s=%.3f", file_path.name, args.bucket, key, len(payload), dt)
        uploaded.append({"file": file_path.name, "key": key, "bytes": len(payload), "duration_seconds": round(dt, 3), "status": "ok"})

    total_dt = time.perf_counter() - started
    summary = {
        "bucket": args.bucket,
        "prefix": args.prefix,
        "file_count": len(uploaded),
        "total_duration_seconds": round(total_dt, 3),
        "uploaded": uploaded,
    }

    summary_path = args.input_dir / "s3_upload_summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    logger.info("upload complete file_count=%s total_duration_s=%.3f summary=%s", len(uploaded), total_dt, summary_path)


if __name__ == "__main__":
    main()
