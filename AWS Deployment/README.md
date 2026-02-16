# AWS Deployment Architecture for Dynamical Forecast Dashboard

This document describes a production-ready architecture for running a **responsive Shiny forecast dashboard** backed by **pre-extracted forecast data** in AWS.

It is written so a future Codex implementation pass can use this as an execution blueprint.

---

## 1) Target End-State (Functional Goals)

### UI capabilities required

1. **Highly responsive Shiny dashboard**
   - Reads pre-extracted, compact forecast artifacts (not raw full-cube queries at interaction time).
   - Mobile-friendly latency target for typical actions.

2. **Core variables**
   - Temperature
   - Precipitation
   - Snow
   - Wind speed
   - Plus a data dictionary mechanism to add variables later without major code rewrites.

3. **Multi-ensemble selection + boxplots**
   - Select forecast source(s) (e.g., GEFS, ECMWF ENS).
   - Display per-time distribution summaries as boxplots and percentile overlays.

4. **Separate map workflow to add forecast points**
   - A dedicated map tab/panel for selecting a new location.
   - User flow: click map -> preview nearest grid point -> enter point name -> confirm save.

5. **Status dashboard at app load**
   - Last successful extraction timestamp.
   - Current number of forecast files/objects in S3 bucket.
   - Optional estimated monthly storage trend to prevent cost surprises.

---

## 2) Recommended AWS Architecture

## 2.1 High-level components

1. **Storage layer (S3)**
   - Primary store for extracted forecast artifacts and metadata.
   - Versioned bucket with lifecycle policies.

2. **Scheduled extraction layer (AWS Batch or ECS Fargate Scheduled Task)**
   - Python/R extraction job pulls source forecast data.
   - Produces compact point-level and regional artifacts.
   - Writes outputs + manifest/status JSON to S3.

3. **Metadata/state layer (DynamoDB, optional but recommended)**
   - Stores user-defined forecast points and app configuration state.
   - Useful for quick writes from Shiny (e.g., add static location).

4. **API layer for controlled writes (API Gateway + Lambda)**
   - Endpoints to add/edit points safely.
   - Performs validation, deduplication, and audit logging.

5. **Dashboard runtime (Shiny)**
   - Hosted on shinyapps.io initially, or AWS-hosted later.
   - Reads artifacts from S3 (or CloudFront).
   - Calls API for point management if write-back is enabled.

6. **Monitoring and cost controls (CloudWatch + Budgets)**
   - Extraction success/failure metrics.
   - S3 object count and storage metrics.
   - Budget alarms to avoid runaway cost.

---

## 2.2 Data flow

1. Scheduler triggers extraction job (e.g., every 6h or daily).
2. Job reads latest source forecast stores.
3. Job computes and writes:
   - Point time-series artifacts.
   - Ensemble summary artifacts (boxplot-ready quantiles/distributions).
   - Optional map-ready aggregates.
4. Job updates manifest/status files in S3.
5. Shiny app loads only compact artifacts for UI responsiveness.
6. User optionally adds location via map -> API -> DynamoDB/S3 config update.

---

## 3) S3 Layout (Proposed)

Use a clear, partitioned path strategy:

```text
s3://<bucket>/
  forecasts/
    source=<source_id>/
      variable=<variable_id>/
        init_date=<YYYY-MM-DD>/
          points.parquet
          ensemble_summary.parquet
          map_summary.parquet
  dashboard/
    status/
      extraction_status.json
      inventory_status.json
    config/
      source_registry.json
      canonical_variables.json
      pre_canned_points.json
  manifests/
    latest.json
    run_id=<timestamp>.json
  logs/
    extraction/
```

### Notes
- Prefer **Parquet** for dense tabular outputs (fast + compact).
- Keep a small **JSON status file** for quick app startup metrics.
- Enable S3 versioning and lifecycle retention (e.g., keep 30–90 days for high-volume artifacts).

---

## 4) Data Product Contracts (for Codex implementation)

Define stable schemas so app and pipeline can evolve independently.

## 4.1 `extraction_status.json`

```json
{
  "last_successful_extract_utc": "2026-02-16T20:30:00Z",
  "run_id": "20260216T203000Z",
  "sources_processed": ["gefs", "ecmwf_ifs_ens"],
  "variables_processed": ["t2m", "precip_surface", "snow_depth", "wind_speed_10m"],
  "errors": []
}
```

## 4.2 `inventory_status.json`

```json
{
  "bucket": "my-forecast-artifacts",
  "object_count": 12345,
  "total_size_bytes": 9876543210,
  "estimated_monthly_storage_usd": 2.73,
  "last_inventory_update_utc": "2026-02-16T20:35:00Z"
}
```

## 4.3 Point timeseries artifact columns

- `source_id`
- `variable_id`
- `point_id`
- `point_name`
- `init_time`
- `valid_time`
- `lead_hours`
- `ensemble_member`
- `value`
- `units_native`

## 4.4 Ensemble summary artifact columns (boxplot-ready)

- `source_id`
- `variable_id`
- `point_id`
- `init_time`
- `valid_time`
- `lead_hours`
- `min`
- `q10`
- `q25`
- `median`
- `q75`
- `q90`
- `max`
- `member_count`

---

## 5) Shiny UI + Feature Plan

## 5.1 Main dashboard tabs

1. **Forecast Overview**
   - Source selector (single or multi-select).
   - Variable selector (temperature/precip/snow/wind speed + future variables).
   - Point selector.
   - Ensemble boxplot/time-series display.

2. **Point Management Map**
   - Interactive map to add locations.
   - Form: point name, tags, enabled/disabled.
   - Confirm button writes to backend.

3. **System Status**
   - Last extraction timestamp.
   - S3 object count (and optional storage size trend).
   - Data freshness indicator (green/yellow/red).

## 5.2 Responsiveness rules

- App reads precomputed data only for normal interaction.
- Avoid full remote forecast cube loads during user events.
- Use filtered reads by source/variable/point/date.
- Precompute boxplot statistics to reduce client compute.

---

## 6) Write-back from Shiny (Adding Locations)

Yes, Shiny can send data back to AWS. Recommended secure flow:

1. Shiny POST -> API Gateway endpoint.
2. API Gateway invokes Lambda.
3. Lambda validates payload (name length, coordinates, uniqueness).
4. Lambda writes record to DynamoDB and/or updates `pre_canned_points.json` in S3.
5. App refreshes location list.

### Minimal request payload

```json
{
  "point_name": "My Cabin",
  "latitude": 44.123,
  "longitude": -110.456,
  "created_by": "owner"
}
```

### Why not direct S3 writes from app?
- Harder to validate and audit.
- Greater risk of malformed config.
- API layer gives schema enforcement and better security.

---

## 7) IAM / Security Baseline

1. **Shiny runtime role/user**
   - Read-only access to specific S3 prefixes.
   - Optional invoke-only permission for API endpoint.

2. **Extractor role**
   - Write to `forecasts/`, `manifests/`, and `dashboard/status/` prefixes.

3. **Lambda write-back role**
   - Write to DynamoDB table and/or `dashboard/config/pre_canned_points.json`.

4. **Secrets handling**
   - Store credentials in environment variables / secrets manager.
   - Never commit keys to repository.

---

## 8) Cost + Scale Guardrails

1. S3 lifecycle policy:
   - Keep latest forever.
   - Archive/delete old high-frequency artifacts after retention window.

2. Inventory update job:
   - Periodically update `inventory_status.json`.

3. AWS Budgets alarms:
   - Alert at 50/80/100% monthly budget thresholds.

4. Extraction limits:
   - Bound number of points/variables processed per run.
   - Skip redundant regeneration when upstream init time has not advanced.

---

## 9) Work Required Before Codex Deployment Pass

## 9.1 AWS infrastructure setup

- [ ] Create S3 bucket for forecast artifacts.
- [ ] Enable versioning + lifecycle rules.
- [ ] Create IAM policies/roles for extractor, app reader, and API writer.
- [ ] Create DynamoDB table for custom points (if using mutable location list).
- [ ] Create API Gateway + Lambda endpoint for adding points.
- [ ] Configure CloudWatch logs and budget alarms.

## 9.2 Application setup inputs

- [ ] Finalize list of default points and naming convention.
- [ ] Finalize canonical variable IDs and unit mappings:
  - temperature
  - precipitation
  - snow
  - wind speed
- [ ] Define extraction cadence (e.g., every 6h or daily).
- [ ] Define retention policy and max artifact footprint.

## 9.3 Pipeline setup inputs

- [ ] Decide runtime for extraction (ECS scheduled task vs AWS Batch vs external cron).
- [ ] Define runbook for extraction failures and retries.
- [ ] Define status contract files consumed by dashboard.

---

## 10) Codex Implementation Checklist (Suggested Order)

1. Add S3 artifact reader module to Shiny app.
2. Implement status panel using `extraction_status.json` + `inventory_status.json`.
3. Integrate variable dictionary for required variables + extension pattern.
4. Implement ensemble boxplot rendering from precomputed summaries.
5. Add separate point-management map with confirm/save flow.
6. Integrate API write-back for custom location creation.
7. Add error handling and stale-data warnings.
8. Validate performance on mobile and tune payload sizes.

---

## 11) Suggested Phase Plan

- **Phase 1:** Read-only dashboard from S3 precomputed data.
- **Phase 2:** Add status metrics and inventory visibility.
- **Phase 3:** Enable map-based point creation via API.
- **Phase 4:** Harden monitoring, cost controls, and retention tuning.


---

## 12) Helper Scripts Added for Local + AWS Functional Testing

The following scripts are included to test extraction, S3 write/read, visualization, and logging/performance instrumentation.

### Script locations

- `AWS Deployment/scripts/local_zarr_test_download.py`
- `AWS Deployment/scripts/s3_upload_test_data.py`
- `AWS Deployment/scripts/s3_read_test_data.py`
- `AWS Deployment/scripts/visualize_test_results.py`

### Output/test folders used by scripts

- `AWS Deployment/test_output/local_zarr/`
- `AWS Deployment/test_output/s3_read/`

> Note: Folder names contain a space. Use quotes around paths in shell commands.

## 12.1 Prerequisites

Python dependencies:

- `xarray`
- `zarr`
- `fsspec`
- `boto3`
- `matplotlib`

Install example:

```bash
python -m pip install xarray zarr fsspec boto3 matplotlib
```

AWS credentials for S3 tests:

- Configure with `aws configure`, environment variables, or an attached IAM role.

---

## 13) End-to-End Test Workflow

## Step A: Run local test data download (10 Zarr traces -> JSON)

This performs 10 point-trace forecast queries and writes one JSON per query plus a summary.

```bash
python "AWS Deployment/scripts/local_zarr_test_download.py" \
  --registry "shiny-dashboard/config/source_registry.json" \
  --output-dir "AWS Deployment/test_output/local_zarr" \
  --max-queries 10 \
  --log-file "AWS Deployment/test_output/local_zarr/query_performance.log"
```

Generated outputs include:

- `AWS Deployment/test_output/local_zarr/Q01_<source>.json` ... `Q10_<source>.json`
- `AWS Deployment/test_output/local_zarr/summary.json`
- `AWS Deployment/test_output/local_zarr/query_performance.log`

## Step B: Upload local test JSON to S3

```bash
python "AWS Deployment/scripts/s3_upload_test_data.py" \
  --bucket <your-bucket-name> \
  --prefix "dashboard/test/local_zarr" \
  --input-dir "AWS Deployment/test_output/local_zarr" \
  --log-file "AWS Deployment/test_output/s3_upload.log"
```

Generated outputs include:

- `AWS Deployment/test_output/local_zarr/s3_upload_summary.json`
- `AWS Deployment/test_output/s3_upload.log`

## Step C: Read back S3 data as stored

```bash
python "AWS Deployment/scripts/s3_read_test_data.py" \
  --bucket <your-bucket-name> \
  --prefix "dashboard/test/local_zarr" \
  --max-files 20 \
  --output-dir "AWS Deployment/test_output/s3_read" \
  --log-file "AWS Deployment/test_output/s3_read.log"
```

Generated outputs include:

- Local copies of retrieved JSON in `AWS Deployment/test_output/s3_read/`
- `AWS Deployment/test_output/s3_read/s3_read_summary.json`
- `AWS Deployment/test_output/s3_read.log`

## Step D: Create simple visualization from extracted traces

```bash
python "AWS Deployment/scripts/visualize_test_results.py" \
  --input-dir "AWS Deployment/test_output/local_zarr" \
  --output "AWS Deployment/test_output/local_zarr/trace_preview.png" \
  --max-series 6
```

Generated output:

- `AWS Deployment/test_output/local_zarr/trace_preview.png`

---

## 14) Logging, Performance, and Success/Failure Details

Each test script logs timing and status so you can verify performance and reliability.

## 14.1 Local Zarr query logging (`local_zarr_test_download.py`)

Per query it logs:

- Query ID and source
- Duration in seconds
- Number of elements returned
- Estimated bytes returned
- Success/failure with error details

Artifacts include:

- Query JSON payloads with `metrics`
- `summary.json` with per-query status
- `query_performance.log` for run traceability

## 14.2 S3 upload logging (`s3_upload_test_data.py`)

Per file upload it logs:

- File name and S3 key
- Payload size
- Upload duration
- Success/failure

Artifacts include:

- `s3_upload_summary.json`
- `s3_upload.log`

## 14.3 S3 read logging (`s3_read_test_data.py`)

Per object retrieval it logs:

- S3 key
- Read duration
- Bytes returned
- Query status from object payload

Artifacts include:

- `s3_read_summary.json`
- `s3_read.log`

---

## 15) Suggested Next Codex Enhancements for These Scripts

1. Add CSV performance rollups (p50/p95 query latency, throughput).
2. Add retry/backoff policy for transient HTTP/S3 errors.
3. Add optional validation schema for output JSON contracts.
4. Add CloudWatch metric push for automated monitoring.
5. Add scheduled smoke-test CI workflow (GitHub Actions or EventBridge-triggered run).
