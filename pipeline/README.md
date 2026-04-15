# DSC3219 — Implemented Pipeline (S3 -> Processing -> Redshift)

This repository documents the full implemented flow:

1. **Input stage:** `online_retail.csv` uploaded to **Amazon S3**
2. **Processing stage:** aggregation via true Spark executor processing
3. **Result store:** aggregated parquet loaded to **Amazon Redshift Serverless**

## Dataset used

- Only dataset used: `data/raw/online_retail.csv`
- Source: UCI Online Retail dataset, downloaded by `scripts/download_retail_dataset.py`

`events.csv` and synthetic paths are not part of the executed flow.

## Stage A — Input (Amazon S3)

Upload raw dataset to S3:

```powershell
python scripts/aws_upload.py --bucket nambooze-bucket --key raw/online_retail.csv --file data/raw/online_retail.csv
```

Expected object:

`s3://nambooze-bucket/raw/online_retail.csv`

## Stage B — Processing (terminal from project root)

From project root, run Spark processing:

```powershell
.\pipeline\run_spark.ps1 -Master "local[4]" -Input "data/raw/online_retail.csv" -Output "data/curated/retail_daily_country"
```

This runs `spark-submit` against `scripts/spark_process_retail.py` and writes curated output:
- `country`
- `invoice_date`
- `total_revenue`
- `order_count`
- `line_count`

Output produced:

`pipeline/data/curated/retail_daily_country/part-00000.parquet`

The output drops directly into the existing Redshift stage (`sql/redshift_copy_retail.sql`).

Both processing modes produce the same curated schema expected by Redshift:
- `country`
- `invoice_date`
- `total_revenue`
- `order_count`
- `line_count`

## Stage C — Result Storage (Amazon Redshift Serverless)

### 1) Upload curated parquet to S3

```powershell
python scripts/aws_upload.py --bucket nambooze-bucket --key curated/retail_daily_country/part-00000.parquet --file data/curated/retail_daily_country/part-00000.parquet
```

### 2) Run load SQL in Redshift Query Editor v2

Use the SQL in `sql/redshift_copy_retail.sql` (or equivalent `CREATE TABLE` + `COPY`) with:
- bucket path `s3://nambooze-bucket/curated/retail_daily_country/`
- IAM role `arn:aws:iam::995501883037:role/RedshiftS3CuratedReadRole` attached to your Redshift Serverless namespace

Query Editor v2 is used to:
- run `CREATE TABLE` / `COPY` load commands
- run validation queries (`count(*)`, sample row checks)
- run analysis queries for report/UI demos

### 3) Verify

```sql
select count(*) from analytics.agg_retail_daily_country;
select * from analytics.agg_retail_daily_country limit 20;
```

## UI and verification

Run UI:

```powershell
.\run_ui.ps1
```

In the UI (`pipeline/demo/app.py`), choose source mode:
- **Local Parquet** (reads curated output file directly)
- **Amazon Redshift** (live query via connection details in sidebar)

## Notes

- `run_spark.ps1` is the processing-stage entrypoint.
- Local DuckDB preview scripts may still exist for convenience, but Redshift Serverless is the final result store used.
