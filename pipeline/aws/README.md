# Deploying this pipeline on AWS

**Yes, it is possible.** Your project uses true Spark (`spark-submit`) processing, with S3 input/curated storage and Redshift result-store loading.

## What you need

1. **An AWS account** (student / personal).
2. **Billing alerts** enabled (EMR and Redshift can cost real money).
3. **AWS CLI v2** installed and configured:  
   `aws configure` (Access Key, Secret, default region, e.g. `eu-west-1` or `af-south-1`).

## Deployment options (simple → full)

| Tier | Services | Good for |
|------|----------|----------|
| **A — Data lake + SQL (low cost)** | **S3** + **Athena** (query Parquet in place) or **Glue** catalog | Demonstrates “store in S3 + query warehouse-style SQL” without a cluster always on |
| **B — Spark on demand** | **S3** + **EMR** (Spark step) or **Glue** (Spark ETL job) | Matches “Spark in the cloud” / parallel processing |
| **C — Full report diagram** | **S3** + **EMR** + **Redshift** + **IAM** + optional **EventBridge** / **Lambda** | Matches the report architecture exactly; highest cost/complexity |

**Redshift** is often the most expensive part for a short project. Many students use **Athena** over S3 Parquet to show “analytical query layer” with minimal spend.

## Typical flow (aligned with your code)

1. Create an **S3 bucket** (raw + curated prefixes).
2. Upload `online_retail.csv` (or Parquet after processing) — your `scripts/aws_upload.py` already does uploads via **boto3**.
3. Run **Spark** on **EMR** reading `s3a://...` (needs EMR cluster + IAM role), **or** run Spark locally via `run_spark.ps1` and upload **Parquet** only.
4. Load into **Redshift** with `COPY` (see `sql/redshift_copy_retail.sql`), **or** create an **Athena** table on curated Parquet.

## Security (exam requirement)

- Use **IAM roles** with least privilege (no long-lived keys in code if you can avoid it).
- On **EC2 / EMR**, use **instance profiles** instead of embedding keys.
- **S3 bucket policy**: restrict prefixes; **encryption** SSE-S3 or KMS; **TLS only**.

## Cost tips

- Shut down **EMR** clusters when idle; use **Spot** task nodes where allowed.
- **Redshift**: pause / use smallest node type / or substitute **Athena**.
- Set **AWS Budgets** and **billing alarms**.

## Script in this folder

- `bootstrap.ps1` — creates a bucket (name you choose), uploads the retail CSV to `raw/`, prints next steps.
- `..\run_spark.ps1` — runs `spark-submit` for true distributed processing output compatible with Redshift stage.

You still run **Spark** in the cloud only if you create an **EMR** cluster or **Glue** job; the script does not auto-launch EMR (that step is account- and region-specific).
