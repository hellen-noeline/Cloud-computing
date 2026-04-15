# DSC3219 — Full implementation mapping (exam brief)

This project uses **real retail data** (UCI *Online Retail*) and implements the three required stages based on the path actually executed.

## a) Input stage — distributed storage

| Mechanism | In this repo |
|-----------|----------------|
| **Amazon S3** | `scripts/aws_upload.py` uploads `data/raw/online_retail.csv` to `s3://<bucket>/raw/online_retail.csv` using boto3. |

Raw file: `data/raw/online_retail.csv` — fetched from the **UCI Machine Learning Repository** using **`ucimlrepo`** (official API), with **CDN ZIP** as fallback (`scripts/download_retail_dataset.py`).

Dataset size after download: ~**541,909** invoice lines (UCI Online Retail).

## b) Processing stage — executed transformation step

| Mechanism | In this repo |
|-----------|----------------|
| **Processing command** | `run_spark.ps1` runs `spark-submit scripts/spark_process_retail.py` for executor-based processing. |
| **Transformation logic** | Filter canceled/invalid transactions and aggregate by `country` + `invoice_date`. |
| **Output** | `data/curated/retail_daily_country/part-00000.parquet` |

Processing execution path (Spark executors):
- `run_spark.ps1` runs `spark-submit scripts/spark_process_retail.py`
- Produces the same curated output schema for direct Redshift loading

## c) Result store — data warehouse

| Mechanism | In this repo |
|-----------|----------------|
| **Amazon Redshift Serverless** | Load curated Parquet from S3 using `sql/redshift_copy_retail.sql` and Redshift Query Editor v2. |
| **Output format** | Curated results are written in Parquet and loaded into Redshift for BI/demo queries. |

Query interface used:
- Redshift Query Editor v2 for DDL, `COPY`, validation, and analytics queries.

## References

- D. Chen, “Online Retail,” UCI Machine Learning Repository, 2015. https://archive.ics.uci.edu/dataset/352/online+retail
