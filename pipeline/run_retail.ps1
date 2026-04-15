# Retail pipeline: download UCI dataset -> S3 input -> Spark processing -> Redshift warehouse
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

if (-not (Test-Path .venv)) {
    python -m venv .venv
}
& .\.venv\Scripts\Activate.ps1
python -m pip install -U pip
pip install -r requirements-spark.txt

python scripts/download_retail_dataset.py
python scripts/spark_process_retail.py --input data/raw/online_retail.csv --output data/curated/retail_daily_country
python scripts/warehouse_load.py --parquet data/curated/retail_daily_country --table agg_retail_daily_country
if ($env:S3_BUCKET) {
    python scripts/aws_upload.py --bucket $env:S3_BUCKET --key raw/online_retail.csv --file data/raw/online_retail.csv
    aws s3 cp data/curated/retail_daily_country "s3://$($env:S3_BUCKET)/curated/retail_daily_country/" --recursive
}

Write-Host "Done. Curated parquet written to data/curated/retail_daily_country and preview loaded to DuckDB."
Write-Host "If S3_BUCKET is set, raw and curated data were uploaded to S3."
Write-Host "Next: run sql/redshift_copy_retail.sql in Redshift Query Editor v2."
