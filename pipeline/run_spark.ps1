[CmdletBinding()]
param(
    [string]$Input = "data/raw/online_retail.csv",
    [string]$Output = "data/curated/retail_daily_country",
    [string]$Master = "local[4]",
    [int]$ShufflePartitions = 8
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

if (-not (Test-Path .venv)) {
    python -m venv .venv
}

& .\.venv\Scripts\Activate.ps1
python -m pip install -U pip
pip install -r requirements-spark.txt

if (-not (Get-Command spark-submit -ErrorAction SilentlyContinue)) {
    throw "spark-submit not found. Install Apache Spark and ensure spark-submit is in PATH."
}

spark-submit `
  --master $Master `
  --conf "spark.sql.shuffle.partitions=$ShufflePartitions" `
  scripts/spark_process_retail.py `
  --input $Input `
  --output $Output

Write-Host ""
Write-Host "Spark processing complete."
Write-Host "Curated parquet output: $Output"
Write-Host "Next stage:"
Write-Host "  1) Upload curated output to S3 curated/ prefix"
Write-Host "  2) Run pipeline/sql/redshift_copy_retail.sql in Redshift Query Editor v2"
