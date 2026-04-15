# Run full local pipeline (Windows PowerShell)
# Prerequisite: Python 3.10+
# Optional: JDK 11/17 for PySpark — then run: pip install -r requirements-spark.txt && python scripts/spark_process.py ...

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

python -m venv .venv
& .\.venv\Scripts\Activate.ps1
python -m pip install -U pip
pip install -r requirements.txt

python scripts/generate_dataset.py --rows 50000
python scripts/process_local.py --input data/raw/events.csv --output data/curated/daily_metrics
python scripts/warehouse_load.py --parquet data/curated/daily_metrics

Write-Host "Done. Warehouse file: data/warehouse/analytics.duckdb"
Write-Host "For PySpark instead of Pandas: pip install -r requirements-spark.txt"
Write-Host "  then: python scripts/spark_process.py --input data/raw/events.csv --output data/curated/daily_metrics"
