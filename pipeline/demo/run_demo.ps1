# Launch Streamlit demo (reads curated Parquet output)
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot -Parent)

if (-not (Test-Path .venv)) {
    python -m venv .venv
}
& .\.venv\Scripts\Activate.ps1
pip install -q -r demo/requirements-demo.txt
streamlit run demo/app.py
