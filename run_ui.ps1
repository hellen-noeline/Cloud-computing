[CmdletBinding()]
param(
    [int]$Port = 8502
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$pipelineDir = Join-Path $repoRoot "pipeline"
$venvDir = Join-Path $pipelineDir ".venv"
$pythonExe = Join-Path $venvDir "Scripts\\python.exe"

if (-not (Test-Path $pipelineDir)) {
    throw "Pipeline folder not found at: $pipelineDir"
}

Set-Location $pipelineDir

if (-not (Test-Path $venvDir)) {
    python -m venv .venv
}

& $pythonExe -m pip install -q -r "demo/requirements-demo.txt"
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

& $pythonExe -m streamlit run "demo/app.py" --server.port $Port
