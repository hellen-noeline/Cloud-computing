# Prerequisites: AWS CLI installed and configured (`aws configure`)
# Usage (from pipeline/):  .\aws\bootstrap.ps1 -BucketName "your-unique-bucket-name-dsc3219"

param(
    [Parameter(Mandatory = $true)]
    [string] $BucketName,
    [string] $Region = "",
    [string] $LocalCsv = ""
)

$ErrorActionPreference = "Stop"

if (-not $Region) {
    $Region = aws configure get region
    if (-not $Region) { $Region = "eu-west-1" }
}

$pipelineDir = Split-Path $PSScriptRoot -Parent
if (-not $LocalCsv) {
    $LocalCsv = Join-Path $pipelineDir "data\raw\online_retail.csv"
}

if (-not (Test-Path $LocalCsv)) {
    Write-Host "CSV not found at: $LocalCsv"
    Write-Host "Run first: python pipeline/scripts/download_retail_dataset.py"
    exit 1
}

Write-Host "Region: $Region  Bucket: $BucketName"

# Create bucket (location constraint needed in some regions)
$exists = aws s3api head-bucket --bucket $BucketName 2>$null
if ($LASTEXITCODE -ne 0) {
    if ($Region -eq "us-east-1") {
        aws s3api create-bucket --bucket $BucketName --region $Region
    } else {
        aws s3api create-bucket --bucket $BucketName --region $Region --create-bucket-configuration LocationConstraint=$Region
    }
}

$key = "raw/online_retail.csv"
aws s3 cp $LocalCsv "s3://$BucketName/$key" --region $Region

Write-Host ""
Write-Host "Uploaded: s3://$BucketName/$key"
Write-Host ""
Write-Host "Next steps (choose one):"
Write-Host "  1) Athena: create table on Parquet after you upload curated/ from Spark, or CSV via crawler."
Write-Host "  2) EMR: launch cluster with Spark, run spark_process_retail.py with s3a:// paths + IAM role."
Write-Host "  3) Redshift: use sql/redshift_copy_retail.sql with your bucket and IAM role ARNs."
Write-Host ""
Write-Host "Set environment for boto3 scripts:"
Write-Host "  `$env:AWS_DEFAULT_REGION = '$Region'"
