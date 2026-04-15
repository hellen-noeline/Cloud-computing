"""
Optional: upload raw CSV to S3 using boto3 (requires AWS credentials and bucket).

Usage:
  set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_DEFAULT_REGION
  python scripts/aws_upload.py --bucket YOUR_BUCKET --key raw/2026/04/06/events.csv --file data/raw/events.csv
"""
from __future__ import annotations

import argparse
from pathlib import Path


def main() -> None:
    import boto3

    p = argparse.ArgumentParser()
    p.add_argument("--bucket", required=True)
    p.add_argument("--key", required=True)
    p.add_argument("--file", type=Path, required=True)
    args = p.parse_args()

    s3 = boto3.client("s3")
    s3.upload_file(str(args.file), args.bucket, args.key)
    print(f"Uploaded s3://{args.bucket}/{args.key}")


if __name__ == "__main__":
    main()
