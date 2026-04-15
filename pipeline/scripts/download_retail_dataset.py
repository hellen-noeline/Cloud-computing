"""
Download the UCI *Online Retail* dataset (real retail transactions from the web).

Primary method: `ucimlrepo` (official UCI API — reliable).
Fallback: UCI CDN ZIP (Excel inside), then normalized CSV.

Source page: https://archive.ics.uci.edu/dataset/352/online+retail

Reference: D. Chen, "Online Retail," UCI ML Repository, 2015.
"""
from __future__ import annotations

import argparse
import io
import ssl
import urllib.request
import zipfile
from pathlib import Path

import pandas as pd

# Fallback if ucimlrepo is unavailable or fails
UCI_ZIP_URLS = (
    "https://cdn.uci-ics-mlr-prod.aws.uci.edu/352/online%2Bretail.zip",
    "https://archive.ics.uci.edu/static/public/352/online%20retail.zip",
)


def _download_bytes(url: str, timeout: int = 300) -> bytes:
    ctx = ssl.create_default_context()
    req = urllib.request.Request(url, headers={"User-Agent": "DSC3219-pipeline/1.0"})
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
        return resp.read()


def _load_from_ucimlrepo() -> pd.DataFrame:
    from ucimlrepo import fetch_ucirepo

    retail = fetch_ucirepo(id=352)
    if hasattr(retail.data, "original") and retail.data.original is not None:
        return retail.data.original.copy()
    # Some versions expose only features
    return retail.data.features.copy()


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).strip() for c in df.columns]
    df = df.rename(
        columns={
            "InvoiceNo": "invoice_no",
            "StockCode": "stock_code",
            "Description": "description",
            "Quantity": "quantity",
            "InvoiceDate": "invoice_datetime",
            "UnitPrice": "unit_price",
            "CustomerID": "customer_id",
            "Country": "country",
        }
    )
    df["invoice_datetime"] = pd.to_datetime(df["invoice_datetime"], dayfirst=True, errors="coerce")
    df["invoice_date"] = df["invoice_datetime"].dt.strftime("%Y-%m-%d")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["line_revenue"] = df["quantity"] * df["unit_price"]
    return df


def _load_from_zip_bytes(data: bytes) -> pd.DataFrame:
    with zipfile.ZipFile(io.BytesIO(data), "r") as zf:
        xlsx_names = [n for n in zf.namelist() if n.lower().endswith(".xlsx")]
        if not xlsx_names:
            raise RuntimeError("ZIP did not contain an .xlsx file")
        raw = zf.read(xlsx_names[0])
    return pd.read_excel(io.BytesIO(raw), engine="openpyxl")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "data" / "raw" / "online_retail.csv",
    )
    parser.add_argument("--cache-dir", type=Path, default=None)
    args = parser.parse_args()

    cache = args.cache_dir or (args.out.parent.parent / "cache")
    cache.mkdir(parents=True, exist_ok=True)
    args.out.parent.mkdir(parents=True, exist_ok=True)

    df = None
    try:
        print("Fetching dataset via ucimlrepo (UCI API)...")
        df = _normalize_columns(_load_from_ucimlrepo())
    except Exception as e:
        print(f"ucimlrepo failed ({e!r}); trying CDN ZIP...")

    if df is None:
        last_err: Exception | None = None
        for url in UCI_ZIP_URLS:
            try:
                print(f"Downloading {url}")
                zip_path = cache / "online_retail.zip"
                zip_path.write_bytes(_download_bytes(url))
                df = _normalize_columns(_load_from_zip_bytes(zip_path.read_bytes()))
                break
            except Exception as e:
                last_err = e
                print(f"  failed: {e!r}")
        if df is None:
            raise RuntimeError("Could not download Online Retail. Check internet and try again.") from last_err

    df.to_csv(args.out, index=False, encoding="utf-8")
    print(f"Saved {len(df)} rows to {args.out}")


if __name__ == "__main__":
    main()
