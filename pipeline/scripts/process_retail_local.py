"""
Retail pipeline (Pandas): MapReduce-style aggregates on UCI Online Retail CSV.

Map: per-row line_revenue; filter cancelled and invalid lines.
Reduce: groupBy country + invoice_date.

Run from pipeline/:
  python scripts/process_retail_local.py --input data/raw/online_retail.csv --output data/curated/retail_daily_country
"""
from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--input", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    args = p.parse_args()

    df = pd.read_csv(args.input)
    df = df.dropna(subset=["invoice_no", "invoice_date", "country"])
    inv = df["invoice_no"].astype(str)
    df = df[~inv.str.startswith("C", na=False)]
    df = df[df["quantity"] > 0]

    rows = []
    for (country, day), sub in df.groupby(["country", "invoice_date"]):
        rows.append(
            {
                "country": country,
                "invoice_date": day,
                "total_revenue": float(sub["line_revenue"].sum()),
                "order_count": sub["invoice_no"].nunique(),
                "line_count": len(sub),
            }
        )
    out_df = pd.DataFrame(rows)

    args.output.mkdir(parents=True, exist_ok=True)
    out_df.to_parquet(args.output / "part-00000.parquet", index=False)
    print(f"Wrote {len(out_df)} rows to {args.output}")


if __name__ == "__main__":
    main()
