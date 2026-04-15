"""
Load curated Parquet into DuckDB as a local stand-in for Redshift testing.

For cloud result storage, use Amazon Redshift with `sql/redshift_copy_retail.sql`.

Examples:
  python scripts/warehouse_load.py --parquet data/curated/retail_daily_country --table agg_retail_daily_country
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

import duckdb

_TABLE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--parquet", type=Path, required=True, help="Parquet folder from Spark")
    parser.add_argument("--table", type=str, default="agg_daily_metrics", help="DuckDB table name")
    parser.add_argument(
        "--db",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "data" / "warehouse" / "analytics.duckdb",
    )
    args = parser.parse_args()
    if not _TABLE_RE.match(args.table):
        raise ValueError("Invalid --table name")
    db.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(args.db))
    pattern = str(args.parquet / "*.parquet")
    sql = f"CREATE OR REPLACE TABLE {args.table} AS SELECT * FROM read_parquet(?)"
    con.execute(sql, [pattern])
    print(f"DuckDB table {args.table}:")
    preview = con.execute(f"SELECT * FROM {args.table} LIMIT 10").fetchdf()
    print(preview)
    con.close()
    print(f"DuckDB saved to {args.db}")


if __name__ == "__main__":
    main()
