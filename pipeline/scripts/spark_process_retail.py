"""
Retail pipeline (PySpark): true distributed aggregation on Online Retail CSV.

Run with spark-submit from pipeline/:
  spark-submit --master local[4] scripts/spark_process_retail.py --input data/raw/online_retail.csv --output data/curated/retail_daily_country
"""
from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F


def _require_columns(df: DataFrame, required: list[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("dsc3219-retail-spark-aggregation")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    try:
        raw_df = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(str(args.input))
        )

        # Accept either invoice_date or invoice_datetime and normalize to a date column.
        if "invoice_date" not in raw_df.columns and "invoice_datetime" in raw_df.columns:
            raw_df = raw_df.withColumn("invoice_date", F.to_date(F.col("invoice_datetime")))

        if "line_revenue" not in raw_df.columns and {"quantity", "unit_price"}.issubset(set(raw_df.columns)):
            raw_df = raw_df.withColumn(
                "line_revenue",
                F.col("quantity").cast("double") * F.col("unit_price").cast("double"),
            )

        _require_columns(
            raw_df,
            ["invoice_no", "invoice_date", "country", "quantity", "line_revenue"],
        )

        clean_df = (
            raw_df.filter(F.col("invoice_no").isNotNull())
            .filter(F.col("invoice_date").isNotNull())
            .filter(F.col("country").isNotNull())
            .filter(~F.col("invoice_no").cast("string").startswith("C"))
            .filter(F.col("quantity").cast("double") > 0)
            .withColumn("invoice_date", F.to_date(F.col("invoice_date")))
            .withColumn("line_revenue", F.col("line_revenue").cast("double"))
        )

        out_df = (
            clean_df.groupBy("country", "invoice_date")
            .agg(
                F.sum("line_revenue").cast("double").alias("total_revenue"),
                F.countDistinct("invoice_no").cast("long").alias("order_count"),
                F.count(F.lit(1)).cast("long").alias("line_count"),
            )
            .select("country", "invoice_date", "total_revenue", "order_count", "line_count")
        )

        out_df.write.mode("overwrite").parquet(str(args.output))
        print(f"Wrote {out_df.count()} rows to {args.output}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
