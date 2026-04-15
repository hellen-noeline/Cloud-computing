-- Result store (Amazon Redshift) — load curated Parquet from S3.
-- Bucket and IAM role are pre-filled for this project.

CREATE TABLE IF NOT EXISTS agg_retail_daily_country (
  country VARCHAR(128),
  invoice_date DATE,
  total_revenue DOUBLE PRECISION,
  order_count BIGINT,
  line_count BIGINT
);

COPY agg_retail_daily_country
FROM 's3://nambooze-bucket/curated/retail_daily_country/'
IAM_ROLE 'arn:aws:iam::995501883037:role/RedshiftS3CuratedReadRole'
FORMAT AS PARQUET;
