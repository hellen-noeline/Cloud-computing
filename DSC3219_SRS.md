# DSC3219 Software Requirements Specification (SRS)

## Project Title
Distributed Retail Data Pipeline (Executed Path)

## 1. Introduction

### 1.1 Purpose
This Software Requirements Specification defines the requirements for a cloud/distributed data pipeline that ingests retail data, processes it into analytics-ready aggregates, stores it in a cloud warehouse, and exposes outputs through a dashboard.

### 1.2 Scope
The implemented solution covers:
- Input ingestion to Amazon S3
- Processing with true distributed execution using `spark-submit` (`spark_process_retail.py`)
- Result storage in Amazon Redshift Serverless
- Query validation in Redshift Query Editor v2
- UI visualization using Streamlit

### 1.3 Intended Audience
- Student developer
- Course lecturer/examiner
- Demo viewers

### 1.4 Definitions
- **Input stage:** raw data ingestion layer
- **Processing stage:** transformation and aggregation layer
- **Result store:** warehouse for analytics queries
- **Curated data:** cleaned and aggregated output

## 2. Overall Description

### 2.1 Product Perspective
The system is a three-stage pipeline:
1. Raw CSV to S3 (`raw/`)
2. Spark executor processing to curated Parquet
3. Curated Parquet to Redshift table

### 2.2 User Needs
- Ingest real data from web source
- Run repeatable processing and loading commands
- Query warehouse data for validation and reporting
- Visualize results in a working UI

### 2.3 Operating Environment
- Windows PowerShell
- Python virtual environment
- AWS (S3 + Redshift Serverless + IAM)
- Streamlit dashboard runtime

## 3. Functional Requirements

- **FR-1:** System shall download and prepare `online_retail.csv` from the UCI source.
- **FR-2:** System shall upload raw CSV to `s3://nambooze-bucket/raw/online_retail.csv`.
- **FR-3:** System shall filter invalid/cancelled rows and aggregate by `country` and `invoice_date`.
- **FR-4:** System shall write curated output to `pipeline/data/curated/retail_daily_country/part-00000.parquet`.
- **FR-5:** System shall upload curated Parquet to S3 curated prefix.
- **FR-6:** System shall load curated Parquet into `analytics.agg_retail_daily_country` in Redshift.
- **FR-7:** System shall support validation SQL in Query Editor v2.
- **FR-8:** UI shall show stage status, KPIs, trend charts, and table preview.
- **FR-9:** UI shall support both local Parquet source mode and Redshift live-query mode.
- **FR-10:** System shall provide a Spark processing path (`run_spark.ps1` + `spark_process_retail.py`) that outputs the same curated schema as local mode.

## 4. Non-Functional Requirements

- **NFR-1 Reliability:** Pipeline commands must be rerunnable without destructive manual recovery steps.
- **NFR-2 Performance:** Curated data shall be stored in Parquet for efficient loading/querying.
- **NFR-3 Security:** Redshift-S3 access must use IAM role (`RedshiftS3CuratedReadRole`), not hardcoded access keys.
- **NFR-4 Usability:** One-command UI startup through `run_ui.ps1`.
- **NFR-5 Maintainability:** Scripts and docs must clearly map to each stage.
- **NFR-6 Traceability:** Report must include executed commands and outcomes.

## 5. Constraints and Assumptions

### 5.1 Constraints
- Requires valid AWS credentials and region configuration
- Requires IAM permissions for S3 list/get for the curated prefix
- Spark mode requires Java runtime and Spark installation (`spark-submit` available in `PATH`)

### 5.2 Assumptions
- Input CSV schema remains compatible with processing script expectations
- Redshift namespace has attached IAM role before `COPY`

## 6. External Interface Requirements

### 6.1 User Interfaces
- Streamlit dashboard (`pipeline/demo/app.py`) with source mode toggle
- Redshift Query Editor v2 for SQL execution

### 6.2 Software Interfaces
- Boto3 S3 API for uploads
- Redshift SQL endpoint via Query Editor and `redshift-connector`

### 6.3 Data Interfaces
- Input: CSV (`online_retail.csv`)
- Intermediate/curated: Parquet (`part-00000.parquet`)
- Warehouse table: `analytics.agg_retail_daily_country`

## 7. Acceptance Criteria

- **AC-1:** Raw dataset is present in S3 `raw/` prefix.
- **AC-2:** Curated Parquet is generated and uploaded to S3 `curated/`.
- **AC-3:** Redshift `COPY` loads rows into target table without permission/type errors.
- **AC-4:** Validation SQL returns rows.
- **AC-5:** UI starts successfully and displays metrics/charts.

## 8. Traceability Matrix

| Requirement | Implementation Evidence |
|---|---|
| FR-2 | `pipeline/scripts/aws_upload.py` raw upload command |
| FR-3 / FR-4 | `pipeline/scripts/spark_process_retail.py` |
| FR-5 | `pipeline/scripts/aws_upload.py` curated upload command |
| FR-6 / FR-7 | `pipeline/sql/redshift_copy_retail.sql` + Query Editor execution |
| FR-8 / FR-9 | `pipeline/demo/app.py` + `run_ui.ps1` |
| FR-10 | `pipeline/run_spark.ps1` + `pipeline/scripts/spark_process_retail.py` |
| NFR-4 | `run_ui.ps1` one-command launch |

## 9. Supporting Documents

- Main report: `DSC3219_Cloud_Distributed_Project_Report.md`
- System design: `DSC3219_System_Design_Standalone.md`
- Execution guide: `pipeline/README.md`
