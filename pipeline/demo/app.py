r"""
Distributed Data Pipeline for Online Retail Analytics dashboard.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

PIPELINE_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_PARQUET = PIPELINE_ROOT / "data" / "curated" / "retail_daily_country" / "part-00000.parquet"
RAW_FILE = PIPELINE_ROOT / "data" / "raw" / "online_retail.csv"
DEFAULT_REDSHIFT_PORT = 5439


def inject_styles() -> None:
    st.markdown(
        """
        <style>
        .hero {
            padding: 1.1rem 1.3rem;
            border-radius: 14px;
            background: linear-gradient(120deg, #0f172a 0%, #0b3a5e 100%);
            color: #f8fafc;
            margin-bottom: 0.9rem;
        }
        .hero h2 { margin: 0; font-size: 1.55rem; }
        .hero p { margin: 0.4rem 0 0 0; color: #dbeafe; }
        .section-note {
            padding: 0.7rem 0.9rem;
            border: 1px solid #e2e8f0;
            border-radius: 10px;
            background: #f8fafc;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def human_mtime(path: Path) -> str:
    if not path.exists():
        return "N/A"
    return datetime.fromtimestamp(path.stat().st_mtime).strftime("%Y-%m-%d %H:%M")


def safe_sum(df: pd.DataFrame, col: str) -> float:
    return float(df[col].sum()) if col in df.columns else 0.0


def load_curated_data(parquet_path: Path) -> pd.DataFrame:
    df = pd.read_parquet(parquet_path)
    if "invoice_date" in df.columns:
        df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")
    return df


def query_redshift(
    *,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    sql_query: str,
) -> pd.DataFrame:
    import redshift_connector

    conn = redshift_connector.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
    )
    try:
        cursor = conn.cursor()
        cursor.execute(sql_query)
        rows = cursor.fetchall()
        cols = [col[0] for col in cursor.description]
        return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()


def normalize_agg_df(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "invoice_date" in out.columns:
        out["invoice_date"] = pd.to_datetime(out["invoice_date"], errors="coerce")
    return out


def render_header(source_mode: str, source_label: str) -> None:
    st.markdown(
        """
        <div class="hero">
            <h2>Distributed Data Pipeline for Online Retail Analytics</h2>
            <p>Production-style monitoring dashboard for S3 ingestion, Spark processing, and Redshift analytics.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.caption(f"Source mode: **{source_mode}** | Active dataset: **{source_label}**")


def render_stage_status(parquet_path: Path, source_mode: str) -> None:
    st.subheader("Pipeline Stage Health")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Input Stage (Amazon S3)", "Ready" if RAW_FILE.is_file() else "Missing")
        st.caption(f"Raw reference file: `online_retail.csv`")
        st.caption(f"Local file timestamp: {human_mtime(RAW_FILE)}")
    with c2:
        st.metric("Processing Stage (Apache Spark)", "Ready" if parquet_path.is_file() else "Missing")
        st.caption("Output from `spark-submit` / `run_spark.ps1`")
        st.caption(f"Curated parquet timestamp: {human_mtime(parquet_path)}")
    with c3:
        status = "Live Query" if source_mode == "Amazon Redshift" else "Configured"
        st.metric("Result Store (Redshift)", status)
        st.caption("Namespace/table queried from Query Editor compatible schema")
        st.caption("Target table: `analytics.agg_retail_daily_country`")


def render_kpis(df: pd.DataFrame) -> None:
    st.subheader("Executive KPIs")
    rows = len(df)
    countries = int(df["country"].nunique()) if "country" in df.columns else 0
    orders = int(safe_sum(df, "order_count"))
    revenue = safe_sum(df, "total_revenue")
    avg_order_value = (revenue / orders) if orders else 0.0

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Rows", f"{rows:,}")
    c2.metric("Countries", f"{countries:,}")
    c3.metric("Orders", f"{orders:,}")
    c4.metric("Revenue", f"{revenue:,.2f}")
    c5.metric("Avg Order Value", f"{avg_order_value:,.2f}")


def render_quality_panel(df: pd.DataFrame) -> None:
    st.subheader("Data Quality Snapshot")
    null_country = int(df["country"].isna().sum()) if "country" in df.columns else -1
    null_date = int(df["invoice_date"].isna().sum()) if "invoice_date" in df.columns else -1
    negative_rev = int((df["total_revenue"] < 0).sum()) if "total_revenue" in df.columns else -1
    dupes = int(df.duplicated(subset=["country", "invoice_date"]).sum()) if {"country", "invoice_date"}.issubset(df.columns) else -1
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Null country", f"{null_country:,}" if null_country >= 0 else "N/A")
    c2.metric("Null invoice_date", f"{null_date:,}" if null_date >= 0 else "N/A")
    c3.metric("Negative revenue rows", f"{negative_rev:,}" if negative_rev >= 0 else "N/A")
    c4.metric("Duplicate country-date", f"{dupes:,}" if dupes >= 0 else "N/A")


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.subheader("Analysis Filters")
    out = df.copy()
    c1, c2, c3 = st.columns(3)

    if "country" in out.columns:
        countries = sorted([c for c in out["country"].dropna().astype(str).unique().tolist()])
        selected = c1.multiselect("Countries", countries, default=countries[:8] if len(countries) > 8 else countries)
        if selected:
            out = out[out["country"].astype(str).isin(selected)]

    if "invoice_date" in out.columns and out["invoice_date"].notna().any():
        min_date = out["invoice_date"].min().date()
        max_date = out["invoice_date"].max().date()
        d_start, d_end = c2.date_input("Date range", value=(min_date, max_date), min_value=min_date, max_value=max_date)
        out = out[(out["invoice_date"].dt.date >= d_start) & (out["invoice_date"].dt.date <= d_end)]

    if "total_revenue" in out.columns and not out.empty:
        rev_min = float(out["total_revenue"].min())
        rev_max = float(out["total_revenue"].max())
        if rev_min < rev_max:
            selected_min, selected_max = c3.slider(
                "Revenue range",
                min_value=rev_min,
                max_value=rev_max,
                value=(rev_min, rev_max),
            )
            out = out[(out["total_revenue"] >= selected_min) & (out["total_revenue"] <= selected_max)]
    return out


def render_charts(df: pd.DataFrame) -> None:
    required = {"country", "invoice_date", "total_revenue"}
    if not required.issubset(df.columns):
        st.warning("Charts require columns: country, invoice_date, total_revenue.")
        return
    if df.empty:
        st.warning("No rows after filters. Adjust filter selections.")
        return

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Top 15 countries by revenue**")
        top = (
            df.groupby("country", as_index=False)["total_revenue"]
            .sum()
            .sort_values("total_revenue", ascending=False)
            .head(15)
            .set_index("country")
        )
        st.bar_chart(top["total_revenue"])
    with c2:
        st.markdown("**Revenue trend over time**")
        daily = (
            df.groupby("invoice_date", as_index=False)["total_revenue"]
            .sum()
            .sort_values("invoice_date")
            .set_index("invoice_date")
        )
        st.line_chart(daily["total_revenue"])


def render_operations_panel(df: pd.DataFrame) -> None:
    st.subheader("Operations and Export")
    left, right = st.columns([2, 1])
    with left:
        st.markdown(
            """
            <div class="section-note">
            <b>Operational query templates</b><br/>
            1) <code>select count(*) from analytics.agg_retail_daily_country;</code><br/>
            2) <code>select country, sum(total_revenue) revenue from analytics.agg_retail_daily_country group by country order by revenue desc limit 15;</code><br/>
            3) <code>select cast(invoice_date as date) d, sum(total_revenue) revenue from analytics.agg_retail_daily_country group by cast(invoice_date as date) order by d;</code>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with right:
        csv_data = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download Filtered CSV",
            data=csv_data,
            file_name="retail_analytics_filtered.csv",
            mime="text/csv",
            use_container_width=True,
        )


def main() -> None:
    st.set_page_config(page_title="Distributed Data Pipeline for Online Retail Analytics", layout="wide")
    inject_styles()

    st.sidebar.header("Dashboard Controls")
    source_mode = st.sidebar.radio("Data source", ["Local Parquet", "Amazon Redshift"], index=0)
    parquet_path = Path(st.sidebar.text_input("Curated parquet path", str(DEFAULT_PARQUET)))
    st.sidebar.caption("Use a custom path if curated output is in a different location.")
    st.sidebar.divider()

    df: pd.DataFrame | None = None
    source_label = "Curated parquet"

    if source_mode == "Local Parquet":
        if not parquet_path.is_file():
            st.error(f"Curated parquet not found: {parquet_path}")
            st.info("Run Spark processing first: `./pipeline/run_spark.ps1`")
            return
        df = load_curated_data(parquet_path)
        source_label = f"Local file ({parquet_path.name})"
    else:
        st.sidebar.subheader("Redshift Connection")
        redshift_host = st.sidebar.text_input("Host", placeholder="workgroup.region.redshift-serverless.amazonaws.com")
        redshift_port = st.sidebar.number_input("Port", min_value=1, max_value=65535, value=DEFAULT_REDSHIFT_PORT)
        redshift_db = st.sidebar.text_input("Database", value="dev")
        redshift_user = st.sidebar.text_input("User")
        redshift_password = st.sidebar.text_input("Password", type="password")
        redshift_schema = st.sidebar.text_input("Schema", value="analytics")
        redshift_table = st.sidebar.text_input("Table", value="agg_retail_daily_country")
        redshift_query = st.sidebar.text_area(
            "SQL query",
            value=f"SELECT * FROM {redshift_schema}.{redshift_table};",
            height=140,
        )
        refresh = st.sidebar.button("Load from Redshift", use_container_width=True)

        if refresh:
            missing = [
                name
                for name, val in [
                    ("Host", redshift_host),
                    ("Database", redshift_db),
                    ("User", redshift_user),
                    ("Password", redshift_password),
                ]
                if not str(val).strip()
            ]
            if missing:
                st.error(f"Missing required Redshift fields: {', '.join(missing)}")
                return
            with st.spinner("Querying Redshift..."):
                try:
                    redshift_df = query_redshift(
                        host=redshift_host.strip(),
                        port=int(redshift_port),
                        database=redshift_db.strip(),
                        user=redshift_user.strip(),
                        password=redshift_password,
                        sql_query=redshift_query.strip(),
                    )
                except Exception as exc:
                    st.error(f"Failed to query Redshift: {exc}")
                    return
            st.session_state["redshift_df"] = redshift_df

        if "redshift_df" not in st.session_state:
            st.info("Enter Redshift details and click **Load from Redshift**.")
            return
        df = st.session_state["redshift_df"]
        source_label = "Redshift live query result"

    if df is None or df.empty:
        st.warning("Loaded dataset contains no rows.")
        return

    df = normalize_agg_df(df)
    render_header(source_mode, source_label)
    render_stage_status(parquet_path, source_mode)
    st.divider()

    filtered_df = apply_filters(df)
    render_kpis(filtered_df)
    st.divider()

    tab1, tab2, tab3 = st.tabs(["Analytics", "Data Quality", "Dataset"])
    with tab1:
        render_charts(filtered_df)
        st.divider()
        render_operations_panel(filtered_df)
    with tab2:
        render_quality_panel(filtered_df)
    with tab3:
        st.subheader("Dataset Preview")
        preview = (
            filtered_df.sort_values(["invoice_date", "country"])
            if {"invoice_date", "country"}.issubset(filtered_df.columns)
            else filtered_df
        )
        st.dataframe(preview, width="stretch", height=420)


if __name__ == "__main__":
    main()
