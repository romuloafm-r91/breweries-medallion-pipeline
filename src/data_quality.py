import os
import pandas as pd


SILVER_BASE_PATH = "/opt/airflow/data-lake/silver/breweries"

def run_data_quality(execution_date: str, ti=None):
    """
    Run data quality validations on Silver layer.
    """
    
    MAX_LAT_NULL_PCT = 0.3
    MAX_UNKNOWN_COUNTRY_PCT = 0.1

    silver_path = os.path.join(
        SILVER_BASE_PATH,
        f"ingestion_date={execution_date}"
    )

    df = pd.read_parquet(silver_path)

    total_records = len(df)

    if total_records == 0:
        raise ValueError("Data Quality Failed: Silver dataset is empty.")

    duplicate_ids = df["id"].duplicated().sum()
    if duplicate_ids > 0:
        raise ValueError(f"Data Quality Failed: Found {duplicate_ids} duplicate IDs.")

    lat_null_pct = df["latitude"].isna().mean()
    lon_null_pct = df["longitude"].isna().mean()
    unknown_country_pct = (df["country"] == "unknown").mean()

    metrics = {
        "execution_date": execution_date,
        "total_records": total_records,
        "duplicate_ids": duplicate_ids,
        "lat_null_pct": round(lat_null_pct, 4),
        "lon_null_pct": round(lon_null_pct, 4),
        "unknown_country_pct": round(unknown_country_pct, 4),
    }

    print(metrics)

    if ti:
        ti.xcom_push(key="dq_metrics", value=metrics)

    if lat_null_pct > MAX_LAT_NULL_PCT:
        raise ValueError(
            f"Data Quality Failed: Latitude null pct {lat_null_pct:.2%} above threshold {MAX_LAT_NULL_PCT:.0%}"
        )

    if unknown_country_pct > MAX_UNKNOWN_COUNTRY_PCT:
        raise ValueError(f"Data Quality Failed: Too many unknown countries ({unknown_country_pct:.2%}). Its above threshold {MAX_UNKNOWN_COUNTRY_PCT:.0%}")

    print("Data Quality Passed Successfully!")
    