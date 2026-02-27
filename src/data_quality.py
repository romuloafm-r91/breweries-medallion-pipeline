import logging
import os
import pandas as pd

SILVER_BASE_PATH = "/opt/airflow/data-lake/silver/breweries"

def get_quality_thresholds():
    try:
        from airflow.models import Variable

        return {
            "max_lat_null_pct": float(
                Variable.get("max_lat_null_pct", default_var=0.3)
            ),
            "max_unknown_country_pct": float(
                Variable.get("max_unknown_country_pct", default_var=0.1)
            ),
        }
    except ModuleNotFoundError:
        # fallback to tests
        return {
            "max_lat_null_pct": 0.3,
            "max_unknown_country_pct": 0.1,
        }

def validate_silver_dataframe(df: pd.DataFrame, thresholds: dict):
    """
    Validate Silver dataframe.
    """

    total_records = len(df)

    if total_records == 0:
        raise ValueError("Silver dataset is empty.")

    duplicate_ids = df["id"].duplicated().sum()
    if duplicate_ids > 0:
        raise ValueError(f"Data Quality Failed: Found {duplicate_ids} duplicate IDs.")

    lat_null_pct = df["latitude"].isna().mean()
    unknown_country_pct = (df["country"] == "unknown").mean()

    if lat_null_pct > thresholds["max_lat_null_pct"]:
        raise ValueError(f"Data Quality Failed: Latitude null pct {lat_null_pct:.2%} above threshold {thresholds['max_lat_null_pct']:.0%}")

    if unknown_country_pct > thresholds["max_unknown_country_pct"]:
        raise ValueError(f"Data Quality Failed: Too many unknown countries ({unknown_country_pct:.2%}). Its above threshold {thresholds['max_unknown_country_pct']:.0%}")

def run_data_quality(execution_date: str, ti=None):
    """
    Run data quality validations on Silver layer.
    """

    silver_path = os.path.join(
        SILVER_BASE_PATH,
        f"ingestion_date={execution_date}"
    )

    df = pd.read_parquet(silver_path)

    validate_silver_dataframe(df, get_quality_thresholds())

    logging.info("Data Quality Passed Successfully!")

    total_records = len(df)

    duplicate_ids = df["id"].duplicated().sum()
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

    logging.info(metrics)

    if ti:
        ti.xcom_push(key="dq_metrics", value=metrics)

    