import os
import json
import pandas as pd
from datetime import datetime


BRONZE_BASE_PATH = "/opt/airflow/data-lake/bronze/breweries"
SILVER_BASE_PATH = "/opt/airflow/data-lake/silver/breweries"


def clean_breweries_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    df = df[
        [
            "id",
            "name",
            "brewery_type",
            "city",
            "state",
            "country",
            "latitude",
            "longitude",
        ]
    ].copy()

    df = df.drop_duplicates(subset=["id"])

    df["name"] = df["name"].fillna("").str.strip()

    df["brewery_type"] = (
        df["brewery_type"]
        .fillna("")
        .str.lower()
        .str.strip()
    )

    df["city"] = (
        df["city"]
        .fillna("unknown")
        .str.lower()
        .str.strip()
    )

    df["country"] = (
        df["country"]
        .fillna("unknown")
        .str.lower()
        .str.strip()
        .str.replace(" ", "_", regex=False)
    )

    df["state"] = (
        df["state"]
        .fillna("unknown")
        .str.lower()
        .str.strip()
        .str.replace(" ", "_", regex=False)
    )

    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    return df


def transform_to_silver(execution_date: str) -> str:

    # Parse execution date
    date_obj = datetime.strptime(execution_date, "%Y-%m-%d")
    year = date_obj.strftime("%Y")
    month = date_obj.strftime("%m")
    day = date_obj.strftime("%d")

    bronze_path = os.path.join(
        BRONZE_BASE_PATH,
        f"ingestion_year={year}",
        f"ingestion_month={month}",
        f"ingestion_day={day}",
        f"raw_{year}_{month}_{day}.json",
    )

    if not os.path.exists(bronze_path):
        raise FileNotFoundError(f"Bronze file not found: {bronze_path}")

    with open(bronze_path, "r") as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    silver_df = clean_breweries_dataframe(df)

    silver_df["ingestion_date"] = execution_date
    silver_df["ingestion_year"] = year
    silver_df["ingestion_month"] = month
    silver_df["ingestion_day"] = day

    silver_df.to_parquet(
        SILVER_BASE_PATH,
        index=False,
        partition_cols=[
            "country",
            "ingestion_year",
            "ingestion_month",
            "ingestion_day",
        ],
        engine="pyarrow",
        existing_data_behavior="delete_matching",
    )

    return SILVER_BASE_PATH