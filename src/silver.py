import os
import json
import shutil
import pandas as pd


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

    bronze_path = os.path.join(
        BRONZE_BASE_PATH,
        f"ingestion_date={execution_date}",
        "raw.json"
    )

    with open(bronze_path, "r") as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    df = clean_breweries_dataframe(df)

    df["ingestion_date"] = execution_date

    # 🔥 IDMPOTÊNCIA REAL
    if os.path.exists(SILVER_BASE_PATH):

        for folder in os.listdir(SILVER_BASE_PATH):

            country_path = os.path.join(SILVER_BASE_PATH, folder)

            if os.path.isdir(country_path):

                date_partition = os.path.join(
                    country_path,
                    f"ingestion_date={execution_date}"
                )

                if os.path.exists(date_partition):
                    shutil.rmtree(date_partition)

    df.to_parquet(
        SILVER_BASE_PATH,
        index=False,
        partition_cols=["country", "ingestion_date"]
    )

    return SILVER_BASE_PATH