import os
import json
import pandas as pd
import shutil


BRONZE_BASE_PATH = "/opt/airflow/data-lake/bronze/breweries"
SILVER_BASE_PATH = "/opt/airflow/data-lake/silver/breweries"

def clean_breweries_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clear dataframe data from bronze layer, removing columns and adding treatments.
    """
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
    df["brewery_type"] = df["brewery_type"].fillna("").str.lower().str.strip()
    df["city"] = df["city"].fillna("unknown").str.strip().str.lower()

    df["country"] = (
        df["country"]
        .fillna("unknown")
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )

    df["state"] = (
        df["state"]
        .fillna("unknown")
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
    )

    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    return df

def transform_to_silver(execution_date: str) -> str:
    """
    Transform raw Bronze JSON into cleaned and standardized Silver parquet.
    """

    bronze_path = os.path.join(
        BRONZE_BASE_PATH,
        f"ingestion_date={execution_date}",
        "raw.json"
    )

    with open(bronze_path, "r") as f:
        data = json.load(f)

    df = clean_breweries_dataframe(pd.DataFrame(data))

    silver_path = os.path.join(
        SILVER_BASE_PATH,
        f"ingestion_date={execution_date}"
    )

    if os.path.exists(silver_path):
        shutil.rmtree(silver_path)

    os.makedirs(silver_path, exist_ok=True)

    df.to_parquet(
        silver_path,
        index=False,
        partition_cols=["country", "state"]
    )

    return silver_path