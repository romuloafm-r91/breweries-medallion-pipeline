import os
import pandas as pd
from datetime import datetime


SILVER_BASE_PATH = "/opt/airflow/data-lake/silver/breweries"
GOLD_BASE_PATH = "/opt/airflow/data-lake/gold/breweries"


def aggregate_breweries(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate dataframe by location and type.
    """
    return (
        df.groupby(
            [
                "country",
                "state",
                "brewery_type",
                "ingestion_year",
                "ingestion_month",
                "ingestion_day",
            ]
        )
        .size()
        .reset_index(name="total_breweries")
    )


def create_gold_layer(execution_date: str):
    """
    Persist dataframe on gold layer.
    Idempotent via partition overwrite.
    """

    if not os.path.exists(SILVER_BASE_PATH):
        raise FileNotFoundError(
            f"Silver base path not found: {SILVER_BASE_PATH}"
        )

    date_obj = datetime.strptime(execution_date, "%Y-%m-%d")

    year = date_obj.year
    month = date_obj.month
    day = date_obj.day

    df = pd.read_parquet(
        SILVER_BASE_PATH,
        filters=[
            ("ingestion_year", "=", year),
            ("ingestion_month", "=", month),
            ("ingestion_day", "=", day),
        ],
    )

    if df.empty:
        raise FileNotFoundError(
            f"No Silver data found for execution_date={execution_date}"
        )

    gold_df = aggregate_breweries(df)

    gold_df.to_parquet(
        GOLD_BASE_PATH,
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

    return GOLD_BASE_PATH