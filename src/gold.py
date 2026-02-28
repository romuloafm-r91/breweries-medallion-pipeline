import os
import pandas as pd

SILVER_BASE_PATH = "/opt/airflow/data-lake/silver/breweries"
GOLD_BASE_PATH = "/opt/airflow/data-lake/gold/breweries"


def aggregate_breweries(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate dataframe by location and type.
    """
    return (
        df.groupby(
            ["country", "state", "brewery_type", "ingestion_date"]
        )
        .size()
        .reset_index(name="total_breweries")
    )


def create_gold_layer(execution_date: str):
    """
    Persist dataframe on gold layer (idempotent via pyarrow overwrite).
    """

    if not os.path.exists(SILVER_BASE_PATH):
        raise FileNotFoundError(
            f"Silver base path not found: {SILVER_BASE_PATH}"
        )

    df = pd.read_parquet(SILVER_BASE_PATH)

    df = df[df["ingestion_date"] == execution_date]

    if df.empty:
        raise FileNotFoundError(
            f"No Silver data found for ingestion_date={execution_date}"
        )

    gold_df = aggregate_breweries(df)

    gold_df.to_parquet(
        GOLD_BASE_PATH,
        index=False,
        partition_cols=["country", "ingestion_date"],
        engine="pyarrow",
        existing_data_behavior="delete_matching"
    )