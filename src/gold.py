import os
import pandas as pd

SILVER_BASE_PATH = "/opt/airflow/data-lake/silver/breweries"
GOLD_BASE_PATH = "/opt/airflow/data-lake/gold/breweries_aggregated"

def aggregate_breweries(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate dataframe by location and type.
    """
    return (
        df.groupby(
            ["country", "state", "brewery_type"]
        )
        .size()
        .reset_index(name="total_breweries")
    )

def create_gold_layer(execution_date: str):
    """
    Persist dataframe on gold layer.
    """

    silver_path = os.path.join(
        SILVER_BASE_PATH,
        f"ingestion_date={execution_date}"
    )

    df = pd.read_parquet(silver_path)

    gold_df = aggregate_breweries(df)

    gold_output_dir = os.path.join(
        GOLD_BASE_PATH,
        f"ingestion_date={execution_date}"
    )

    os.makedirs(gold_output_dir, exist_ok=True)

    gold_file_path = os.path.join(
        gold_output_dir,
        "breweries_aggregated.parquet"
    )

    gold_df.to_parquet(gold_file_path, index=False)