import os
import json
import pandas as pd
import shutil


BRONZE_BASE_PATH = "/opt/airflow/data-lake/bronze/breweries"
SILVER_BASE_PATH = "/opt/airflow/data-lake/silver/breweries"


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

    df = pd.DataFrame(data)

    # Selecionar colunas relevantes
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
    ]

    # Remover duplicados
    df = df.drop_duplicates(subset=["id"])

    # -----------------------------
    # 🔹 Padronização de strings
    # -----------------------------

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

    # -----------------------------
    # 🔹 Garantir tipos numéricos
    # -----------------------------

    df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
    df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")

    # -----------------------------
    # 🔹 Criar diretório Silver
    # -----------------------------

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