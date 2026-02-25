import json
import os
from typing import List, Dict


BASE_PATH = "/opt/airflow/data-lake/bronze/breweries"


def save_raw_data(data: List[Dict], execution_date: str) -> str:
    folder_path = os.path.join(
        BASE_PATH,
        f"ingestion_date={execution_date}"
    )

    os.makedirs(folder_path, exist_ok=True)

    file_path = os.path.join(folder_path, "raw.json")

    with open(file_path, "w") as f:
        json.dump(data, f)

    return file_path