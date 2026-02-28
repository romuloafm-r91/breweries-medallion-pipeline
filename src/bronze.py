import json
import os
from typing import List, Dict
from datetime import datetime


BASE_PATH = "/opt/airflow/data-lake/bronze/breweries"


def save_raw_data(data: List[Dict], execution_date: str) -> str:
    """
    Save all original data on bronze layer.
    """

    date_obj = datetime.strptime(execution_date, "%Y-%m-%d")

    year = date_obj.strftime("%Y")
    month = date_obj.strftime("%m")
    day = date_obj.strftime("%d")

    folder_path = os.path.join(
        BASE_PATH,
        f"ingestion_year={year}",
        f"ingestion_month={month}",
        f"ingestion_day={day}",
    )

    os.makedirs(folder_path, exist_ok=True)

    file_name = f"raw_{year}_{month}_{day}.json"
    file_path = os.path.join(folder_path, file_name)

    with open(file_path, "w") as f:
        json.dump(data, f)

    return file_path