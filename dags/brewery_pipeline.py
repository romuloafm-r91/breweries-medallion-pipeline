from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.extract import fetch_breweries
from src.bronze import save_raw_data
from src.silver import transform_to_silver
from src.data_quality import run_data_quality
from src.gold import create_gold_layer

default_args = {
    "owner": "romulo",
    "retries": 2,
    "email": ["romuloafm@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
}


def extract_and_save(**context):
    execution_date = context["ds"]

    breweries = fetch_breweries()

    save_raw_data(
        data=breweries,
        execution_date=execution_date,
    )

def silver_task(**context):
    execution_date = context["ds"]
    transform_to_silver(execution_date)

def data_quality_task(**context):
    execution_date = context["ds"]
    ti = context["ti"]
    run_data_quality(execution_date, ti=ti)

def gold_task(**context):
    execution_date = context["ds"]
    create_gold_layer(execution_date)

with DAG(
    dag_id="brewery_medallion_pipeline",
    default_args=default_args,
    description="Pipeline to ingest breweries data and build medallion layers",
    schedule_interval="0 15 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["case", "medallion", "breweries", "BEES", "ABInBev"],
) as dag:

    extract_bronze = PythonOperator(
        task_id="extract_and_save_bronze",
        python_callable=extract_and_save,
        provide_context=True,
    )

    transform_silver = PythonOperator(
        task_id="transform_silver",
        python_callable=silver_task,
        provide_context=True,
    )

    data_quality = PythonOperator(
        task_id="data_quality",
        python_callable=data_quality_task,
        provide_context=True,
    )

    make_gold_layer = PythonOperator(
        task_id="create_gold_layer",
        python_callable=gold_task,
        provide_context=True,
    )

    extract_bronze >> transform_silver >> data_quality >> make_gold_layer
