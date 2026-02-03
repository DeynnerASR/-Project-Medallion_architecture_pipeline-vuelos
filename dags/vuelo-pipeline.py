import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


AIRFLOW_HOME = PATH('/opt/airflow')

if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

from scripts.bronze_ingest import correr_ingestion_bronze

default_args = {
    "owner":"airflow",
    "retries":0,
    "retry_delay":timedelta(minutes=5)
}

with DAG(
    dag_id= "vuelos_pipeline",
    default_args = default_args,
    start_date = datetime(2025,2,2),
    schedule_interval = "/30 * * * *",
    catchup = False,
)as dag:
    tarea_ingestion_bronze = PythonOperator(
        task_id = "ingestion_bronze",
        python_callable = correr_ingestion_bronze
    )