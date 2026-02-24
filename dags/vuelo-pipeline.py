import sys
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


AIRFLOW_HOME = Path('/opt/airflow')

if str(AIRFLOW_HOME) not in sys.path:
    sys.path.insert(0, str(AIRFLOW_HOME))

from scripts.bronze_ingest import correr_ingestion_bronze
from scripts.silver_transform import correr_transformacion_silver
from scripts.gold_aggregate import correr_aggregate_gold  
from scripts.load_goldData_to_snowflake import cargar_gold_layer_snowflake

default_args = {
    "owner":"airflow",
    "retries":0,
    "retry_delay":timedelta(minutes=5)
}

with DAG(
    dag_id= "vuelos_pipeline",
    default_args = default_args,
    start_date = datetime(2025,2,2),
    schedule_interval = "*/30 * * * *",
    catchup = False,
)as dag:
    task_ingestion_bronze = PythonOperator(
        task_id = "ingestion_bronze",
        python_callable = correr_ingestion_bronze
    )

    task_transformacion_silver = PythonOperator(
        task_id = "transformacion_silver",
        python_callable = correr_transformacion_silver,
    )

    task_agregación_gold = PythonOperator(
        task_id = "agregacion_gold",
        python_callable = correr_aggregate_gold,
    )

    task_cargar_data_snowflake = PythonOperator(
        task_id = "load_gold_to_snowflake",
        python_callable = cargar_gold_layer_snowflake,
    ),
    task_ingestion_bronze >> task_transformacion_silver >> task_agregación_gold >> task_cargar_data_snowflake # Indico las dependencia. Es decir, la task "transformacion_silver", depende de la ejecucion exitosa de la task "ingestion_bronze"