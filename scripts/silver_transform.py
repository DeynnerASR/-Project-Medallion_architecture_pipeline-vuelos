import json
import pandas as pd
from pathlib import Path


def correr_transformacion_silver(**context):

    fecha_ejecucion = context["ds_nodash"]

    silver_path = Path(f"/opt/airflow/data/silver")
    silver_path.mkdir(parents=True, exist_ok=True)


    # Extraere el archivo de la capa bronze, usando un xcom de la tarea anterior de Airflow
    ultimo_archivo_bronze = context["ti"].xcom_pull(key="bronze_file", task_ids="ingestion_bronze")

    if not ultimo_archivo_bronze:
        raise ValueError("No se encontró el archivo de la capa bronze")

    with open(ultimo_archivo_bronze,'r') as f:
        raw_data = json.load(f)

        df_raw = pd.DataFrame(raw_data["states"])

        # Para el dataframe, les formateamos las columnas apropiadas
        df_raw.columns = [
            "icao24",
            "callsign",
            "origin_country",
            "time_position",
            "last_contact",
            "longitude",
            "latitude",
            "baro_altitude",
            "on_ground",
            "velocity",
            "true_track",
            "vertical_rate",
            "sensors",
            "geo_altitude",
            "squawk",
            "spi",
            "position_source"
        ]

        df_limpio = df_raw[
            [
                "icao24",
                "origin_country",
                "velocity",
                "geo_altitude",
            ]
        ]

        #output_path = silver_path / f"vuelos_silver_{fecha_ejecucion}.parquet"
        output_path = silver_path / f"vuelos_silver_{fecha_ejecucion}.csv"
        df_limpio.to_csv(output_path, index=False)

        # Comparto informacion a la siguiente tarea (task) dentro del dag, usando xcom 
        # El key es el identificador unico, y su valor en este caso sera la direccion
        context["ti"].xcom_push(key="silver_file", value=str(output_path))